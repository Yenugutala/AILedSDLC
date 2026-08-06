# Data Quality Patterns — SQL DQ Rules for Silver Layer

Complete reference for the 128 DQ rules applied to securities data.
All rules are SQL SELECT statements — failing rows route to `_rejects` tables.

---

## 1. DQ Rule Anatomy

Every rule in `specs/silver/rules.yaml` follows this structure:

```yaml
- rule_id: RULE0001
  table: product
  column: id_type
  dq_dimension: Validity          # Validity | Completeness | Uniqueness | Consistency | Accuracy | Timeliness
  rule_type: ENUM_MEMBERSHIP      # See section 2 for all rule types
  severity: HIGH                  # HIGH | MEDIUM | LOW
  description: "id_type must be one of the allowed identifier types"
  allowed_values: [CUSIP, ISIN, SEDOL, TICKER, BLOOMBERG_ID]
  sql: |
    SELECT * FROM statestreet.b_statestreet.product
    WHERE id_type NOT IN ('CUSIP','ISIN','SEDOL','TICKER','BLOOMBERG_ID')
```

The `sql` field is a **SELECT that returns FAILING rows**.
Empty result = all rows pass. Non-empty = those rows go to `_rejects`.

---

## 2. DQ Rule Types & SQL Templates

### ENUM_MEMBERSHIP — Value must be in allowed set

```sql
-- Failing rows: id_type not in allowed list
SELECT * FROM statestreet.b_statestreet.product
WHERE id_type NOT IN ('CUSIP','ISIN','SEDOL','TICKER','BLOOMBERG_ID')
   OR id_type IS NULL;

-- Silver DQ check (after Bronze load)
INSERT INTO statestreet.s_statestreet.product_rejects
SELECT *,
  'RULE0001'                              AS _rule_id,
  CONCAT('Invalid id_type: ', id_type)   AS _violation_detail,
  current_timestamp()                     AS _rejected_ts,
  '${dq_rule_version}'                    AS _dq_rule_version
FROM statestreet.b_statestreet.product
WHERE id_type NOT IN ('CUSIP','ISIN','SEDOL','TICKER','BLOOMBERG_ID');
```

**Used for:** `id_type`, `status`, `type`, `sub_type`, `coupon_type`, `rating_code`

### NOT_NULL — Column must have a value

```sql
SELECT * FROM statestreet.b_statestreet.product
WHERE product_id IS NULL OR TRIM(product_id) = '';
```

**Used for:** `product_id`, `legal_entity_id`, `series_id`, `coupon_id` — all primary keys and FKs

### REFERENTIAL_INTEGRITY — FK must exist in parent table

```sql
-- Every bond must have a product entry
SELECT b.* FROM statestreet.b_statestreet.bond b
LEFT JOIN statestreet.b_statestreet.product p ON b.product_id = p.product_id
WHERE p.product_id IS NULL;

-- Every product_rating must reference a valid product
SELECT pr.* FROM statestreet.b_statestreet.product_rating pr
LEFT JOIN statestreet.b_statestreet.product p ON pr.product_id = p.product_id
WHERE p.product_id IS NULL;
```

### RANGE_CHECK — Numeric value within expected bounds

```sql
-- Coupon rate must be between 0% and 100%
SELECT * FROM statestreet.b_statestreet.coupon
WHERE coupon_rate < 0 OR coupon_rate > 100;

-- Issue price cannot be negative
SELECT * FROM statestreet.b_statestreet.product
WHERE issue_price IS NOT NULL AND issue_price < 0;

-- current_face_value should be between 0 and 100 (percentage)
SELECT * FROM statestreet.b_statestreet.product
WHERE current_face_value IS NOT NULL
  AND (current_face_value < 0 OR current_face_value > 100);
```

### DATE_VALIDITY — Date must be in valid range or order

```sql
-- Issue date cannot be in the future
SELECT * FROM statestreet.b_statestreet.product
WHERE issue_date > current_date();

-- Maturity date must be after issue date
SELECT b.* FROM statestreet.b_statestreet.bond b
JOIN statestreet.b_statestreet.product p ON b.product_id = p.product_id
WHERE b.maturity_date <= p.issue_date;

-- Payment date cannot be before issue date
SELECT c.* FROM statestreet.b_statestreet.coupon c
JOIN statestreet.b_statestreet.product p ON c.product_id = p.product_id
WHERE c.payment_date < p.issue_date;
```

### UNIQUENESS — No duplicate primary keys

```sql
-- product_id must be unique in product table
SELECT product_id, COUNT(*) AS cnt
FROM statestreet.b_statestreet.product
GROUP BY product_id
HAVING COUNT(*) > 1;

-- Composite PK: listed_derivative_tick (product_id, tick_id)
SELECT product_id, tick_id, COUNT(*) AS cnt
FROM statestreet.b_statestreet.listed_derivative_tick
GROUP BY product_id, tick_id
HAVING COUNT(*) > 1;
```

### REGEX_FORMAT — Value must match a pattern

```sql
-- CUSIP: 9 alphanumeric characters
SELECT * FROM statestreet.b_statestreet.identifiers
WHERE identifier_type = 'CUSIP'
  AND identifier_value NOT RLIKE '^[A-Z0-9]{9}$';

-- ISIN: 2-letter country code + 10 alphanumeric
SELECT * FROM statestreet.b_statestreet.identifiers
WHERE identifier_type = 'ISIN'
  AND identifier_value NOT RLIKE '^[A-Z]{2}[A-Z0-9]{10}$';

-- ISO currency code: exactly 3 uppercase letters
SELECT * FROM statestreet.b_statestreet.product
WHERE face_currency_code IS NOT NULL
  AND face_currency_code NOT RLIKE '^[A-Z]{3}$';
```

### CROSS_TABLE_CONSISTENCY — Business rules across tables

```sql
-- Bond must have coupon_type set when coupon records exist
SELECT DISTINCT b.product_id
FROM statestreet.b_statestreet.bond b
JOIN statestreet.b_statestreet.coupon c ON b.product_id = c.product_id
WHERE b.coupon_type IS NULL;

-- Fund must not appear in bond table (type enforcement)
SELECT f.product_id
FROM statestreet.b_statestreet.fund f
JOIN statestreet.b_statestreet.bond b ON f.product_id = b.product_id;

-- Legal entity on a product must exist in legal_entity table
SELECT p.product_id, p.issuer_legal_entity_id
FROM statestreet.b_statestreet.product p
LEFT JOIN statestreet.b_statestreet.legal_entity le
  ON p.issuer_legal_entity_id = le.legal_entity_id
WHERE p.issuer_legal_entity_id IS NOT NULL
  AND le.legal_entity_id IS NULL;
```

### COMPLETENESS — Required field completeness by product type

```sql
-- Bonds must have maturity_date
SELECT b.product_id FROM statestreet.b_statestreet.bond b
WHERE b.maturity_date IS NULL;

-- Coupon bonds must have coupon records (non-zero-coupon bonds)
SELECT b.product_id
FROM statestreet.b_statestreet.bond b
WHERE b.coupon_type != 'ZERO_COUPON'
  AND NOT EXISTS (
    SELECT 1 FROM statestreet.b_statestreet.coupon c
    WHERE c.product_id = b.product_id
  );

-- Listed derivatives must have an underlying_product_id
SELECT ld.product_id FROM statestreet.b_statestreet.listed_derivative ld
WHERE ld.underlying_product_id IS NULL;
```

---

## 3. DQ Rule Versioning

Every time `specs/silver/rules.yaml` changes, a new version is computed and stamped on Silver rows.

```python
# src/dq/dq_rule_version.py
import hashlib

def get_current_version(rules_yaml_path: str) -> str:
    """SHA256 of the entire rules.yaml file content."""
    content = open(rules_yaml_path, "rb").read()
    return hashlib.sha256(content).hexdigest()[:16]   # 16-char short hash
```

```sql
-- Every Silver row carries the version of rules used to evaluate it
SELECT _dq_rule_version, COUNT(*) AS rows
FROM statestreet.s_statestreet.product
GROUP BY _dq_rule_version
ORDER BY 1;

-- Find rows evaluated under old rules (stale rows — need rescan)
SELECT COUNT(*) AS stale_rows
FROM statestreet.s_statestreet.product
WHERE _dq_rule_version != '${current_dq_rule_version}';
```

---

## 4. DQ Reject Report

When DQ runs, failing rows are inserted to `_rejects` tables. A report is generated and logged.

```sql
-- Summary: how many rejects per rule per table
SELECT
  _rule_id,
  COUNT(*) AS reject_count,
  MAX(_rejected_ts) AS last_rejected_at
FROM statestreet.s_statestreet.product_rejects
GROUP BY _rule_id
ORDER BY reject_count DESC;

-- Full reject detail for a specific rule
SELECT
  product_id,
  id_type,
  _rule_id,
  _violation_detail,
  _rejected_ts
FROM statestreet.s_statestreet.product_rejects
WHERE _rule_id = 'RULE0001'
ORDER BY _rejected_ts DESC
LIMIT 100;

-- Cross-table reject summary (for DQ report dashboard)
SELECT 'product'       AS table_name, COUNT(*) AS rejects FROM statestreet.s_statestreet.product_rejects
UNION ALL
SELECT 'bond',                         COUNT(*) FROM statestreet.s_statestreet.bond_rejects
UNION ALL
SELECT 'legal_entity',                 COUNT(*) FROM statestreet.s_statestreet.legal_entity_rejects
ORDER BY rejects DESC;
```

---

## 5. DQ Selective Rescan (When Rules Change)

When `silver/rules.yaml` is updated (new rule, fixed SQL, changed thresholds):

1. New SHA256 version is computed automatically
2. Pipeline identifies Silver tables with rows under the old version
3. Only those tables are rescanned — others are untouched

```python
# src/dq/dq_rule_version.py
def tables_needing_rescan(silver_schema: str, catalog: str, current_version: str, spark) -> list:
    """Tables that have ANY rows evaluated under a different (old) rule version."""
    result = []
    tables = spark.sql(f"SHOW TABLES IN {catalog}.{silver_schema}").collect()
    for row in tables:
        table = row["tableName"]
        if table.endswith("_rejects"):
            continue
        try:
            stale = spark.sql(f"""
                SELECT COUNT(*) AS cnt
                FROM {catalog}.{silver_schema}.{table}
                WHERE _dq_rule_version != '{current_version}'
            """).first()["cnt"]
            if stale > 0:
                result.append(table)
        except Exception:
            pass
    return result
```

---

## 6. Known DQ Issues in Securities Data

See `known_issues.md` for full list. Key patterns:

| Issue | Root Cause | DQ Dimension | Workaround |
|-------|-----------|--------------|------------|
| `coupon.payment_date` NULL for some bonds | Source system omits future dates | Completeness | Accept NULL if `coupon_type = 'ZERO_COUPON'` |
| `product.sub_type` blank for older records | Legacy data pre-dates sub_type field | Completeness | Accept NULL in MEDIUM severity rules |
| `identifiers` duplicate CUSIP across dates | CUSIP reuse across matured/new securities | Uniqueness | Scope uniqueness check to `status = 'ACTIVE'` |
| `legal_entity_id` FK violation for external issuers | Issuer not in our reference data | Referential Integrity | Warn (MEDIUM) instead of reject |

---

## 7. DQ Dimensions Reference

| Dimension | Definition | Example Rule |
|-----------|------------|-------------|
| **Validity** | Values conform to allowed format/domain | `id_type IN ('CUSIP','ISIN',...)` |
| **Completeness** | Required fields are populated | `product_id IS NOT NULL` |
| **Uniqueness** | No duplicate records | `GROUP BY pk HAVING COUNT(*) > 1` |
| **Consistency** | Logical relationships hold across tables | Bond with coupon → coupon records exist |
| **Accuracy** | Value matches trusted reference source | Rating code matches `product_rating_type` |
| **Timeliness** | Data freshness within SLA | `_ingestion_ts > current_timestamp() - INTERVAL 1 DAY` |

---

## 8. Severity Levels

| Severity | Behavior | Used For |
|----------|----------|----------|
| **HIGH** | Row rejected to `_rejects` table; blocked from Silver | PK null, invalid enum, FK violation |
| **MEDIUM** | Row rejected with warning flag; alert sent | Optional fields, edge cases |
| **LOW** | Row passes Silver; anomaly logged but not rejected | Statistical outliers, legacy quirks |

```sql
-- MEDIUM severity — flag in Silver with _dq_warnings column (not rejected)
ALTER TABLE statestreet.s_statestreet.product
ADD COLUMNS IF NOT EXISTS _dq_warnings ARRAY<STRING>;

-- Update Silver row with warnings (not rejected)
UPDATE statestreet.s_statestreet.product
SET _dq_warnings = ARRAY('RULE0045: sub_type is null for pre-2000 record')
WHERE product_id = '...' AND is_current = TRUE;
```

---

## 9. Testing DQ Rules

Each DQ rule has a corresponding SQL test (generated by QA Agent):

```sql
-- tests/silver/test_rule0001.sql
-- Asserts: no product rows in Silver where id_type is invalid
SELECT COUNT(*) AS invalid_rows
FROM statestreet.s_statestreet.product
WHERE is_current = TRUE
  AND id_type NOT IN ('CUSIP','ISIN','SEDOL','TICKER','BLOOMBERG_ID');
-- Expected: 0
```

```python
# tests/silver/test_dq_rules.py  (pytest wrapper)
def test_rule0001_id_type_enum(spark):
    result = spark.sql("""
        SELECT COUNT(*) AS cnt
        FROM statestreet.s_statestreet.product
        WHERE is_current = TRUE
          AND id_type NOT IN ('CUSIP','ISIN','SEDOL','TICKER','BLOOMBERG_ID')
    """).first()["cnt"]
    assert result == 0, f"RULE0001 violation: {result} rows with invalid id_type in Silver"
```
