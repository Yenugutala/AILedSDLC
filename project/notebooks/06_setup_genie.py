# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Setup Databricks Genie Space
# MAGIC
# MAGIC Registers the Gold tables in a Genie Space for natural language queries.
# MAGIC Run once after Gold marts are built.
# MAGIC
# MAGIC After setup, access Genie at:
# MAGIC   Databricks workspace → SQL → Genie → "Securities Master Data"

# COMMAND ----------
import os
import requests
import json

# Get workspace URL and token from Databricks secrets (or environment)
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl", "")
if not WORKSPACE_URL:
    WORKSPACE_URL = os.environ.get("DATABRICKS_HOST", "")

# Token from Databricks secret scope (recommended) or environment variable
try:
    TOKEN = dbutils.secrets.get(scope="sml-secrets", key="databricks-token")
except Exception:
    TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
    if not TOKEN:
        print("⚠️  No Databricks token found.")
        print("   Set via: dbutils.secrets or DATABRICKS_TOKEN environment variable")

CATALOG = "statestreet"
GOLD_SCHEMA = "g_statestreet"

# COMMAND ----------
# MAGIC %md ## Verify Gold Tables Exist

# COMMAND ----------
gold_tables = [
    "dim_product",
    "dim_legal_entity",
    "fact_product_rating",
    "fact_coupon_schedule",
]

for table in gold_tables:
    full_name = f"{CATALOG}.{GOLD_SCHEMA}.{table}"
    try:
        count = spark.table(full_name).count()
        print(f"  ✓ {full_name}: {count:,} rows")
    except Exception as e:
        print(f"  ✗ {full_name}: {e}")
        print("   Run 05_gold_build.sql first!")

# COMMAND ----------
# MAGIC %md ## Create Genie Space via REST API

# COMMAND ----------
if WORKSPACE_URL and TOKEN:
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
    }

    genie_payload = {
        "title": "Securities Master Data",
        "description": (
            "Ask questions about securities: products, ratings, coupons, legal entities. "
            "Covers all product types: Equity, Debt (Bonds, Munis), Fund, Derivative, Right. "
            "Sample questions: 'How many active equity products?', "
            "'Show bonds maturing in 2025', 'Top 10 products by coupon rate'"
        ),
        "warehouse_id": "",  # Will use default SQL warehouse if empty
        "tables": [
            {
                "catalog_name": CATALOG,
                "schema_name": GOLD_SCHEMA,
                "table_name": table,
            }
            for table in gold_tables
        ],
    }

    try:
        response = requests.post(
            f"https://{WORKSPACE_URL}/api/2.0/genie/spaces",
            headers=headers,
            json=genie_payload,
            timeout=30,
        )
        if response.status_code in (200, 201):
            result = response.json()
            space_id = result.get("id", "")
            print(f"\n✅ Genie Space created!")
            print(f"   Space ID: {space_id}")
            print(f"   URL: https://{WORKSPACE_URL}/genie/spaces/{space_id}")
        else:
            print(f"\n⚠️  Genie API response: {response.status_code}")
            print(f"   {response.text[:500]}")
            print("\nManual setup:")
            print("  Databricks → SQL → Genie → New Space")
            print(f"  Add tables: {', '.join([f'{CATALOG}.{GOLD_SCHEMA}.{t}' for t in gold_tables])}")
    except Exception as e:
        print(f"\n⚠️  Could not call Genie API: {e}")
        print("\nManual setup instructions:")
        print("  1. Go to Databricks → SQL → Genie")
        print("  2. Click 'New Space'")
        print("  3. Title: 'Securities Master Data'")
        print("  4. Add these tables:")
        for t in gold_tables:
            print(f"     - {CATALOG}.{GOLD_SCHEMA}.{t}")
else:
    print("⚠️  Workspace URL or token not configured.")
    print("\nManual setup instructions:")
    print("  1. Go to Databricks → SQL → Genie")
    print("  2. Click 'New Space'")
    print("  3. Title: 'Securities Master Data'")
    print("  4. Add tables:")
    for t in gold_tables:
        print(f"     - {CATALOG}.{GOLD_SCHEMA}.{t}")

# COMMAND ----------
# MAGIC %md ## Sample Genie Questions to Try

# COMMAND ----------
print("""
Sample questions to ask in the Genie Space:

1. "How many active equity products do we have?"
   → SELECT COUNT(*) FROM dim_product WHERE type = 'EQUITY' AND status = 'ACTIVE'

2. "Show all bonds with maturity date in 2025"
   → (Genie joins to identifiers and filters on issue_currency_code/type)

3. "Which legal entities have the most products?"
   → SELECT issuer_legal_entity_id, COUNT(*) FROM dim_product GROUP BY 1 ORDER BY 2 DESC

4. "What is the average coupon rate by bond type?"
   → SELECT type, AVG(coupon_rate) FROM fact_coupon_schedule GROUP BY type

5. "Show products with rating code AAA"
   → SELECT p.product_id, p.description FROM dim_product p
      JOIN fact_product_rating r ON p.product_id = r.product_id
      WHERE r.rating_code = 'AAA'

✅ Genie setup complete! The space is now queryable from Databricks SQL → Genie.
""")
