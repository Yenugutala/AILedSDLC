# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Data Dictionary & Ontology
# MAGIC Builds the data dictionary and ontology for the bronze / silver / gold tables. **Read-only on the lakehouse**:
# MAGIC only SELECT, `information_schema` and system lineage are used against those tables; results are written only to
# MAGIC `<catalog>.<metadata schema>`. Relationships are inferred from data values (names are not trusted);
# MAGIC Claude writes names/descriptions only. Every run is a full rebuild.

# COMMAND ----------
# MAGIC %pip install -q pyyaml

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "statestreet")
dbutils.widgets.text("bronze_schema", "b_statestreet")
dbutils.widgets.text("silver_schema", "s_statestreet")
dbutils.widgets.text("gold_schema", "g_statestreet")
dbutils.widgets.text("metadata_schema", "m_statestreet")
dbutils.widgets.text("github_repo", "Yenugutala/AILedSDLC")
dbutils.widgets.text("git_ref", "securities-master-dev")
dbutils.widgets.text("spec_glob", "project/use-cases/*/specs/*/tables.yaml")
dbutils.widgets.text("chat_model", "anthropic/claude-sonnet-4.5")
dbutils.widgets.text("embedding_model", "openai/text-embedding-3-large")
dbutils.widgets.text("min_match", "0.95")
dbutils.widgets.text("min_key_uniqueness", "0.98")
dbutils.widgets.text("enum_max_distinct", "20")
dbutils.widgets.text("max_distinct", "200000")
dbutils.widgets.text("reference_max_rows", "500")
dbutils.widgets.text("reference_max_columns", "3")
dbutils.widgets.text("use_ai", "true")
dbutils.widgets.text("expect", "bond.product_id>debt.product_id:is_a;"
                               "product.issuer_legal_entity_id>legal_entity.legal_entity_id:references;"
                               "bond.issue_currency_code>currency.code:references")

# Paste the key here only in the workspace copy — never commit it (the repo is public).
OPENROUTER_API_KEY = "<PASTE_OPENROUTER_API_KEY>"

W = {k: dbutils.widgets.get(k).strip() for k in (
        "catalog", "bronze_schema", "silver_schema", "gold_schema", "metadata_schema", "github_repo", "git_ref",
        "spec_glob", "chat_model", "embedding_model", "min_match", "min_key_uniqueness", "enum_max_distinct",
        "max_distinct", "reference_max_rows", "reference_max_columns", "use_ai", "expect")}
CAT, META = W["catalog"], W["metadata_schema"]
LAYERS = {W["bronze_schema"]: "bronze", W["silver_schema"]: "silver", W["gold_schema"]: "gold"}
SILVER = W["silver_schema"]
MIN_MATCH, MIN_KEY = float(W["min_match"]), float(W["min_key_uniqueness"])
ENUM_MAX, MAX_DISTINCT = int(W["enum_max_distinct"]), int(W["max_distinct"])
REF_ROWS, REF_COLS = int(W["reference_max_rows"]), int(W["reference_max_columns"])
SCD2 = {"effective_start_date", "effective_end_date", "is_current"}
USE_AI = W["use_ai"].lower() == "true"   # false = structure only (no LLM descriptions, no embeddings)

if META in LAYERS:
    raise ValueError(f"metadata_schema '{META}' is a lakehouse layer schema — refusing to write there")
if USE_AI and OPENROUTER_API_KEY.startswith("<"):
    raise ValueError("Set OPENROUTER_API_KEY in the configuration cell before running.")

# COMMAND ----------
# MAGIC %md ## 1. Schema (information_schema, read-only)

# COMMAND ----------
from collections import defaultdict

schemas_sql = ", ".join(f"'{s}'" for s in LAYERS)
COLS = defaultdict(list)
for r in spark.sql(f"""SELECT table_schema, table_name, column_name, data_type, is_nullable, comment, ordinal_position
                       FROM {CAT}.information_schema.columns WHERE table_schema IN ({schemas_sql})
                       ORDER BY table_schema, table_name, ordinal_position""").collect():
    COLS[f"{CAT}.{r.table_schema}.{r.table_name}"].append(r.asDict())
TABLE_COMMENTS = {f"{CAT}.{r.table_schema}.{r.table_name}": r.comment for r in spark.sql(
    f"SELECT table_schema, table_name, comment FROM {CAT}.information_schema.tables WHERE table_schema IN ({schemas_sql})").collect()}
DECLARED = spark.sql(f"""
    SELECT kcu.table_schema, kcu.table_name, kcu.column_name, tc.constraint_type
    FROM {CAT}.information_schema.table_constraints tc
    JOIN {CAT}.information_schema.key_column_usage kcu
      ON tc.constraint_schema = kcu.constraint_schema AND tc.constraint_name = kcu.constraint_name
    WHERE tc.table_schema IN ({schemas_sql})""").collect()
print(f"{len(COLS)} tables, {sum(map(len, COLS.values()))} columns, {len(DECLARED)} declared key columns")

# COMMAND ----------
# MAGIC %md ## 2. Profile every table (one Spark job per table, all columns at once)

# COMMAND ----------
import re
from pyspark.sql import functions as F

NUMERIC = ("INT", "BIGINT", "SMALLINT", "TINYINT", "DOUBLE", "FLOAT", "DECIMAL", "LONG", "SHORT")


def shape(col, values, uniqueness):
    t = col["data_type"].upper()
    if t.startswith("BOOLEAN"):
        return "flag"
    if t.startswith(("DATE", "TIMESTAMP")):
        return "date"
    if t.startswith(NUMERIC):
        return "number"
    if values and all(re.fullmatch(r"[A-Z]{3}", v) for v in values):
        return "code3"
    if values and len(values) <= ENUM_MAX and uniqueness < MIN_KEY:
        return "enum"
    if values and all(" " not in v for v in values):
        return "id"
    return "text"


PROFILE, VALUES, ROWS = {}, {}, {}
for t, cols in COLS.items():
    names = [c["column_name"] for c in cols]
    df = spark.table(t)
    aggs = [F.count(F.lit(1)).alias("__rows")]
    for i, n in enumerate(names):
        c = F.col(f"`{n}`")
        aggs += [F.count(c).alias(f"nn{i}"), F.countDistinct(c).alias(f"d{i}"),
                 F.min(c).cast("string").alias(f"mn{i}"), F.max(c).cast("string").alias(f"mx{i}"),
                 F.slice(F.collect_set(c.cast("string")), 1, MAX_DISTINCT + 1).alias(f"v{i}")]
    s = df.agg(*aggs).first()
    ROWS[t] = s["__rows"]
    for i, col in enumerate(cols):
        nn, d, vals = s[f"nn{i}"], s[f"d{i}"], s[f"v{i}"] or []
        uniq = d / nn if nn else 0.0
        key = (t, col["column_name"])
        VALUES[key] = set(vals) if len(vals) <= MAX_DISTINCT else None
        PROFILE[key] = dict(
            null_pct=round(1 - nn / ROWS[t], 4) if ROWS[t] else None, distinct_count=d,
            uniqueness=round(uniq, 4), min_value=s[f"mn{i}"], max_value=s[f"mx{i}"],
            sample_values=sorted(vals)[:5], value_shape=shape(col, vals, uniq))
print(f"profiled {len(PROFILE)} columns")

# COMMAND ----------
# MAGIC %md ## 3. Spec YAMLs from GitHub (human descriptions, primary keys, FK hints)

# COMMAND ----------
import fnmatch, io, tarfile, requests, yaml

gh = {"Accept": "application/vnd.github+json"}
sha = requests.get(f"https://api.github.com/repos/{W['github_repo']}/commits/{W['git_ref']}", headers=gh, timeout=60).json()["sha"]
tar = requests.get(f"https://api.github.com/repos/{W['github_repo']}/tarball/{sha}", headers=gh, timeout=300)
SPEC_DESC, SPEC_PK, SPEC_FK = {}, {}, set()
FK_HINT = re.compile(r"FK\s*(?:to|→|->)\s*(\w+)\.(\w+)", re.I)
with tarfile.open(fileobj=io.BytesIO(tar.content), mode="r:gz") as tf:
    for m in tf.getmembers():
        if m.isfile() and fnmatch.fnmatch(m.name.split("/", 1)[-1], W["spec_glob"]):
            for tbl in (yaml.safe_load(tf.extractfile(m)) or {}).get("tables") or []:
                if not isinstance(tbl, dict) or not tbl.get("name"):
                    continue
                SPEC_PK.setdefault(tbl["name"], tbl.get("primary_key") or [])
                SPEC_DESC.setdefault((tbl["name"], None), tbl.get("description"))
                for c in tbl.get("columns") or []:
                    if isinstance(c, dict) and c.get("name"):
                        SPEC_DESC.setdefault((tbl["name"], c["name"]), c.get("description"))
                        if (h := FK_HINT.search(str(c.get("description") or ""))):
                            SPEC_FK.add((tbl["name"], c["name"], h.group(1), h.group(2)))
print(f"spec: {len(SPEC_PK)} tables, {len(SPEC_FK)} FK hints")

# COMMAND ----------
# MAGIC %md ## 4. Keys and relationships (by data values, on the silver layer)

# COMMAND ----------
def short(t):
    return t.split(".")[-1]


def usable(t, col):
    return (col["column_name"] not in SCD2 and not col["column_name"].startswith("_")
            and PROFILE[(t, col["column_name"])]["value_shape"] in ("id", "code3", "enum")
            and VALUES.get((t, col["column_name"])) is not None)


silver = [t for t in COLS if t.split(".")[1] == SILVER]
declared_pk = {(f"{CAT}.{r.table_schema}.{r.table_name}", r.column_name) for r in DECLARED if r.constraint_type == "PRIMARY KEY"}
def tolerance(n, min_rate):
    """Allowed exceptions: a rate on big sets, but at least 1 bad row on small ones (seeded bad data)."""
    return max(1, int((1 - min_rate) * n)) if n >= 5 else 0


def is_key(t, c):
    p = PROFILE[(t, c)]
    non_null = round(ROWS[t] * (1 - (p["null_pct"] or 0)))
    return (p["distinct_count"] >= 5 and (p["null_pct"] or 0) <= 1 - MIN_KEY
            and non_null - p["distinct_count"] <= tolerance(non_null, MIN_KEY))


KEYS = {(t, c["column_name"]) for t in COLS for c in COLS[t]
        if (t, c["column_name"]) in declared_pk or is_key(t, c["column_name"])}
parents = [(t, c["column_name"]) for t in silver for c in COLS[t] if (t, c["column_name"]) in KEYS and usable(t, c)]
children = [(t, c["column_name"]) for t in silver for c in COLS[t]
            if usable(t, c) and PROFILE[(t, c["column_name"])]["distinct_count"] >= 2]

REL = []
for ct, cc in children:
    cv = VALUES[(ct, cc)]
    hits = [(pt, pc, len(cv - VALUES[(pt, pc)])) for pt, pc in parents if pt != ct]
    hits = [(pt, pc, 1 - miss / len(cv)) for pt, pc, miss in hits if miss <= tolerance(len(cv), MIN_MATCH)]
    if not hits:
        continue
    pt, pc, rate = min(hits, key=lambda h: (ROWS[h[0]], -h[2]))      # nearest parent = smallest superset
    child_is_key = (ct, cc) in KEYS
    evidence = [f"data_match:{rate:.3f}"] + (["spec_fk"] if (short(ct), cc, short(pt), pc) in SPEC_FK else [])
    REL.append(dict(from_table=ct, from_column=cc, to_table=pt, to_column=pc,
                    relation="is_a" if child_is_key else "references",
                    cardinality="1:1" if child_is_key else "N:1", match_rate=round(rate, 4),
                    confidence=(0.9 if "spec_fk" in evidence else 0.7) - (0.1 if rate < MIN_MATCH else 0),
                    evidence=evidence))

# bridge tables: composite key made of 2+ FK columns → associates between the parents
for t in silver:
    pk = SPEC_PK.get(short(t)) or []
    fks = [r for r in REL if r["from_table"] == t and r["from_column"] in pk]
    if len(pk) >= 2 and len(fks) >= 2:
        for r in fks:
            r.update(relation="associates", cardinality="N:M")
print(f"{len(KEYS)} key columns, {len(REL)} relationships")

# COMMAND ----------
# MAGIC %md ## 5. Classes, hierarchy, discriminators and layer links (UC lineage)

# COMMAND ----------
is_a = {r["from_table"]: r for r in REL if r["relation"] == "is_a"}
out_deg = defaultdict(int)
in_deg = defaultdict(int)
for r in REL:
    if r["relation"] == "references":
        out_deg[r["from_table"]] += 1
        in_deg[r["to_table"]] += 1


def descendants(t):
    kids = [c for c, r in is_a.items() if r["to_table"] == t]
    return kids + [d for k in kids for d in descendants(k)]


def discriminator(child):
    rel = is_a[child]
    root, rkey = rel["to_table"], rel["to_column"]
    while root in is_a:                                    # discriminators live on the root table
        root, rkey = is_a[root]["to_table"], is_a[root]["to_column"]
    enums = [c["column_name"] for c in COLS[root] if PROFILE[(root, c["column_name"])]["value_shape"] == "enum"
             and not c["column_name"].startswith("_") and c["column_name"] not in SCD2]
    if not enums:
        return None, None
    keys = VALUES[(child, rel["from_column"])] - set().union(*[VALUES[(d, is_a[d]["from_column"])] for d in descendants(child)] or [set()])
    keys = keys or VALUES[(child, rel["from_column"])]
    rows = spark.table(root).where(F.col(f"`{rkey}`").cast("string").isin(list(keys))).dropDuplicates([rkey]).select(*[f"`{e}`" for e in enums]).collect()
    best = None
    for e in enums:
        counts = defaultdict(int)
        for r in rows:
            counts[r[e]] += 1
        value, n = max(counts.items(), key=lambda kv: kv[1]) if counts else (None, 0)
        if rows and len(rows) - n <= tolerance(len(rows), MIN_MATCH) and (best is None or PROFILE[(root, e)]["distinct_count"] > best[2]):
            best = (e, value, PROFILE[(root, e)]["distinct_count"])
    return (best[0], str(best[1])) if best else (None, None)


lineage = spark.sql("""
    SELECT DISTINCT source_table_full_name AS src, target_table_full_name AS tgt FROM system.access.table_lineage
    WHERE target_table_catalog = :cat AND source_table_full_name IS NOT NULL""", args={"cat": CAT}).collect()
LINKS = [(r.src, r.tgt) for r in lineage if r.src in COLS and r.tgt in COLS and r.src != r.tgt]
for src, tgt in LINKS:
    REL.append(dict(from_table=tgt, from_column=None, to_table=src, to_column=None,
                    relation="copy_of" if short(src) == short(tgt) else "derived_from",
                    cardinality=None, match_rate=None, confidence=1.0, evidence=["uc_lineage"]))
col_lineage = spark.sql("""
    SELECT DISTINCT source_table_full_name AS st, source_column_name AS sc, target_table_full_name AS tt, target_column_name AS tc
    FROM system.access.column_lineage WHERE target_table_catalog = :cat AND source_table_full_name IS NOT NULL""",
    args={"cat": CAT}).collect()
SOURCE_COLS = defaultdict(set)
for r in col_lineage:
    if r.st in COLS and r.tt in COLS and short(r.st) != short(r.tt):
        SOURCE_COLS[(r.tt, r.tc)].add(f"{r.st}.{r.sc}")

CLASSES = {}
for t in silver:
    non_key = [c for c in COLS[t] if c["column_name"] not in SCD2 and not c["column_name"].startswith("_")][1:]
    kind = "association" if any(r["from_table"] == t and r["relation"] == "associates" for r in REL) else \
        "reference" if in_deg[t] and not out_deg[t] and t not in is_a and ROWS[t] <= REF_ROWS and len(non_key) <= REF_COLS else "entity"
    col, val = discriminator(t) if t in is_a else (None, None)
    CLASSES[t] = dict(kind=kind, parent_table=is_a[t]["to_table"] if t in is_a else None,
                      discriminator_column=col, discriminator_value=val,
                      bronze_tables=sorted(s for s, g in LINKS if g == t),
                      gold_tables=sorted(g for s, g in LINKS if s == t and g.split(".")[1] == W["gold_schema"]))
print({k: sum(1 for c in CLASSES.values() if c["kind"] == k) for k in ("entity", "reference", "association")},
      f"| {len(LINKS)} lineage links, {len(SOURCE_COLS)} gold/silver columns with column lineage")

# COMMAND ----------
# MAGIC %md ## 6. Names and descriptions with Claude (text only — structure above is not changed)

# COMMAND ----------
import json, time


def chat(prompt):
    for attempt in range(5):
        r = requests.post("https://openrouter.ai/api/v1/chat/completions", timeout=300,
                          headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}"},
                          json={"model": W["chat_model"], "temperature": 0, "max_tokens": 8000,
                                "messages": [{"role": "user", "content": prompt}]})
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        text = r.json()["choices"][0]["message"]["content"]
        return json.loads(text[text.index("{"):text.rindex("}") + 1])
    raise RuntimeError(f"OpenRouter chat failed: {r.status_code} {r.text[:300]}")


def table_facts(t):
    rels = [f"{r['from_column'] or '*'} {r['relation']} {r['to_table']}.{r['to_column'] or '*'}"
            for r in REL if r["from_table"] == t]
    cols = [dict(name=c["column_name"], type=c["data_type"], comment=c["comment"],
                 spec=SPEC_DESC.get((short(t), c["column_name"])), key=(t, c["column_name"]) in KEYS,
                 **{k: PROFILE[(t, c["column_name"])][k] for k in ("null_pct", "distinct_count", "sample_values", "value_shape")})
            for c in COLS[t]]
    return dict(table=t, rows=ROWS[t], table_comment=TABLE_COMMENTS.get(t), spec=SPEC_DESC.get((short(t), None)),
                relationships=rels, class_info=CLASSES.get(t), columns=cols)


PROMPT = """You document tables of a securities-master data lakehouse. Use ONLY the facts below (measured from the data).
Return JSON only: {"class_name": "PascalCase business entity", "table_description": "...", "grain": "one row per ...",
"columns": [{"name": "<exact column name>", "business_name": "...", "description": "...", "pii": false, "confidence": 0.0-1.0}]}
Include every column. Keep descriptions to one or two sentences; say what the values mean, using the samples.
FACTS:
"""
copy_source = {t: s for s, t in LINKS if short(s) == short(t)}      # silver → its bronze source
silver_of = {s: t for t, s in copy_source.items()}                  # bronze → its silver copy (reuses silver text)
AI = {}
for t in sorted(COLS, key=lambda x: (x.split(".")[1] != SILVER, x)):
    if not USE_AI:
        AI[t] = {}
        continue
    if silver_of.get(t) in AI:
        AI[t] = AI[silver_of[t]]
        continue
    AI[t] = chat(PROMPT + json.dumps(table_facts(t), default=str))
print(f"described {len(AI)} tables")

# COMMAND ----------
# MAGIC %md ## 7. Write dictionary and ontology tables (metadata schema only, full rebuild)

# COMMAND ----------
UNIFORM = "TBLPROPERTIES ('delta.enableIcebergCompatV2' = 'true', 'delta.universalFormat.enabledFormats' = 'iceberg')"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CAT}.{META}")


def write(name, rows, schema):
    spark.createDataFrame(rows, schema).createOrReplaceTempView(f"v_{name}")
    spark.sql(f"CREATE OR REPLACE TABLE {CAT}.{META}.{name} {UNIFORM} AS SELECT *, current_timestamp() AS _built_at FROM v_{name}")
    print(f"{CAT}.{META}.{name}: {len(rows)} rows")


def pick(*pairs):
    return next(((v, s) for v, s in pairs if v), (None, None))


fk_of = {(r["from_table"], r["from_column"]): f"{r['to_table']}.{r['to_column']}" for r in REL if r["from_column"]}
table_rows, column_rows, docs = [], [], []
for t, cols in COLS.items():
    ai = AI.get(t, {})
    ai_cols = {c.get("name"): c for c in ai.get("columns", []) if isinstance(c, dict)}
    desc, src = pick((TABLE_COMMENTS.get(t), "uc_comment"), (SPEC_DESC.get((short(t), None)), "spec"), (ai.get("table_description"), "ai"))
    silver_t = t if t in CLASSES else silver_of.get(t)
    pk = SPEC_PK.get(short(t)) or [c["column_name"] for c in cols if (t, c["column_name"]) in KEYS][:1]
    table_rows.append((t, LAYERS[t.split(".")[1]], ROWS[t], len(cols), pk, ai.get("grain"), desc, src,
                       ai.get("class_name"), "approved" if src != "ai" else "proposed"))
    docs.append((t, "table", f"{t} ({ai.get('class_name')}): {desc} Grain: {ai.get('grain')}. Rows: {ROWS[t]}."
                 + (f" Is a {CLASSES[silver_t]['parent_table']}." if silver_t and CLASSES[silver_t]["parent_table"] else "")))
    for c in cols:
        n, p, a = c["column_name"], PROFILE[(t, c["column_name"])], ai_cols.get(c["column_name"], {})
        cdesc, csrc = pick((c["comment"], "uc_comment"), (SPEC_DESC.get((short(t), n)), "spec"), (a.get("description"), "ai"))
        fk = fk_of.get((t, n)) or fk_of.get((silver_t, n))
        lineage_src = sorted(SOURCE_COLS.get((t, n), []))
        column_rows.append((t, n, c["ordinal_position"], c["data_type"], c["is_nullable"] == "YES", p["null_pct"],
                            p["distinct_count"], p["uniqueness"] >= MIN_KEY, p["min_value"], p["max_value"],
                            p["value_shape"], p["sample_values"], a.get("business_name"), cdesc, csrc, bool(a.get("pii")),
                            (t, n) in KEYS, fk, lineage_src, float(a.get("confidence") or 0), "approved" if csrc != "ai" else "proposed"))
        docs.append((f"{t}.{n}", "column", f"{t}.{n} ({a.get('business_name') or n}): {cdesc or ''} Type {c['data_type']}"
                     f"{', values like ' + ', '.join(p['sample_values'][:3]) if p['sample_values'] else ''}."
                     f"{' References ' + fk + '.' if fk else ''}{' Derived from ' + ', '.join(lineage_src) + '.' if lineage_src else ''}"))

class_rows = [(AI.get(t, {}).get("class_name") or short(t), c["kind"], t, c["bronze_tables"], c["gold_tables"],
               (AI.get(c["parent_table"], {}).get("class_name") or short(c["parent_table"])) if c["parent_table"] else None,
               c["discriminator_column"], c["discriminator_value"], AI.get(t, {}).get("table_description"))
              for t, c in CLASSES.items()]
rel_rows = [(r["from_table"], r["from_column"], r["to_table"], r["to_column"], r["relation"], r["cardinality"],
             r["match_rate"], r["confidence"], r["evidence"]) for r in REL]

write("dictionary_tables", table_rows, "table_fqn STRING, layer STRING, row_count BIGINT, column_count INT, "
      "primary_key ARRAY<STRING>, grain STRING, description STRING, description_source STRING, class_name STRING, status STRING")
write("dictionary_columns", column_rows, "table_fqn STRING, column_name STRING, ordinal INT, data_type STRING, "
      "nullable BOOLEAN, null_pct DOUBLE, distinct_count BIGINT, is_unique BOOLEAN, min_value STRING, max_value STRING, "
      "value_shape STRING, sample_values ARRAY<STRING>, business_name STRING, description STRING, description_source STRING, "
      "pii BOOLEAN, is_key BOOLEAN, fk_target STRING, derived_from ARRAY<STRING>, confidence DOUBLE, status STRING")
write("ontology_classes", class_rows, "class_name STRING, kind STRING, table_fqn STRING, bronze_tables ARRAY<STRING>, "
      "gold_tables ARRAY<STRING>, parent_class STRING, discriminator_column STRING, discriminator_value STRING, description STRING")
write("ontology_relationships", rel_rows, "from_table STRING, from_column STRING, to_table STRING, to_column STRING, "
      "relation STRING, cardinality STRING, match_rate DOUBLE, confidence DOUBLE, evidence ARRAY<STRING>")

# COMMAND ----------
# MAGIC %md ## 8. Embeddings for tables and columns (relationships are queried with SQL, not embedded)

# COMMAND ----------
from datetime import datetime, timezone


def embed(texts):
    for attempt in range(6):
        r = requests.post("https://openrouter.ai/api/v1/embeddings", timeout=180,
                          headers={"Authorization": f"Bearer {OPENROUTER_API_KEY}"},
                          json={"model": W["embedding_model"], "input": texts})
        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        return [d["embedding"] for d in sorted(r.json()["data"], key=lambda d: d.get("index", 0))]
    raise RuntimeError(f"OpenRouter embeddings failed: {r.status_code} {r.text[:300]}")


now, emb_rows = datetime.now(timezone.utc), []
for i in range(0, len(docs) if USE_AI else 0, 64):
    batch = docs[i:i + 64]
    for (item_id, item_type, text), vec in zip(batch, embed([d[2] for d in batch])):
        emb_rows.append((item_id, item_type, text, W["embedding_model"], len(vec), [float(x) for x in vec], now))
if USE_AI:
    write("dictionary_embeddings", emb_rows, "item_id STRING, item_type STRING, text STRING, model STRING, dims INT, "
          "embedding ARRAY<FLOAT>, embedded_at TIMESTAMP")

# COMMAND ----------
# MAGIC %md ## 9. Checks and summary

# COMMAND ----------
checks = []
for e in filter(None, (x.strip() for x in W["expect"].split(";"))):
    pair, rel = e.split(":")
    (ft, fc), (tt, tc) = (p.rsplit(".", 1) for p in pair.split(">"))
    ok = any(short(r["from_table"]) == ft and r["from_column"] == fc and short(r["to_table"]) == tt
             and r["to_column"] == tc and r["relation"] == rel for r in REL if r["from_table"].split(".")[1] == SILVER)
    checks.append((e, "PASS" if ok else "FAIL"))
for c in checks:
    print(*c)
summary = dict(tables=len(table_rows), columns=len(column_rows), relationships=len(rel_rows), classes=len(class_rows),
               embeddings=len(emb_rows), checks_failed=sum(s == "FAIL" for _, s in checks))
print(summary)
dbutils.notebook.exit(json.dumps(summary))
