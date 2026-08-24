from __future__ import annotations
"""
data_profiler_agent.py

Profiles every column in the statestreet Unity Catalog by:
  1. Querying INFORMATION_SCHEMA.COLUMNS for the full column list.
  2. Running profiling SQL per column: null %, cardinality, sample values.
  3. Sending each column's profile to Claude → generates a business description
     from actual data, not hardcoded documentation.
  4. Detecting join keys across tables (columns shared by multiple tables).
  5. Building and storing a join map (how tables are related to each other).
  6. Indexing everything into ChromaDB collection "data_catalog".
  7. Writing a human-readable join_map.yaml for transparency.

Run via: sml profile

Why this matters:
  Static data_dictionary.md goes stale. Business column names change.
  This agent derives business meaning from REAL data values — if the column
  contains "FIXED", "FLOATING", "ZERO", Claude knows it's a coupon type enum.
  If a column called product_id appears in 12 tables, it's the join key.
  No assumptions, no hardcodes. Re-run sml profile whenever schema changes.
"""

import json
import time
from pathlib import Path
from typing import Optional

import anthropic
import chromadb
from chromadb.utils import embedding_functions
import yaml
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()

_COLLECTION_NAME = "data_catalog"
_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
_MODEL = "claude-haiku-4-5-20251001"  # Fast + cheap for batch description generation
_CATALOG = "statestreet"
_PROFILE_SAMPLE_LIMIT = 5          # top-N sample values per column
_MAX_TABLES_PER_LAYER = 50         # safety cap


def _sql(host: str, token: str, warehouse_id: str, statement: str) -> list[dict]:
    """Execute SQL via Databricks REST API and return rows as list of dicts."""
    import urllib.request
    import urllib.error

    url = f"{host.rstrip('/')}/api/2.0/sql/statements"
    payload = json.dumps({
        "statement": statement,
        "warehouse_id": warehouse_id,
        "wait_timeout": "60s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }).encode()

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    max_retries, delay = 3, 2
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(req, timeout=90) as resp:
                body = json.loads(resp.read().decode())
                break
        except urllib.error.HTTPError as e:
            if attempt < max_retries - 1:
                time.sleep(delay)
                delay *= 2
                continue
            raise RuntimeError(f"Databricks SQL error: {e.read().decode()}") from e

    status = body.get("status", {}).get("state", "")
    if status not in ("SUCCEEDED",):
        raise RuntimeError(f"Query failed: {status} — {body.get('status', {}).get('error', {})}")

    schema = body.get("manifest", {}).get("schema", {}).get("columns", [])
    col_names = [c["name"] for c in schema]
    rows = body.get("result", {}).get("data_array", []) or []
    return [dict(zip(col_names, row)) for row in rows]


def _profile_column(
    host: str, token: str, warehouse_id: str,
    table_fqn: str, column: str, data_type: str,
) -> dict:
    """
    Run profiling queries for one column.
    Returns: { null_pct, cardinality, sample_values, is_numeric }
    """
    is_numeric = any(t in data_type.upper() for t in ("INT", "LONG", "DOUBLE", "FLOAT", "DECIMAL", "BIGINT"))
    is_boolean = "BOOLEAN" in data_type.upper()

    profile = {
        "column": column,
        "data_type": data_type,
        "null_pct": None,
        "cardinality": None,
        "sample_values": [],
        "min_val": None,
        "max_val": None,
        "is_numeric": is_numeric,
    }

    try:
        # Null % and cardinality (single pass)
        rows = _sql(host, token, warehouse_id, f"""
            SELECT
              ROUND(100.0 * SUM(CASE WHEN `{column}` IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS null_pct,
              COUNT(DISTINCT `{column}`) AS cardinality
            FROM {table_fqn}
        """)
        if rows:
            profile["null_pct"] = rows[0].get("null_pct")
            profile["cardinality"] = rows[0].get("cardinality")

        # Sample values
        if is_numeric:
            rows = _sql(host, token, warehouse_id, f"""
                SELECT
                  MIN(`{column}`) AS min_val,
                  MAX(`{column}`) AS max_val
                FROM {table_fqn}
                WHERE `{column}` IS NOT NULL
            """)
            if rows:
                profile["min_val"] = rows[0].get("min_val")
                profile["max_val"] = rows[0].get("max_val")
                profile["sample_values"] = [str(profile["min_val"]), str(profile["max_val"])]
        elif not is_boolean:
            rows = _sql(host, token, warehouse_id, f"""
                SELECT CAST(`{column}` AS STRING) AS val
                FROM {table_fqn}
                WHERE `{column}` IS NOT NULL
                GROUP BY `{column}`
                ORDER BY COUNT(*) DESC
                LIMIT {_PROFILE_SAMPLE_LIMIT}
            """)
            profile["sample_values"] = [r.get("val", "") for r in rows if r.get("val")]
    except Exception as e:
        profile["error"] = str(e)

    return profile


def _generate_description(
    client: anthropic.Anthropic,
    table_name: str,
    schema_name: str,
    column: str,
    profile: dict,
    appears_in_tables: list[str],
) -> str:
    """
    Ask Claude to generate a business description from actual data profile.
    No static documentation referenced — derives meaning purely from data shape.
    """
    join_hint = ""
    if len(appears_in_tables) > 1:
        join_hint = (
            f"\nNOTE: This column name '{column}' appears in {len(appears_in_tables)} tables: "
            f"{', '.join(appears_in_tables[:8])}. "
            f"This is likely a join key / foreign key."
        )

    sample_str = ", ".join(f'"{v}"' for v in profile.get("sample_values", []) if v)
    numeric_range = ""
    if profile.get("is_numeric") and profile.get("min_val") is not None:
        numeric_range = f"\nRange: {profile['min_val']} to {profile['max_val']}"

    prompt = (
        f"Table: {schema_name}.{table_name}\n"
        f"Column: {column}\n"
        f"Data type: {profile['data_type']}\n"
        f"Null %: {profile.get('null_pct', 'unknown')}%\n"
        f"Distinct values: {profile.get('cardinality', 'unknown')}\n"
        f"Sample values: {sample_str or '(none available)'}"
        f"{numeric_range}"
        f"{join_hint}\n\n"
        "Write a concise 1-2 sentence business description of what this column represents. "
        "Base it ONLY on the column name, data type, and sample values shown above. "
        "Do not mention the profiling statistics. "
        "If sample values look like an enum (FIXED/FLOATING/ZERO), describe the business meaning of each. "
        "If this appears to be a join key, say so."
    )

    resp = client.messages.create(
        model=_MODEL,
        max_tokens=150,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.content[0].text.strip()


def _detect_join_keys(all_columns: list[dict]) -> dict[str, list[str]]:
    """
    Find columns that appear in multiple tables — these are join key candidates.
    Returns: { column_name: [table1, table2, ...] }
    """
    col_to_tables: dict[str, list[str]] = {}
    for col in all_columns:
        name = col["column_name"]
        tbl = f"{col['table_schema']}.{col['table_name']}"
        col_to_tables.setdefault(name, []).append(tbl)

    # Only return columns that appear in 2+ tables
    return {k: v for k, v in col_to_tables.items() if len(v) > 1}


def _build_join_map(join_keys: dict[str, list[str]], all_tables: list[str]) -> dict:
    """
    Build a human-readable join map from the detected join keys.

    Output structure:
    {
      "join_keys": {
        "product_id": { "type": "FK", "tables": [...], "grain": "one row per security" }
      },
      "relationships": [
        { "from": "product", "to": "bond", "key": "product_id", "type": "1:0..1" }
      ]
    }
    """
    join_map = {"join_keys": {}, "relationships": []}

    for col_name, tables in join_keys.items():
        join_map["join_keys"][col_name] = {
            "tables": tables,
            "appears_in_n_tables": len(tables),
            "likely_role": "primary/foreign key" if col_name.endswith("_id") else "shared attribute",
        }

    # Build relationships for known FK patterns (col ending in _id)
    for col_name, tables in join_keys.items():
        if not col_name.endswith("_id"):
            continue
        # Heuristic: the table with the fewest rows or named after the key root is the PK side
        key_root = col_name.replace("_id", "")
        pk_tables = [t for t in tables if key_root in t.split(".")[-1]]
        fk_tables = [t for t in tables if key_root not in t.split(".")[-1]]
        if pk_tables:
            for fk in fk_tables:
                join_map["relationships"].append({
                    "from_table": pk_tables[0],
                    "to_table": fk,
                    "join_key": col_name,
                    "cardinality": "1:0..1",
                    "note": f"Auto-detected: {col_name} appears in both tables",
                })

    return join_map


def run(
    databricks_host: str,
    databricks_token: str,
    warehouse_id: str,
    chroma_path: Path,
    catalog: str = _CATALOG,
    layers: Optional[list[str]] = None,
) -> int:
    """
    Profile all columns in the catalog, generate LLM descriptions, detect join keys.

    Returns: number of column documents indexed into ChromaDB "data_catalog".
    """
    if layers is None:
        layers = ["b_statestreet", "s_statestreet", "g_statestreet"]

    client = anthropic.Anthropic()

    # ── 1. Get all columns from INFORMATION_SCHEMA ───────────────────────────
    console.print("[bold cyan][PROFILE][/] Fetching column list from Databricks...")
    schema_filter = "', '".join(layers)
    all_columns = _sql(databricks_host, databricks_token, warehouse_id, f"""
        SELECT table_schema, table_name, column_name, data_type, is_nullable, comment
        FROM {catalog}.information_schema.columns
        WHERE table_schema IN ('{schema_filter}')
        ORDER BY table_schema, table_name, ordinal_position
    """)
    console.print(f"[dim]  Found {len(all_columns)} columns across {len(set(c['table_name'] for c in all_columns))} tables[/]")

    # ── 2. Detect join keys ──────────────────────────────────────────────────
    join_keys = _detect_join_keys(all_columns)
    console.print(f"[dim]  Join key candidates: {list(join_keys.keys())[:10]}[/]")

    # ── 3. Profile + describe each column ───────────────────────────────────
    ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name=_EMBEDDING_MODEL)
    chroma_client = chromadb.PersistentClient(path=str(chroma_path))
    # Delete and recreate to ensure fresh profile
    try:
        chroma_client.delete_collection(_COLLECTION_NAME)
    except Exception:
        pass
    collection = chroma_client.create_collection(_COLLECTION_NAME, embedding_function=ef)

    total = 0
    layer_map = {
        "b_statestreet": "bronze",
        "s_statestreet": "silver",
        "g_statestreet": "gold",
    }

    # Group by table to batch profiling
    from itertools import groupby
    from operator import itemgetter

    sorted_cols = sorted(all_columns, key=itemgetter("table_schema", "table_name"))

    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as p:
        task = p.add_task("Profiling tables...", total=None)

        for (schema, table), cols in groupby(sorted_cols, key=lambda r: (r["table_schema"], r["table_name"])):
            col_list = list(cols)
            table_fqn = f"{catalog}.{schema}.{table}"
            layer = layer_map.get(schema, schema)

            p.update(task, description=f"Profiling {table_fqn} ({len(col_list)} cols)...")

            # Skip _rejects tables in profiling (less valuable for agents)
            if table.endswith("_rejects"):
                continue

            ids, docs, metas = [], [], []
            for col_row in col_list:
                col_name = col_row["column_name"]
                data_type = col_row.get("data_type", "STRING")

                # Profile
                profile = _profile_column(
                    databricks_host, databricks_token, warehouse_id,
                    table_fqn, col_name, data_type,
                )

                # Tables where this column also appears (for join context)
                appears_in = join_keys.get(col_name, [table_fqn])

                # Generate business description from actual data
                description = _generate_description(
                    client, table, schema, col_name, profile, appears_in,
                )

                # Build document text for embedding
                sample_str = ", ".join(str(v) for v in profile["sample_values"][:3] if v)
                doc_text = (
                    f"{layer} | {table_fqn}.{col_name} | {data_type}\n"
                    f"Description: {description}\n"
                    f"Sample values: {sample_str or 'N/A'}\n"
                    f"Null %: {profile.get('null_pct', 'unknown')} | "
                    f"Cardinality: {profile.get('cardinality', 'unknown')}"
                )
                if col_name in join_keys and len(appears_in) > 1:
                    doc_text += f"\nJoin key: appears in {len(appears_in)} tables: {', '.join(appears_in[:5])}"

                uid = f"{catalog}_{schema}_{table}_{col_name}"

                ids.append(uid)
                docs.append(doc_text)
                metas.append({
                    "catalog": catalog,
                    "schema": schema,
                    "table": table,
                    "table_fqn": table_fqn,
                    "column": col_name,
                    "layer": layer,
                    "data_type": data_type,
                    "null_pct": str(profile.get("null_pct") or ""),
                    "cardinality": str(profile.get("cardinality") or ""),
                    "sample_values": json.dumps(profile.get("sample_values", [])[:5]),
                    "description": description,
                    "is_join_key": "True" if col_name in join_keys and len(appears_in) > 1 else "False",
                    "join_tables": json.dumps(appears_in[:10]) if col_name in join_keys else "[]",
                    "comment": col_row.get("comment") or "",
                })

            if ids:
                collection.upsert(ids=ids, documents=docs, metadatas=metas)
                total += len(ids)

    # ── 4. Build and save join map ───────────────────────────────────────────
    all_table_names = list(set(c["table_name"] for c in all_columns))
    join_map = _build_join_map(join_keys, all_table_names)

    join_map_path = Path(__file__).parents[2] / "project" / "use-cases" / "securities-master" / "join_map.yaml"
    join_map_path.parent.mkdir(parents=True, exist_ok=True)
    with open(join_map_path, "w") as f:
        yaml.dump(join_map, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    console.print(f"[green]  ✓ Join map written: {join_map_path}[/]")
    console.print(f"[dim]    {len(join_map['relationships'])} relationships detected, "
                  f"{len(join_map['join_keys'])} join key columns[/]")

    console.print(f"[green]  ✓ Data catalog indexed: {total} columns[/]")
    return total
