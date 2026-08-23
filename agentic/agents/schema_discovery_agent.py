from __future__ import annotations
"""
schema_discovery_agent.py
Queries Databricks INFORMATION_SCHEMA.COLUMNS for the statestreet catalog
and indexes every column as a vector document into ChromaDB collection "schema_catalog".

This builds the live data dictionary and ontology that Beat 3 Check 3 uses
to semantically discover whether a required column already exists (surface) or is new (create).

Run via:  sml schema
Auto-run: sml index  (at demo prep time)
"""

import time
from pathlib import Path

import requests

_SQL = """
SELECT
    table_catalog,
    table_schema,
    table_name,
    column_name,
    data_type,
    is_nullable,
    comment
FROM information_schema.columns
WHERE table_catalog = 'statestreet'
ORDER BY table_schema, table_name, ordinal_position
"""

_COLLECTION_NAME = "schema_catalog"
_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


def _layer_from_schema(schema: str) -> str:
    """Map Databricks schema name to lakehouse layer label."""
    if schema.startswith("b_"):
        return "bronze"
    if schema.startswith("s_"):
        return "silver"
    if schema.startswith("g_"):
        return "gold"
    return "unknown"


def run(
    databricks_host: str,
    databricks_token: str,
    warehouse_id: str,
    chroma_path: Path,
) -> int:
    """
    Query Databricks INFORMATION_SCHEMA and index all columns into ChromaDB.

    Args:
        databricks_host: e.g. "https://adb-xxxx.azuredatabricks.net"
        databricks_token: personal access token
        warehouse_id: SQL warehouse ID for executing the query
        chroma_path: local path where ChromaDB stores its data

    Returns:
        Number of column documents indexed.
    """
    import chromadb
    from chromadb.utils import embedding_functions
    from rich.console import Console

    console = Console()
    console.print("[bold cyan][SCHEMA][/] Querying Databricks INFORMATION_SCHEMA.COLUMNS...")

    # ── Execute SQL via Databricks SQL Statement API ──────────────────
    host = databricks_host.rstrip("/")
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }

    resp = requests.post(
        f"{host}/api/2.0/sql/statements",
        headers=headers,
        json={
            "statement": _SQL,
            "warehouse_id": warehouse_id,
            "wait_timeout": "60s",
            "format": "JSON_ARRAY",
        },
        timeout=90,
    )
    resp.raise_for_status()
    data = resp.json()

    # Poll until complete
    statement_id = data.get("statement_id", "")
    state = data.get("status", {}).get("state", "")
    while state in ("PENDING", "RUNNING"):
        time.sleep(2)
        poll = requests.get(
            f"{host}/api/2.0/sql/statements/{statement_id}",
            headers=headers,
            timeout=30,
        )
        poll.raise_for_status()
        data = poll.json()
        state = data.get("status", {}).get("state", "")

    if state != "SUCCEEDED":
        err = data.get("status", {}).get("error", {}).get("message", state)
        raise RuntimeError(f"SQL statement failed (state={state}): {err}")

    schema_cols = [c["name"] for c in data["manifest"]["schema"]["columns"]]
    rows = data.get("result", {}).get("data_array", [])
    console.print(f"[dim]  Retrieved {len(rows)} columns from INFORMATION_SCHEMA[/]")

    if not rows:
        console.print("[yellow]  No columns found — is 'statestreet' catalog deployed?[/]")
        return 0

    # ── Build ChromaDB collection ─────────────────────────────────────
    chroma_path.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(chroma_path))
    ef = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name=_EMBEDDING_MODEL
    )

    # Recreate collection for a fresh index
    try:
        client.delete_collection(_COLLECTION_NAME)
    except Exception:
        pass
    collection = client.create_collection(_COLLECTION_NAME, embedding_function=ef)

    docs, ids, metadatas = [], [], []
    for row in rows:
        record = dict(zip(schema_cols, row))
        catalog    = record.get("table_catalog", "")
        schema     = record.get("table_schema", "")
        table      = record.get("table_name", "")
        column     = record.get("column_name", "")
        data_type  = record.get("data_type", "")
        is_nullable = record.get("is_nullable", "YES")
        comment    = (record.get("comment") or "").strip()

        table_fqn = f"{catalog}.{schema}.{table}"
        layer = _layer_from_schema(schema)
        nullable_bool = is_nullable.upper() != "NO"

        # Document text: rich natural-language description for embedding
        if comment:
            doc_text = f"{table_fqn}.{column}: {comment} ({data_type})"
        else:
            doc_text = f"{table_fqn}.{column}: {data_type} column in {layer} layer {table}"

        doc_id = f"{table_fqn}.{column}"
        docs.append(doc_text)
        ids.append(doc_id)
        metadatas.append({
            "layer":     layer,
            "table":     table_fqn,
            "column":    column,
            "data_type": data_type,
            "nullable":  str(nullable_bool),
            "comment":   comment[:500],
        })

    # Insert in batches
    batch_size = 500
    for i in range(0, len(docs), batch_size):
        collection.add(
            documents=docs[i:i + batch_size],
            ids=ids[i:i + batch_size],
            metadatas=metadatas[i:i + batch_size],
        )
        console.print(f"[dim]  Indexed batch {i // batch_size + 1}: {min(i + batch_size, len(docs))}/{len(docs)} columns[/]")

    console.print(f"[green]  ✓ Schema catalog ready: {len(docs)} columns indexed into ChromaDB '{_COLLECTION_NAME}'[/]")
    return len(docs)
