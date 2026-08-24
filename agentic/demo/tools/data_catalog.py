from __future__ import annotations
"""
demo/tools/data_catalog.py

DataCatalog — semantic search over the AI-profiled data catalog.

Unlike schema_catalog (which indexes raw INFORMATION_SCHEMA column names/types),
this collection stores:
  • LLM-generated business descriptions derived from ACTUAL data values
  • Data profiles: null %, cardinality, sample values
  • Join key detection: which columns link which tables

Built by: sml profile  (data_profiler_agent.run())
Used by:  Beat 2 (clarification grounding) and Beat 3 Check 3 (spec validation)
"""

import json
from pathlib import Path

_COLLECTION_NAME = "data_catalog"
_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


class DataCatalog:
    def __init__(self, chroma_path: Path):
        self._chroma_path = chroma_path
        self._client = None
        self._collection = None

    def _ensure_loaded(self) -> None:
        if self._collection is not None:
            return
        try:
            import chromadb
            from chromadb.utils import embedding_functions
            ef = embedding_functions.SentenceTransformerEmbeddingFunction(
                model_name=_EMBEDDING_MODEL
            )
            self._client = chromadb.PersistentClient(path=str(self._chroma_path))
            self._collection = self._client.get_collection(
                _COLLECTION_NAME, embedding_function=ef
            )
        except Exception:
            self._collection = None

    def available(self) -> bool:
        self._ensure_loaded()
        if self._collection is None:
            return False
        try:
            return self._collection.count() > 0
        except Exception:
            return False

    def search(self, query: str, n: int = 10) -> list[dict]:
        """
        Semantic search for columns relevant to query.

        Returns list of dicts with profiling data and LLM-generated description:
          [{table, column, layer, data_type, description,
            null_pct, cardinality, sample_values,
            is_join_key, join_tables, score}]
        """
        self._ensure_loaded()
        if self._collection is None:
            return []
        try:
            count = self._collection.count()
            if count == 0:
                return []
            results = self._collection.query(
                query_texts=[query],
                n_results=min(n, count),
                include=["documents", "metadatas", "distances"],
            )
        except Exception:
            return []

        output = []
        for i, doc in enumerate(results["documents"][0]):
            meta = results["metadatas"][0][i]
            distance = results["distances"][0][i]
            score = max(0.0, 1.0 - distance)

            sample_vals = []
            try:
                sample_vals = json.loads(meta.get("sample_values", "[]"))
            except Exception:
                pass

            join_tables = []
            try:
                join_tables = json.loads(meta.get("join_tables", "[]"))
            except Exception:
                pass

            output.append({
                "table":        meta.get("table_fqn", meta.get("table", "")),
                "column":       meta.get("column", ""),
                "layer":        meta.get("layer", ""),
                "data_type":    meta.get("data_type", ""),
                "description":  meta.get("description", ""),
                "null_pct":     meta.get("null_pct", ""),
                "cardinality":  meta.get("cardinality", ""),
                "sample_values": sample_vals,
                "is_join_key":  meta.get("is_join_key", "False") == "True",
                "join_tables":  join_tables,
                "score":        round(score, 3),
                "document":     doc,
            })
        return output

    def search_join_keys(self) -> list[dict]:
        """Return all columns flagged as join keys (appear in 2+ tables)."""
        self._ensure_loaded()
        if self._collection is None:
            return []
        try:
            results = self._collection.get(
                where={"is_join_key": "True"},
                include=["metadatas"],
            )
            output = []
            for meta in results.get("metadatas", []):
                try:
                    join_tables = json.loads(meta.get("join_tables", "[]"))
                except Exception:
                    join_tables = []
                output.append({
                    "column": meta.get("column", ""),
                    "data_type": meta.get("data_type", ""),
                    "join_tables": join_tables,
                    "n_tables": len(join_tables),
                })
            return sorted(output, key=lambda x: x["n_tables"], reverse=True)
        except Exception:
            return []

    def format_for_agent(self, results: list[dict]) -> str:
        """
        Format profiled catalog results as structured text for a Claude prompt.

        Includes business description, data profile, and join information.
        Agents use this to reason about whether to surface an existing column
        or propose a new one — grounded in real data, not static docs.
        """
        if not results:
            return (
                "DATA CATALOG: No similar columns found in the profiled catalog.\n"
                "This appears to be a greenfield column — not yet present in bronze, silver, or gold."
            )

        lines = [
            "DATA CATALOG (derived from live Databricks data profiling + AI-generated descriptions):",
            "(Business descriptions are generated from actual data values — not static documentation.)",
            "",
        ]
        for r in results:
            lines.append(f"Layer: {r['layer']} | Table: {r['table']} | Column: {r['column']}")
            lines.append(f"Type: {r['data_type']} | Null %: {r['null_pct']} | Cardinality: {r['cardinality']}")
            if r["sample_values"]:
                samples = ", ".join(str(v) for v in r["sample_values"][:3])
                lines.append(f"Sample values: {samples}")
            lines.append(f"Description: {r['description']}")
            if r["is_join_key"] and r["join_tables"]:
                tables_str = ", ".join(r["join_tables"][:4])
                lines.append(f"JOIN KEY — links: {tables_str}")
            lines.append(f"Similarity: {r['score']}")
            lines.append("")

        return "\n".join(lines)

    def format_join_map(self) -> str:
        """Format all join keys as a concise text for agent context."""
        join_keys = self.search_join_keys()
        if not join_keys:
            return "JOIN MAP: Not available (run sml profile first)."

        lines = ["JOIN MAP (auto-detected from data profiling):", ""]
        for jk in join_keys[:20]:
            tables = " ↔ ".join(t.split(".")[-1] for t in jk["join_tables"][:6])
            lines.append(f"  {jk['column']} ({jk['data_type']}) — links {jk['n_tables']} tables: {tables}")
        return "\n".join(lines)
