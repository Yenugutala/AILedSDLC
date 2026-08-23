from __future__ import annotations
"""
demo/tools/schema_catalog.py
SchemaCatalog — vector search over the live Databricks schema catalog.

The schema_catalog ChromaDB collection is built by schema_discovery_agent.run()
(via `sml schema` or `sml index`).  Each document represents one column from
the bronze, silver, or gold layers of the statestreet Unity Catalog.

Used by Beat 3 Check 3 to answer: "does this required column already exist?
Should we surface it from silver, or create a new one?"
"""

from pathlib import Path

_COLLECTION_NAME = "schema_catalog"
_EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"


class SchemaCatalog:
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
        """Return True if the schema catalog exists and has indexed columns."""
        self._ensure_loaded()
        if self._collection is None:
            return False
        try:
            return self._collection.count() > 0
        except Exception:
            return False

    def search(self, query: str, n: int = 10) -> list[dict]:
        """
        Semantic vector search for existing columns similar to query.

        Returns list of dicts:
          [{table, column, layer, data_type, comment, nullable, score, document}]

        Sorted by descending similarity score (1.0 = identical, 0.0 = unrelated).
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
            # ChromaDB returns L2 or cosine distance. Convert to 0–1 similarity.
            score = max(0.0, 1.0 - distance)
            output.append({
                "table":     meta.get("table", ""),
                "column":    meta.get("column", ""),
                "layer":     meta.get("layer", ""),
                "data_type": meta.get("data_type", ""),
                "comment":   meta.get("comment", ""),
                "nullable":  meta.get("nullable", "True") == "True",
                "score":     round(score, 3),
                "document":  doc,
            })
        return output

    def format_for_agent(self, results: list[dict]) -> str:
        """
        Format search results as structured text for a Claude system/user prompt.

        Produces output like:
            EXISTING SCHEMA CATALOG (from live Databricks INFORMATION_SCHEMA):

            Layer: silver | Table: statestreet.s_statestreet.bond | Column: net_settlement_amount
            Type: DECIMAL(18,6) | Nullable: True | Comment: Net cash settlement amount
            Similarity: 0.941

            ...
        """
        if not results:
            return (
                "EXISTING SCHEMA CATALOG: No similar columns found in the live Databricks schema.\n"
                "This appears to be a greenfield column — it does not exist in bronze or silver."
            )

        lines = [
            "EXISTING SCHEMA CATALOG (from live Databricks INFORMATION_SCHEMA):",
            "(These are real columns discovered from your statestreet Unity Catalog.)",
            "",
        ]
        for r in results:
            lines.append(
                f"Layer: {r['layer']} | Table: {r['table']} | Column: {r['column']}"
            )
            lines.append(
                f"Type: {r['data_type']} | Nullable: {r['nullable']} | Comment: {r['comment'] or '(no comment)'}"
            )
            lines.append(f"Similarity score: {r['score']}")
            lines.append("")

        return "\n".join(lines)
