from __future__ import annotations
"""
demo/knowledge/retriever.py
Semantic search over ChromaDB. Used by beats and the knowledge agent.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import chromadb
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction

from demo.knowledge.indexer import get_collection


@dataclass
class SearchResult:
    text: str
    source: str
    source_type: str
    distance: float

    def format(self) -> str:
        return f"[{self.source}]\n{self.text}"


class Retriever:
    def __init__(self, chroma_path: Path):
        self._collection = get_collection(chroma_path)

    def search(self, query: str, n: int = 5, source_type: Optional[str] = None) -> list[SearchResult]:
        """Return top-n semantically similar chunks."""
        where = {"source_type": source_type} if source_type else None
        kwargs = {"query_texts": [query], "n_results": min(n, max(1, self._collection.count()))}
        if where:
            kwargs["where"] = where
        try:
            results = self._collection.query(**kwargs)
        except Exception:
            return []

        hits = []
        docs = results["documents"][0]
        metas = results["metadatas"][0]
        dists = results["distances"][0]
        for doc, meta, dist in zip(docs, metas, dists):
            hits.append(SearchResult(
                text=doc,
                source=meta.get("source", ""),
                source_type=meta.get("source_type", ""),
                distance=dist,
            ))
        return hits

    def format_context(self, results: list[SearchResult]) -> str:
        parts = []
        for r in results:
            parts.append(f"--- Source: {r.source} ---\n{r.text}")
        return "\n\n".join(parts)
