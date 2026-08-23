from __future__ import annotations
"""
demo/knowledge/indexer.py
Indexes the full codebase, spec YAMLs, and a Jira ticket into ChromaDB.
Run via: sml index
"""

import hashlib
import re
from pathlib import Path
from typing import Optional

import yaml
import chromadb
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()


def _ef():
    return SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")


def get_collection(chroma_path: Path) -> chromadb.Collection:
    client = chromadb.PersistentClient(path=str(chroma_path))
    return client.get_or_create_collection(
        name="ai-dlc",
        embedding_function=_ef(),
        metadata={"hnsw:space": "cosine"},
    )


def _chunk_text(text: str, source: str, max_chars: int = 800) -> list[dict]:
    """Split text into overlapping chunks."""
    chunks = []
    lines = text.splitlines()
    current, current_len = [], 0

    for line in lines:
        current.append(line)
        current_len += len(line) + 1
        if current_len >= max_chars:
            chunk_text = "\n".join(current).strip()
            if chunk_text:
                chunks.append({"text": chunk_text, "source": source})
            # 20% overlap
            overlap = current[max(0, len(current) - len(current) // 5):]
            current = list(overlap)
            current_len = sum(len(l) + 1 for l in current)

    if current:
        chunk_text = "\n".join(current).strip()
        if chunk_text:
            chunks.append({"text": chunk_text, "source": source})
    return chunks


def _uid(text: str, source: str, idx: int) -> str:
    return hashlib.md5(f"{source}:{idx}:{text[:50]}".encode()).hexdigest()


def index_file(collection: chromadb.Collection, path: Path, repo_root: Path, source_type: str):
    try:
        text = path.read_text(errors="ignore").strip()
    except OSError:
        return 0
    if not text:
        return 0

    rel = str(path.relative_to(repo_root))
    chunks = _chunk_text(text, rel)

    ids, docs, metas = [], [], []
    for i, c in enumerate(chunks):
        ids.append(_uid(c["text"], rel, i))
        docs.append(c["text"])
        metas.append({"source": rel, "source_type": source_type})

    if ids:
        collection.upsert(ids=ids, documents=docs, metadatas=metas)
    return len(ids)


def index_jira_ticket(collection: chromadb.Collection, ticket_key: str, ticket_text: str):
    chunks = _chunk_text(ticket_text, f"jira:{ticket_key}")
    ids = [_uid(c["text"], f"jira:{ticket_key}", i) for i, c in enumerate(chunks)]
    docs = [c["text"] for c in chunks]
    metas = [{"source": f"jira:{ticket_key}", "source_type": "jira"} for _ in chunks]
    if ids:
        collection.upsert(ids=ids, documents=docs, metadatas=metas)
    return len(ids)


def run(repo_root: Path, chroma_path: Path, ticket_key: Optional[str] = None, ticket_text: Optional[str] = None) -> int:
    """Index everything. Returns total chunk count."""
    collection = get_collection(chroma_path)

    patterns_and_types = [
        ("project/use-cases/**/*.yaml", "spec"),
        ("project/use-cases/**/*.md",   "doc"),
        ("project/notebooks/*.py",       "code"),
        ("project/notebooks/*.sql",      "code"),
        ("project/tests/*.py",           "test"),
        ("project/docs/*.md",            "doc"),
        ("project/specs/*.md",           "doc"),
        ("project/skills/*.md",          "knowledge"),
        ("CLAUDE.md",                    "standards"),
    ]

    total = 0
    with Progress(SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console) as p:
        task = p.add_task("Indexing codebase...", total=None)

        for pattern, src_type in patterns_and_types:
            for fpath in repo_root.glob(pattern):
                n = index_file(collection, fpath, repo_root, src_type)
                total += n
                p.update(task, description=f"Indexed {total} chunks... ({fpath.name})")

        if ticket_key and ticket_text:
            n = index_jira_ticket(collection, ticket_key, ticket_text)
            total += n
            p.update(task, description=f"Indexed Jira {ticket_key} ({n} chunks)")

    console.print(f"[green]  ✓ Indexed {total} chunks into ChromaDB at {chroma_path}[/]")
    return total
