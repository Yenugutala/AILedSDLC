from __future__ import annotations
"""
demo/knowledge/index_state.py

Tracks which Jira tickets and whether the codebase has been indexed into ChromaDB.
State is stored in .chroma/index_state.json — gitignored, local-only.

Usage:
  state = IndexState(chroma_path)
  if not state.is_ticket_indexed("SCRUM-5"):
      index_jira_ticket(...)
      state.mark_ticket_indexed("SCRUM-5")
"""

import json
from pathlib import Path


class IndexState:
    def __init__(self, chroma_path: Path):
        self._path = Path(chroma_path) / "index_state.json"
        if self._path.exists():
            try:
                self._data = json.loads(self._path.read_text())
            except Exception:
                self._data = {}
        else:
            self._data = {}

    def is_codebase_indexed(self) -> bool:
        return self._data.get("codebase_indexed", False)

    def is_ticket_indexed(self, key: str) -> bool:
        return key in self._data.get("tickets", [])

    def mark_codebase_indexed(self) -> None:
        self._data["codebase_indexed"] = True
        self._save()

    def mark_ticket_indexed(self, key: str) -> None:
        tickets = self._data.setdefault("tickets", [])
        if key not in tickets:
            tickets.append(key)
        self._save()

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(self._data, indent=2))
