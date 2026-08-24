from __future__ import annotations
"""
demo/tools/demo_state.py

Persistent demo session state per Jira ticket.
Stored in .chroma/demo_state/{ticket_key}.json — gitignored, local-only.

Enables resume after crash or mid-session exit:
  state = DemoState.load(chroma_path, "SCRUM-5")  # None if first run
  state = DemoState.new(chroma_path, "SCRUM-5", session_id)
  state.mark_done("beat2", chroma_path, question="...", answer="...")
  if state.is_done("beat2"):
      clarification = ClarificationContext(...)  # reconstruct from artifacts
"""

import json
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional


@dataclass
class DemoState:
    ticket_key: str
    session_id: str
    completed_beats: list = field(default_factory=list)
    artifacts: dict = field(default_factory=dict)
    updated_at: str = ""

    # ── Factory methods ───────────────────────────────────────────────────────

    @classmethod
    def load(cls, chroma_path: Path, ticket_key: str) -> Optional["DemoState"]:
        """Load existing state for ticket_key. Returns None if no state exists."""
        path = _state_path(chroma_path, ticket_key)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text())
            return cls(**data)
        except Exception:
            return None

    @classmethod
    def new(cls, chroma_path: Path, ticket_key: str, session_id: str) -> "DemoState":
        """Create a fresh state for a new session."""
        return cls(ticket_key=ticket_key, session_id=session_id)

    # ── Persistence ───────────────────────────────────────────────────────────

    def save(self, chroma_path: Path) -> None:
        path = _state_path(chroma_path, self.ticket_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.updated_at = time.strftime("%Y-%m-%dT%H:%M:%S")
        path.write_text(json.dumps(asdict(self), indent=2))

    # ── Beat tracking ─────────────────────────────────────────────────────────

    def is_done(self, beat_id: str) -> bool:
        return beat_id in self.completed_beats

    def mark_done(self, beat_id: str, chroma_path: Path, **artifacts: Any) -> None:
        if beat_id not in self.completed_beats:
            self.completed_beats.append(beat_id)
        self.artifacts[beat_id] = artifacts
        self.save(chroma_path)

    def get(self, beat_id: str, key: str, default: Any = None) -> Any:
        return self.artifacts.get(beat_id, {}).get(key, default)

    def last_completed(self) -> str | None:
        return self.completed_beats[-1] if self.completed_beats else None


def _state_path(chroma_path: Path, ticket_key: str) -> Path:
    return Path(chroma_path) / "demo_state" / f"{ticket_key}.json"
