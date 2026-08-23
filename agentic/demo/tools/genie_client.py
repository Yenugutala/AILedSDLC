from __future__ import annotations
"""
demo/tools/genie_client.py
Databricks Genie REST API client — polls until result is ready.
"""

import time
from dataclasses import dataclass
from typing import Optional

import requests


@dataclass
class GenieResult:
    question: str
    answer_text: str
    sql_query: Optional[str]
    row_count: int


class GenieClient:
    def __init__(self, host: str, token: str, space_id: str):
        self.host = host.rstrip("/")
        self.space_id = space_id
        self._headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def query(self, question: str, timeout_s: int = 90) -> GenieResult:
        """Send a natural-language question and poll until complete."""
        # Start conversation
        r = requests.post(
            f"{self.host}/api/2.0/genie/spaces/{self.space_id}/start-conversation",
            headers=self._headers,
            json={"content": question},
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        conv_id = data["conversation_id"]
        msg_id = data["message_id"]

        # Poll
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            r = requests.get(
                f"{self.host}/api/2.0/genie/spaces/{self.space_id}"
                f"/conversations/{conv_id}/messages/{msg_id}",
                headers=self._headers,
                timeout=15,
            )
            r.raise_for_status()
            msg = r.json()
            status = msg.get("status", "")

            if status == "COMPLETED":
                return self._parse_result(question, msg)
            if status in ("FAILED", "CANCELLED"):
                raise RuntimeError(f"Genie query {status}: {msg.get('error', '')}")
            time.sleep(2)

        raise TimeoutError(f"Genie query timed out after {timeout_s}s")

    def _parse_result(self, question: str, msg: dict) -> GenieResult:
        answer_text = ""
        sql_query = None
        row_count = 0

        for att in msg.get("attachments", []):
            if att.get("text"):
                answer_text = att["text"].get("content", "")
            if att.get("query"):
                q = att["query"]
                sql_query = q.get("query", "")
                row_count = q.get("row_count", 0)

        if not answer_text and not sql_query:
            answer_text = str(msg)

        return GenieResult(
            question=question,
            answer_text=answer_text,
            sql_query=sql_query,
            row_count=row_count,
        )
