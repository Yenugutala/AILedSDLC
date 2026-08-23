from __future__ import annotations
"""
demo/tools/jira_client.py
Direct Jira REST API v3 client — no MCP dependency.
Works standalone when the demo harness runs as a CLI process.
All comments are posted as rich Atlassian Document Format (ADF).
"""

import re
from base64 import b64encode
from dataclasses import dataclass
import requests


@dataclass
class JiraTicket:
    key: str
    summary: str
    description: str
    status: str
    priority: str
    assignee: str
    labels: list[str]
    requirements: list[Requirement]


@dataclass
class Requirement:
    id: str       # e.g. "REQ-01"
    text: str


class JiraClient:
    def __init__(self, url: str, username: str, api_token: str):
        self.base_url = url.rstrip("/")
        creds = b64encode(f"{username}:{api_token}".encode()).decode()
        self._headers = {
            "Authorization": f"Basic {creds}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _get(self, path: str) -> dict:
        r = requests.get(f"{self.base_url}{path}", headers=self._headers, timeout=15)
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, payload: dict) -> dict:
        r = requests.post(f"{self.base_url}{path}", headers=self._headers, json=payload, timeout=15)
        r.raise_for_status()
        return r.json()

    def _put(self, path: str, payload: dict) -> None:
        r = requests.put(f"{self.base_url}{path}", headers=self._headers, json=payload, timeout=15)
        r.raise_for_status()

    def get_open_tickets(self, project_key: str) -> list[JiraTicket]:
        """Return all open tickets in the project."""
        jql = (
            f"project = {project_key} "
            f"AND status != Done "
            f"ORDER BY priority DESC, created DESC"
        )
        data = self._get(
            f"/rest/api/3/search/jql?jql={requests.utils.quote(jql)}"
            "&fields=summary,status,priority,assignee,description,labels"
            "&maxResults=20"
        )
        tickets = []
        for issue in data.get("issues", []):
            f = issue["fields"]
            desc_raw = f.get("description")
            desc_text = _adf_to_text(desc_raw) if isinstance(desc_raw, dict) else (desc_raw or "")
            tickets.append(JiraTicket(
                key=issue["key"],
                summary=f.get("summary", ""),
                description=desc_text,
                status=(f.get("status") or {}).get("name", ""),
                priority=(f.get("priority") or {}).get("name", ""),
                assignee=((f.get("assignee") or {}).get("displayName") or "Unassigned"),
                labels=f.get("labels", []),
                requirements=_parse_requirements(desc_text),
            ))
        return tickets

    def get_issue(self, key: str) -> JiraTicket:
        data = self._get(
            f"/rest/api/3/issue/{key}"
            "?fields=summary,status,priority,assignee,description,labels"
        )
        f = data["fields"]
        desc_raw = f.get("description")
        desc_text = _adf_to_text(desc_raw) if isinstance(desc_raw, dict) else (desc_raw or "")
        return JiraTicket(
            key=data["key"],
            summary=f.get("summary", ""),
            description=desc_text,
            status=f.get("status", {}).get("name", ""),
            priority=f.get("priority", {}).get("name", ""),
            assignee=((f.get("assignee") or {}).get("displayName") or "Unassigned"),
            labels=f.get("labels", []),
            requirements=_parse_requirements(desc_text),
        )

    def add_comment(self, key: str, body: str) -> str:
        """Post a plain-text comment (auto-converted to ADF) and return the comment ID."""
        data = self._post(
            f"/rest/api/3/issue/{key}/comment",
            {"body": _text_to_adf(body)},
        )
        return data.get("id", "")

    def _post_adf_comment(self, key: str, adf: dict) -> str:
        """Post a pre-built ADF document as a comment and return the comment ID."""
        data = self._post(
            f"/rest/api/3/issue/{key}/comment",
            {"body": adf},
        )
        return data.get("id", "")

    def add_label(self, key: str, label: str) -> None:
        issue = self._get(f"/rest/api/3/issue/{key}?fields=labels")
        existing = issue["fields"].get("labels", [])
        if label not in existing:
            self._put(
                f"/rest/api/3/issue/{key}",
                {"fields": {"labels": existing + [label]}},
            )

    def post_clarification(
        self,
        key: str,
        question: str,
        answer: str,
        engineer_name: str = "Kiran Kumar Yenugutala",
    ) -> str:
        """Post a formatted clarification Q&A comment and return the comment ID."""
        adf = _adf_doc([
            _heading(3, "🤖 AI-DLC · BA Agent Clarification"),
            _paragraph([_plain(f"Answered by: "), _bold(engineer_name)]),
            _divider(),
            _paragraph([_bold("Q: "), _plain(question)]),
            _paragraph([_bold("A: "), _plain(answer)]),
        ])
        return self._post_adf_comment(key, adf)

    def post_build_ready_stamp(
        self,
        key: str,
        checks: list[dict],
        artifacts: list[str],
        session_id: str,
    ) -> str:
        """Post a formatted build-ready stamp comment and return the comment ID."""
        check_items = []
        for c in checks:
            icon = "✅ " if c["passed"] else "❌ "
            check_items.append([_plain(icon), _bold(c["name"] + ": "), _plain(c["detail"])])

        adf = _adf_doc([
            _heading(3, "🤖 AI-DLC · Build-Ready Stamp"),
            _paragraph([_bold("Session: "), _plain(session_id)]),
            _divider(),
            _heading(4, "Verification Checks"),
            _bullet_list(check_items),
            _heading(4, "Spec Artifacts"),
            _bullet_list([[_code(a)] for a in artifacts]),
        ])
        comment_id = self._post_adf_comment(key, adf)
        self.add_label(key, "build-ready")
        return comment_id

    def post_sign_off(
        self,
        key: str,
        notebook_path: str,
        test_path: str,
        req_matrix: list[str],
    ) -> str:
        """Post a formatted build sign-off comment and return the comment ID."""
        adf = _adf_doc([
            _heading(3, "🤖 AI-DLC · Build Sign-Off"),
            _paragraph([_bold("Notebook: "), _code(notebook_path)]),
            _paragraph([_bold("Tests: "), _code(test_path)]),
            _divider(),
            _heading(4, "Requirement Traceability"),
            _bullet_list([[_code(row)] for row in req_matrix]),
        ])
        return self._post_adf_comment(key, adf)


# ── ADF document builders ─────────────────────────────────────────────────────

def _adf_doc(nodes: list[dict]) -> dict:
    return {"type": "doc", "version": 1, "content": nodes}


def _heading(level: int, text: str) -> dict:
    return {
        "type": "heading",
        "attrs": {"level": level},
        "content": [{"type": "text", "text": text}],
    }


def _paragraph(inline_nodes: list[dict]) -> dict:
    return {"type": "paragraph", "content": inline_nodes}


def _divider() -> dict:
    return {"type": "rule"}


def _bullet_list(items: list[list[dict]]) -> dict:
    """Each item is a list of inline nodes (e.g. [_bold('x'), _plain('y')])."""
    return {
        "type": "bulletList",
        "content": [
            {
                "type": "listItem",
                "content": [{"type": "paragraph", "content": inline}],
            }
            for inline in items
        ],
    }


# ── Inline node helpers ───────────────────────────────────────────────────────

def _bold(text: str) -> dict:
    return {"type": "text", "text": text, "marks": [{"type": "strong"}]}


def _plain(text: str) -> dict:
    return {"type": "text", "text": text}


def _code(text: str) -> dict:
    return {"type": "text", "text": text, "marks": [{"type": "code"}]}


# ── ADF read helpers ──────────────────────────────────────────────────────────

def _adf_to_text(adf: dict) -> str:
    """Recursively extract plain text from an Atlassian Document Format node."""
    if not adf or not isinstance(adf, dict):
        return ""
    node_type = adf.get("type", "")
    parts = []
    if node_type == "text":
        parts.append(adf.get("text", ""))
    for child in adf.get("content", []):
        parts.append(_adf_to_text(child))
    text = "".join(parts)
    if node_type in ("paragraph", "heading", "bulletList", "orderedList", "listItem", "tableRow"):
        text = text + "\n"
    if node_type in ("tableCell", "tableHeader"):
        text = text.strip() + " | "
    return text


def _text_to_adf(text: str) -> dict:
    """Convert plain text with optional *bold* markers to ADF."""
    content = []
    for line in text.split("\n"):
        if line.strip():
            content.append(_paragraph(_parse_inline(line)))
        else:
            content.append(_paragraph([]))
    return _adf_doc(content or [_paragraph([])])


def _parse_inline(text: str) -> list[dict]:
    """Parse *bold* markers into ADF inline nodes."""
    nodes = []
    for idx, part in enumerate(re.split(r"\*([^*]+)\*", text)):
        if not part:
            continue
        if idx % 2 == 1:  # between * markers → bold
            nodes.append(_bold(part))
        else:
            nodes.append(_plain(part))
    return nodes or [_plain("")]


# ── Requirement parser ────────────────────────────────────────────────────────

def _parse_requirements(description: str) -> list[Requirement]:
    """Extract REQ-NN rows from the description (plain text)."""
    reqs = []
    pattern = re.compile(r"\|?\s*(REQ-\d+)[^\|]*\|([^\|]+)\|?")
    for m in pattern.finditer(description):
        reqs.append(Requirement(id=m.group(1).strip(), text=m.group(2).strip()))
    if not reqs:
        pattern2 = re.compile(r"(REQ-\d+)[:\s]+(.+)")
        for m in pattern2.finditer(description):
            reqs.append(Requirement(id=m.group(1).strip(), text=m.group(2).strip()))
    return reqs
