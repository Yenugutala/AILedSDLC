from __future__ import annotations
"""
demo/config.py
Load and validate all environment variables needed by the demo harness.
"""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

# Load .env from repo root (two levels up from agentic/demo/)
_REPO_ROOT = Path(__file__).parent.parent.parent
load_dotenv(_REPO_ROOT / ".env")


@dataclass(frozen=True)
class DemoConfig:
    # Jira
    jira_url: str
    jira_username: str
    jira_api_token: str

    # Anthropic
    anthropic_api_key: str

    # Databricks
    databricks_host: str
    databricks_token: str
    databricks_warehouse_id: str
    genie_space_id: str

    # Langfuse observability (optional)
    langfuse_public_key: str
    langfuse_secret_key: str
    langfuse_host: str

    # Paths
    repo_root: Path
    chroma_path: Path


def load() -> DemoConfig:
    def _require(key: str) -> str:
        val = os.environ.get(key, "").strip()
        if not val:
            raise RuntimeError(f"Missing required env var: {key}  (add it to .env)")
        return val

    return DemoConfig(
        jira_url=_require("JIRA_URL"),
        jira_username=_require("JIRA_USERNAME"),
        jira_api_token=_require("JIRA_API_TOKEN"),
        anthropic_api_key=_require("ANTHROPIC_API_KEY"),
        databricks_host=_require("DATABRICKS_HOST"),
        databricks_token=_require("DATABRICKS_TOKEN"),
        databricks_warehouse_id=os.environ.get("DATABRICKS_WAREHOUSE_ID", ""),
        genie_space_id=os.environ.get("GENIE_SPACE_ID", ""),
        langfuse_public_key=os.environ.get("LANGFUSE_PUBLIC_KEY", ""),
        langfuse_secret_key=os.environ.get("LANGFUSE_SECRET_KEY", ""),
        langfuse_host=os.environ.get("LANGFUSE_HOST", "https://cloud.langfuse.com"),
        repo_root=_REPO_ROOT,
        chroma_path=_REPO_ROOT / ".chroma",
    )
