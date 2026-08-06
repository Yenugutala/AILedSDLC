from __future__ import annotations
"""
setup_agent.py
Setup Agent — provisions Databricks workspace for a Securities Master pipeline.

Runs LOCALLY (no Spark). Connects to Databricks via SDK using env credentials.

Steps:
  1. Create Unity Catalog + schemas (b_statestreet, s_statestreet, g_statestreet, securities_master)
  2. Create Volume: /Volumes/statestreet/securities_master/raw_files/
  3. Upload CSV files from local_data_dir (if provided)
  4. Verify setup and print summary

Called by: sml setup --local-data-dir <path>
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel

console = Console()

# Load .env from repo root so DATABRICKS_* vars are available
_REPO_ROOT = Path(__file__).parent.parent.parent
_ENV_FILE  = _REPO_ROOT / ".env"
if _ENV_FILE.exists():
    load_dotenv(_ENV_FILE)


def run(local_data_dir: str | None = None) -> dict:
    """
    Provision Databricks infra and optionally upload CSV files.

    Args:
        local_data_dir: Local folder containing CSV files. If None, skips upload.

    Returns:
        Summary dict: {schemas_created, volume_created, files_uploaded, volume_path}
    """
    console.print(Panel(
        "[bold]Securities Master — Databricks Setup[/]\n"
        "Provisioning catalog, schemas, volume, and uploading source files.",
        title="[SML] SETUP AGENT",
        expand=False,
    ))

    # Import here to keep startup fast when not using setup
    # Add project/ to path so 'src' package is resolvable
    _PROJECT_ROOT = Path(__file__).parent.parent.parent / "project"
    if str(_PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(_PROJECT_ROOT))
    from src.common.setup_utils import SetupManager

    # Validate credentials before proceeding
    host  = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    wh_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

    if not host or not token:
        console.print(
            "[red][ERROR][/] DATABRICKS_HOST and DATABRICKS_TOKEN must be set.\n"
            f"Edit [bold]{_ENV_FILE}[/] and add your credentials, then re-run."
        )
        sys.exit(1)

    if not wh_id:
        console.print(
            "[yellow][WARN][/] DATABRICKS_WAREHOUSE_ID is not set.\n"
            "DDL statements (CREATE CATALOG / SCHEMA / VOLUME) will be skipped.\n"
            "Set DATABRICKS_WAREHOUSE_ID in .env to provision infra automatically."
        )

    mgr = SetupManager(host=host, token=token, warehouse_id=wh_id)

    # ── 1. Create schemas and volume ─────────────────────────────────────────
    schemas_created = False
    if wh_id:
        try:
            mgr.create_schemas()
            schemas_created = True
        except Exception as e:
            console.print(f"[red][ERROR] Schema creation failed: {e}[/]")
            sys.exit(1)
    else:
        console.print("[dim]  Skipping schema creation (no warehouse ID)[/]")

    # ── 2. Upload CSV files ───────────────────────────────────────────────────
    files_uploaded = 0
    if local_data_dir:
        try:
            files_uploaded = mgr.upload_files(local_data_dir)
        except FileNotFoundError as e:
            console.print(f"[red][ERROR] {e}[/]")
            sys.exit(1)
        except Exception as e:
            console.print(f"[red][ERROR] File upload failed: {e}[/]")
            sys.exit(1)
    else:
        console.print(
            "\n[dim]  --local-data-dir not provided. Skipping file upload.\n"
            "  Upload CSV files manually via Databricks UI:\n"
            "  Catalog → statestreet → securities_master → raw_files → Upload[/]"
        )

    # ── 3. Verify ────────────────────────────────────────────────────────────
    summary = mgr.verify()

    # ── 4. Print next steps ──────────────────────────────────────────────────
    console.print(Panel(
        "[bold green]✅ Setup complete![/]\n\n"
        "Next steps:\n"
        "  1. Fill in request.yaml for your use case\n"
        "  2. Run: [bold]sml generate --use-case securities-master[/]\n"
        "     Agents will generate notebooks → create a GitHub PR\n"
        "  3. Merge the PR, then run:\n"
        "     [bold]sml deploy --use-case securities-master[/]\n"
        "     [bold]sml run --use-case securities-master --job bronze_ingest_job[/]",
        title="[SML] Setup Complete",
        expand=False,
    ))

    return {
        "schemas_created": schemas_created,
        "volume_created":  schemas_created,
        "files_uploaded":  files_uploaded,
        "volume_path":     summary.get("volume_path"),
        "file_count":      summary.get("file_count", 0),
    }
