from __future__ import annotations
"""
setup_utils.py
Local SDK-based setup manager — runs from your laptop, no Spark required.

Provisions the Databricks workspace for the Securities Master pipeline:
  1. Creates Unity Catalog + 4 schemas (b_statestreet, s_statestreet, g_statestreet, securities_master)
  2. Creates the raw_files Volume
  3. Uploads local CSV files to /Volumes/statestreet/securities_master/raw_files/
  4. Verifies the setup

Credentials are read from environment variables (set in .env):
  DATABRICKS_HOST          e.g. https://dbc-09f2622e-69d1.cloud.databricks.com
  DATABRICKS_TOKEN         Personal Access Token
  DATABRICKS_WAREHOUSE_ID  SQL Warehouse ID for executing DDL statements

Usage:
    from src.common.setup_utils import SetupManager
    mgr = SetupManager()
    mgr.create_schemas()
    mgr.create_volume()
    mgr.upload_files("/path/to/local/csv/folder")
    mgr.verify()
"""

import os
import time
from pathlib import Path
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from rich.console import Console
from rich.table import Table

console = Console()

CATALOG       = "statestreet"
BRONZE_SCHEMA = "b_statestreet"
SILVER_SCHEMA = "s_statestreet"
GOLD_SCHEMA   = "g_statestreet"
VOLUME_SCHEMA = "securities_master"
VOLUME_NAME   = "raw_files"
VOLUME_PATH   = f"/Volumes/{CATALOG}/{VOLUME_SCHEMA}/{VOLUME_NAME}"

_SETUP_SQL = [
    f"CREATE CATALOG IF NOT EXISTS {CATALOG}",
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}",
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SILVER_SCHEMA}",
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}",
    f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{VOLUME_SCHEMA}",
    f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{VOLUME_SCHEMA}.{VOLUME_NAME}",
    # Schema drift audit tables (Bronze)
    f"""CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}._schema_changes (
        table_name STRING, change_type STRING,
        columns_affected STRING, detected_at TIMESTAMP
    ) USING DELTA""",
    f"""CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}._schema_quarantine (
        batch_id STRING, table_name STRING, change_type STRING,
        columns_affected STRING, quarantined_at TIMESTAMP
    ) USING DELTA""",
    # Audit log (Gold)
    f"""CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_SCHEMA}.audit_log (
        run_id STRING, use_case_name STRING, job_name STRING, stage STRING,
        started_at TIMESTAMP, completed_at TIMESTAMP,
        rows_read LONG, rows_written LONG, rows_rejected LONG,
        status STRING, triggered_by STRING
    ) USING DELTA""",
]


class SetupManager:
    """
    Provisions Databricks infra for the Securities Master pipeline.
    Reads credentials from environment variables unless overridden in constructor.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        warehouse_id: Optional[str] = None,
    ):
        self.host         = host  or os.environ.get("DATABRICKS_HOST", "").rstrip("/")
        self.token        = token or os.environ.get("DATABRICKS_TOKEN", "")
        self.warehouse_id = warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

        if not self.host or not self.token:
            raise ValueError(
                "DATABRICKS_HOST and DATABRICKS_TOKEN must be set "
                "(in .env or passed to SetupManager)."
            )

        self._client = WorkspaceClient(host=self.host, token=self.token)
        console.print(f"[dim]  Connected to: {self.host}[/]")

    # ─────────────────────────────────────────────────────────────────────────
    # PUBLIC API
    # ─────────────────────────────────────────────────────────────────────────

    def create_schemas(self) -> None:
        """Execute all DDL: catalog, schemas, volume, audit tables."""
        console.print("\n[bold cyan][SETUP][/] Creating catalog, schemas, and volume...")

        if not self.warehouse_id:
            raise ValueError(
                "DATABRICKS_WAREHOUSE_ID must be set to execute SQL statements. "
                "Find your warehouse ID in Databricks UI → SQL Warehouses."
            )

        for sql in _SETUP_SQL:
            label = sql.strip().split("\n")[0][:72]
            self._exec_sql(sql, label=label)

        console.print("[green]  ✓ All schemas and tables created.[/]")

    def create_volume(self) -> None:
        """Alias — volume is already created in create_schemas(). Kept for explicitness."""
        console.print(f"[green]  ✓ Volume ready: {VOLUME_PATH}[/]")

    def upload_files(self, local_dir: str) -> int:
        """
        Upload all CSV files from local_dir to the Databricks Volume.

        Args:
            local_dir: Path to local folder containing CSV files.

        Returns:
            Number of files uploaded.
        """
        src = Path(local_dir)
        if not src.exists():
            raise FileNotFoundError(f"local_dir not found: {local_dir}")

        csv_files = sorted(src.glob("*.csv"))
        if not csv_files:
            console.print(f"[yellow]  ⚠ No CSV files found in {local_dir}[/]")
            return 0

        console.print(f"\n[bold cyan][SETUP][/] Uploading {len(csv_files)} CSV files to {VOLUME_PATH}...")

        uploaded = 0
        for csv_path in csv_files:
            dest = f"{VOLUME_PATH}/{csv_path.name}"
            try:
                with open(csv_path, "rb") as f:
                    self._client.files.upload(file_path=dest, contents=f, overwrite=True)
                console.print(f"  [green]✓[/] {csv_path.name}")
                uploaded += 1
            except Exception as e:
                console.print(f"  [red]✗ {csv_path.name}: {e}[/]")

        console.print(f"[green]  ✓ Uploaded {uploaded}/{len(csv_files)} files.[/]")
        return uploaded

    def verify(self) -> dict:
        """
        List files in Volume and return a summary dict.

        Returns:
            dict with: volume_path, file_count, files (list of names)
        """
        console.print(f"\n[bold cyan][SETUP][/] Verifying Volume contents...")
        try:
            items = list(self._client.files.list_directory_contents(VOLUME_PATH))
            csv_files = [i.path.split("/")[-1] for i in items if i.path.endswith(".csv")]

            table = Table(title=f"Volume: {VOLUME_PATH}", show_lines=False)
            table.add_column("File", style="cyan")
            table.add_column("Status", style="green")
            for name in csv_files:
                table.add_row(name, "✓ present")
            console.print(table)

            summary = {
                "volume_path": VOLUME_PATH,
                "file_count":  len(csv_files),
                "files":       csv_files,
            }

            if len(csv_files) >= 29:
                console.print(f"[bold green]  ✅ All {len(csv_files)} CSV files verified.[/]")
            else:
                console.print(f"[yellow]  ⚠ {len(csv_files)} files found (expected 29).[/]")

            return summary

        except Exception as e:
            console.print(f"[red]  ✗ Could not list Volume: {e}[/]")
            return {"volume_path": VOLUME_PATH, "file_count": 0, "files": []}

    # ─────────────────────────────────────────────────────────────────────────
    # PRIVATE HELPERS
    # ─────────────────────────────────────────────────────────────────────────

    def _exec_sql(self, sql: str, label: str = "") -> None:
        """Execute a SQL statement via the SQL Warehouse and wait for completion."""
        display = label or sql[:60]
        try:
            response = self._client.statement_execution.execute_statement(
                statement=sql,
                warehouse_id=self.warehouse_id,
                wait_timeout="30s",
            )
            state = response.status.state

            # Poll if still running after initial wait
            stmt_id = response.statement_id
            for _ in range(20):
                if state in (StatementState.SUCCEEDED,):
                    break
                if state in (StatementState.FAILED, StatementState.CANCELED, StatementState.CLOSED):
                    err = response.status.error
                    raise RuntimeError(f"SQL failed [{state}]: {err.message if err else 'unknown'}")
                time.sleep(2)
                response = self._client.statement_execution.get_statement(stmt_id)
                state = response.status.state

            console.print(f"  [green]✓[/] {display}")
        except Exception as e:
            # Ignore "already exists" errors — setup is idempotent
            msg = str(e).lower()
            if "already exists" in msg or "cannot create" in msg:
                console.print(f"  [dim]~ already exists: {display.split('(')[0].strip()}[/]")
            else:
                raise
