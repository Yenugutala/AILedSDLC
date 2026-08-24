"""
sml — Securities Master Lakehouse CLI
Entry point for the `sml` terminal command (defined in pyproject.toml).

Usage:
    sml setup    --local-data-dir ./data/
    sml generate --use-case securities-master
    sml debug    --use-case securities-master --job silver_conform_job
    sml deploy   --use-case securities-master
    sml status   --use-case securities-master --job orchestrate_pipeline_job
    sml run      --use-case securities-master --job bronze_ingest_job
    sml validate --use-case securities-master

    # Demo commands
    sml index                          # Index codebase into ChromaDB
    sml schema                         # Index live Databricks schema catalog
    sml profile                        # Profile tables + generate AI column descriptions + join map
    sml demo                           # Run the 6-beat live demo
    sml ask                            # Knowledge agent REPL (audience Q&A)
    sml change "rename column x to y"  # Propose a code change via RAG
"""

import click
from dotenv import load_dotenv
from agents import pipeline, debug_agent, validate_spec_agent

load_dotenv()  # auto-load .env (DATABRICKS_HOST, DATABRICKS_TOKEN, ANTHROPIC_API_KEY)


@click.group()
@click.version_option(version="1.0.0", prog_name="sml")
def main():
    """Securities Master Lakehouse — BMAD agent pipeline CLI."""
    pass


@main.command()
@click.option(
    "--local-data-dir",
    default=None,
    help="Local folder containing CSV files to upload to Volume. "
         "Omit to provision infra only (no file upload).",
)
def setup(local_data_dir: str):
    """
    Provision Databricks workspace for the Securities Master pipeline.

    Creates Unity Catalog, Bronze/Silver/Gold schemas, and the raw_files Volume.
    Optionally uploads CSV files from --local-data-dir.

    Reads credentials from .env:
      DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID
    """
    from agents import setup_agent
    setup_agent.run(local_data_dir=local_data_dir)


@main.command()
@click.option(
    "--use-case",
    default="securities-master",
    show_default=True,
    help="Use-case name (folder under use-cases/).",
)
def generate(use_case: str):
    """
    Run the full BMAD agent loop for a use case.

    Reads use-cases/<name>/request.yaml automatically.
    Runs BA → Architect → Developer → QA → Doc agents with human approval gates.
    Creates a GitHub PR with the generated code when done.
    """
    pipeline.run(use_case_name=use_case)


@main.command()
@click.option("--use-case", default="securities-master", show_default=True, help="Use-case name")
@click.option("--job", required=True, help="Failing Databricks job name")
def debug(use_case: str, job: str):
    """
    Diagnose and fix a failing Databricks job.

    Fetches logs via Databricks MCP, classifies the failure,
    generates a fix, and raises a PR after human approval.
    """
    debug_agent.run(use_case_name=use_case, job_name=job)


@main.command()
@click.option("--use-case", default="securities-master", show_default=True, help="Use-case name")
def deploy(use_case: str):
    """
    Deploy the Databricks Asset Bundle to the workspace.

    Runs: databricks bundle validate → databricks bundle deploy
    """
    from agents.deploy_agent import deploy as _deploy
    _deploy(use_case_name=use_case)


@main.command()
@click.option("--use-case", default="securities-master", show_default=True, help="Use-case name")
@click.option("--job", required=True, help="Databricks job name")
def status(use_case: str, job: str):
    """Check the status of a running or completed Databricks job."""
    from agents.deploy_agent import get_job_status
    get_job_status(job_name=job)


@main.command()
@click.option("--use-case", default="securities-master", show_default=True, help="Use-case name")
@click.option("--job", required=True, help="Databricks job name to trigger")
def run(use_case: str, job: str):
    """Trigger a Databricks job run."""
    from agents.deploy_agent import trigger_job
    trigger_job(job_name=job)


@main.command()
@click.option("--use-case", default="securities-master", show_default=True, help="Use-case name")
def validate(use_case: str):
    """
    Validate all spec YAML files against CLAUDE.md naming rules.

    Run this in CI (validate-specs.yml) or locally before raising a PR.
    """
    validate_spec_agent.run(use_case_name=use_case)


# ── Demo commands ─────────────────────────────────────────────────────────────

@main.command()
def demo():
    """
    Run the 6-beat AI-DLC live demo.

    Shows your open Jira tickets, lets you select one, then runs:
    Pull Ticket → Clarify → Verify → Build → Genie → Observe.
    A live HTML dashboard auto-opens at http://localhost:8765.
    """
    from demo.harness import DemoHarness
    DemoHarness().run()


@main.command(name="index")
def index_cmd():
    """
    Index the full codebase, specs, and docs into ChromaDB.

    Run this once before `sml demo` to build the vector knowledge base.
    Also auto-runs `sml schema` to refresh the live Databricks schema catalog.
    The index is stored in .chroma/ at the repo root.
    """
    from demo.config import load
    from demo.knowledge.indexer import run as index_run
    from demo.knowledge.index_state import IndexState
    from rich import print as rprint
    conf = load()
    total = index_run(repo_root=conf.repo_root, chroma_path=conf.chroma_path)
    IndexState(conf.chroma_path).mark_codebase_indexed()
    rprint(f"[green]  ✓ Indexed {total} chunks into {conf.chroma_path}[/]")

    # Also refresh schema catalog if Databricks is configured
    if conf.databricks_host and conf.databricks_token and getattr(conf, "databricks_warehouse_id", None):
        rprint("[dim]  Auto-refreshing schema catalog from Databricks...[/]")
        from agents import schema_discovery_agent
        try:
            n = schema_discovery_agent.run(
                databricks_host=conf.databricks_host,
                databricks_token=conf.databricks_token,
                warehouse_id=conf.databricks_warehouse_id,
                chroma_path=conf.chroma_path,
            )
            rprint(f"[green]  ✓ Schema catalog: {n} columns indexed[/]")
        except Exception as e:
            rprint(f"[yellow]  ⚠ Schema catalog skipped: {e}[/]")
            rprint("[dim]  Run `sml schema` manually after configuring DATABRICKS_WAREHOUSE_ID[/]")


@main.command(name="schema")
def schema_cmd():
    """
    Index the live Databricks schema into ChromaDB.

    Queries INFORMATION_SCHEMA.COLUMNS for the statestreet catalog and
    builds a vector knowledge base of all columns (bronze + silver + gold).

    Beat 3 Check 3 uses this catalog to:
      - Detect if a required column already exists (action=surface)
      - Propose new column definitions for truly new columns (action=create)

    Requires .env:
      DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID
    """
    from demo.config import load
    from agents import schema_discovery_agent
    from rich import print as rprint
    conf = load()
    warehouse_id = getattr(conf, "databricks_warehouse_id", None)
    if not warehouse_id:
        rprint("[red]  DATABRICKS_WAREHOUSE_ID not set in .env — cannot query INFORMATION_SCHEMA[/]")
        raise SystemExit(1)
    n = schema_discovery_agent.run(
        databricks_host=conf.databricks_host,
        databricks_token=conf.databricks_token,
        warehouse_id=warehouse_id,
        chroma_path=conf.chroma_path,
    )
    rprint(f"[green]  ✓ Schema catalog ready: {n} columns indexed from statestreet catalog[/]")


@main.command(name="profile")
def profile_cmd():
    """
    Profile the live Databricks catalog — generate AI column descriptions + join map.

    For every column in statestreet.* (bronze, silver, gold):
      - Measures null %, cardinality, and sample values from real data
      - Sends the data profile to Claude → generates a business description
        based on ACTUAL data values (not static documentation)
      - Detects join keys: columns that appear in multiple tables
      - Builds a join map showing how tables relate to each other
      - Indexes everything into ChromaDB collection "data_catalog"
      - Writes join_map.yaml to use-cases/securities-master/

    Run after schema changes or whenever you want fresh AI-generated column descriptions.
    Requires: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID
    """
    from demo.config import load
    from agents import data_profiler_agent
    from rich import print as rprint
    conf = load()
    warehouse_id = getattr(conf, "databricks_warehouse_id", None)
    if not warehouse_id:
        rprint("[red]  DATABRICKS_WAREHOUSE_ID not set in .env[/]")
        raise SystemExit(1)
    n = data_profiler_agent.run(
        databricks_host=conf.databricks_host,
        databricks_token=conf.databricks_token,
        warehouse_id=warehouse_id,
        chroma_path=conf.chroma_path,
    )
    rprint(f"[green]  ✓ Data catalog ready: {n} columns profiled and indexed[/]")
    rprint("[dim]  join_map.yaml written to use-cases/securities-master/[/]")


@main.command(name="ask")
def ask_cmd():
    """
    Open the knowledge agent REPL.

    Ask any question about the codebase, specs, Jira tickets, or schemas.
    The agent retrieves relevant context from ChromaDB and calls Claude.

    Examples:
      > What DQ rules apply to the bond table?
      > change: rename column principal_amount to face_value_amount
    """
    from demo.config import load
    from demo.knowledge.retriever import Retriever
    from demo.knowledge.knowledge_agent import KnowledgeAgent, run_repl
    from demo.tools.metrics import MetricsTracker
    conf = load()
    metrics = MetricsTracker()
    retriever = Retriever(conf.chroma_path)
    agent = KnowledgeAgent(retriever, conf.repo_root, metrics)
    run_repl(agent)


def _strip_spec_columns_from_notebook(notebook_path: "Path", col_names: list, con: "Console") -> bool:
    """
    Remove SELECT expressions and COMMENT ON COLUMN statements for the given
    column names from a Gold SQL notebook. Returns True if the file was changed.
    """
    if not notebook_path.exists() or not col_names:
        return False

    original = notebook_path.read_text(encoding="utf-8")
    lines = original.split("\n")
    out: list[str] = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # ── Detect SELECT block header: "-- ── {col_name}" ──────────────────
        matched_col = next(
            (c for c in col_names if f"── {c}" in line or f"-- ── {c}" in line),
            None,
        )
        if matched_col is not None:
            # Remove the preceding blank line (if any)
            while out and not out[-1].strip():
                out.pop()
            # Skip this header line and all lines until the next section marker
            i += 1
            while i < len(lines):
                l = lines[i]
                # Stop when we hit the next "-- ──" section or FROM clause
                if ("-- ──" in l and matched_col not in l) or l.lstrip().startswith("FROM "):
                    break
                # Skip ### marker lines (e.g. ### COMMENT_EXPR)
                if l.startswith("###"):
                    i += 1
                    continue
                # Skip COMMENT ON COLUMN inside the SELECT area (misplaced by patch)
                if l.startswith("COMMENT ON COLUMN") and matched_col in l:
                    while i < len(lines) and not lines[i].rstrip().endswith(";"):
                        i += 1
                    i += 1  # skip the ";" line
                    continue
                i += 1
            continue  # re-process the line we stopped at (don't advance i)

        # ── Detect COMMENT ON COLUMN in the comment section ─────────────────
        if line.startswith("COMMENT ON COLUMN") and any(f".{c} IS" in line for c in col_names):
            # Also remove the preceding -- COMMAND ---------- + blank line
            while out and (not out[-1].strip() or "-- COMMAND ----------" in out[-1]):
                out.pop()
            # Skip until end of statement (line ending with ;)
            while i < len(lines) and not lines[i].rstrip().endswith(";"):
                i += 1
            i += 1  # skip the ";" line
            # Skip trailing blank line
            if i < len(lines) and not lines[i].strip():
                i += 1
            continue

        # ── Drop stray ### COMMENT_EXPR lines ───────────────────────────────
        if line.startswith("### COMMENT_EXPR"):
            i += 1
            continue

        out.append(line)
        i += 1

    new_content = "\n".join(out)
    if new_content != original:
        notebook_path.write_text(new_content, encoding="utf-8")
        con.print(f"  [green]✓[/] Stripped {len(col_names)} column(s) from {notebook_path.name}: {col_names}")
        return True
    con.print(f"  [dim]  {notebook_path.name}: columns not found in notebook — nothing stripped[/]")
    return False


@main.command(name="revert")
@click.option("--use-case", default="securities-master", show_default=True, help="Use-case name")
@click.option(
    "--what",
    default="all",
    type=click.Choice(["generated", "specs", "state", "all"]),
    show_default=True,
    help=(
        "generated — notebooks + tests only\n"
        "specs     — use-case YAML spec files only\n"
        "state     — demo state + gold/tables.yaml reset\n"
        "all       — everything above"
    ),
)
@click.option("--layer", default=None, type=click.Choice(["bronze", "silver", "gold"]),
              help="Limit to one layer (generated/specs only).")
@click.option("--dry-run", is_flag=True, default=False, help="Show what would be reverted without changing files.")
def revert_cmd(use_case: str, what: str, layer: str, dry_run: bool):
    """
    Revert agent-generated files to their last committed git state.

    Examples:\n
        sml revert                          Revert everything\n
        sml revert --what generated         Notebooks + tests only\n
        sml revert --what specs             Spec YAMLs only\n
        sml revert --what generated --layer gold   Only the gold notebook + gold test\n
        sml revert --what state             Reset demo state + gold/tables.yaml\n
        sml revert --dry-run                Show what would change, don't touch files\n
    """
    import subprocess
    import yaml as _yaml
    from pathlib import Path
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    con = Console()
    repo_root = Path(__file__).resolve().parents[2]

    # ── File groups ────────────────────────────────────────────────────────────
    notebooks = {
        "bronze": repo_root / "project/notebooks/03_bronze_ingest.py",
        "silver": repo_root / "project/notebooks/04_silver_conform.sql",
        "gold":   repo_root / "project/notebooks/05_gold_build.sql",
    }
    tests = {
        "bronze": repo_root / "project/tests/test_bronze.py",
        "silver": repo_root / "project/tests/test_silver.py",
        "gold":   repo_root / "project/tests/test_gold.py",
    }
    specs = {
        "bronze": [
            repo_root / f"project/use-cases/{use_case}/specs/bronze/tables.yaml",
            repo_root / f"project/use-cases/{use_case}/specs/bronze/rules.yaml",
        ],
        "silver": [
            repo_root / f"project/use-cases/{use_case}/specs/silver/tables.yaml",
            repo_root / f"project/use-cases/{use_case}/specs/silver/rules.yaml",
        ],
        "gold": [
            repo_root / f"project/use-cases/{use_case}/specs/gold/tables.yaml",
            repo_root / f"project/use-cases/{use_case}/specs/gold/rules.yaml",
        ],
    }
    demo_state_dir = repo_root / ".chroma/demo_state"
    gold_yaml_path = repo_root / f"project/use-cases/{use_case}/specs/gold/tables.yaml"
    gold_nb_path   = repo_root / "project/notebooks/05_gold_build.sql"

    layers = [layer] if layer else ["bronze", "silver", "gold"]

    to_restore: list[Path] = []
    to_clear_state: list[Path] = []

    if what in ("generated", "all"):
        for lyr in layers:
            to_restore.append(notebooks[lyr])
            to_restore.append(tests[lyr])

    if what in ("specs", "all"):
        for lyr in layers:
            to_restore.extend(specs[lyr])

    if what in ("state", "all"):
        to_clear_state.append(gold_yaml_path)
        if demo_state_dir.exists():
            to_clear_state.extend(demo_state_dir.glob("*.json"))

    # ── Read spec column names from git HEAD (not local file, which may already be cleared) ─
    # HEAD is the source of truth: it contains the columns that were committed and
    # need to be stripped from the notebook after git restore.
    spec_col_names: list[str] = []
    head_yaml_result = subprocess.run(
        ["git", "show", f"HEAD:project/use-cases/{use_case}/specs/gold/tables.yaml"],
        cwd=repo_root, capture_output=True, text=True,
    )
    if head_yaml_result.returncode == 0 and head_yaml_result.stdout.strip():
        head_data = _yaml.safe_load(head_yaml_result.stdout) or {}
        spec_col_names = [
            c["name"]
            for t in head_data.get("tables", [])
            for c in t.get("columns", [])
        ]

    # ── Dry-run: just report ────────────────────────────────────────────────
    if dry_run:
        tbl = Table(title="Files that would be reverted", show_lines=True)
        tbl.add_column("File", style="cyan")
        tbl.add_column("Action", style="yellow")
        for p in to_restore:
            rel = p.relative_to(repo_root) if p.is_relative_to(repo_root) else p
            tbl.add_row(str(rel), "git restore")
        for p in to_clear_state:
            rel = p.relative_to(repo_root) if p.is_relative_to(repo_root) else p
            if p.suffix == ".yaml":
                tbl.add_row(str(rel), "reset columns: []")
            else:
                tbl.add_row(str(rel), "delete")
        if spec_col_names and gold_nb_path.exists():
            tbl.add_row(str(gold_nb_path.relative_to(repo_root)),
                        f"strip columns: {spec_col_names}")
        con.print(tbl)
        return

    # ── Apply: git restore tracked files ───────────────────────────────────
    reverted, skipped = [], []
    for p in to_restore:
        rel = str(p.relative_to(repo_root))
        result = subprocess.run(
            ["git", "restore", rel],
            cwd=repo_root, capture_output=True, text=True,
        )
        if result.returncode == 0:
            reverted.append(rel)
        else:
            skipped.append(f"{rel} ({result.stderr.strip()})")

    # ── Strip spec columns from Gold notebook ────────────────────────────────
    if spec_col_names and ("gold" in layers) and (what in ("generated", "all", "state")):
        con.print(f"[dim]  Stripping spec columns from gold notebook: {spec_col_names}[/]")
        stripped = _strip_spec_columns_from_notebook(gold_nb_path, spec_col_names, con)
        if stripped:
            reverted.append(str(gold_nb_path.relative_to(repo_root)) + " (columns stripped)")

    # ── Apply: reset gold spec + delete demo state ──────────────────────────
    for p in to_clear_state:
        if not p.exists():
            continue
        if p.suffix == ".yaml":
            data = _yaml.safe_load(p.read_text()) or {}
            for tbl_def in data.get("tables", []):
                tbl_def["columns"] = []
            p.write_text(_yaml.dump(data, allow_unicode=True, sort_keys=False))
            reverted.append(str(p.relative_to(repo_root)))
        else:
            p.unlink()
            reverted.append(str(p.relative_to(repo_root)) + " (deleted)")

    # ── Summary: local files ───────────────────────────────────────────────
    if not reverted and not skipped:
        con.print("[dim]Nothing to revert.[/]")
        return

    if reverted:
        con.print(Panel(
            "\n".join(f"  [green]✓[/] {f}" for f in reverted),
            title="[bold green]Local Files Reverted[/]",
            border_style="green",
        ))
    if skipped:
        con.print(Panel(
            "\n".join(f"  [yellow]⚠[/] {f}" for f in skipped),
            title="[yellow]Skipped (not in git)[/]",
            border_style="yellow",
        ))

    # ── Step 2: Commit + Push to Git ──────────────────────────────────────
    con.print("\n[bold cyan][GIT][/] Staging changes...")

    # Discover ALL files that differ from HEAD (modified, deleted, or new vs HEAD)
    # using git status --porcelain. Stage only the relevant project files.
    status_result = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=repo_root, capture_output=True, text=True,
    )
    changed_files = []
    for status_line in status_result.stdout.splitlines():
        state = status_line[:2].strip()
        path  = status_line[3:].strip()
        # Only stage project/ and agentic/ files — not .chroma/ or .claude/
        if path.startswith("project/") or path.startswith("agentic/"):
            changed_files.append((state, path))
            if state in ("D", "?"):
                subprocess.run(["git", "rm", "--cached", "--ignore-unmatch", path],
                               cwd=repo_root, capture_output=True)
            else:
                subprocess.run(["git", "add", path], cwd=repo_root, capture_output=True)

    if changed_files:
        con.print("[dim]  Staged files:[/]")
        for s, p in changed_files:
            con.print(f"  [dim]{s}[/] {p}")
    else:
        con.print("[dim]  git status: (nothing changed vs HEAD)[/]")

    diff_result = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        cwd=repo_root, capture_output=True, text=True,
    )
    staged = diff_result.stdout.strip()

    if staged:
        branch_result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=repo_root, capture_output=True, text=True,
        )
        branch = branch_result.stdout.strip()

        commit_msg = (
            f"revert: reset agent-generated files ({what})\n\n"
            f"Files reverted:\n"
            + "\n".join(f"  - {l}" for l in staged.splitlines())
            + f"\n\nReverted via `sml revert --what {what}` on branch {branch}.\n"
            f"Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
        )
        commit_result = subprocess.run(
            ["git", "commit", "-m", commit_msg],
            cwd=repo_root, capture_output=True, text=True,
        )
        if commit_result.returncode == 0:
            # Print the short commit hash so user can verify in GitHub
            hash_result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=repo_root, capture_output=True, text=True,
            )
            sha = hash_result.stdout.strip()
            con.print(f"[green]  ✓ Committed revert [{sha}] on branch '{branch}'[/]")
        else:
            con.print(f"[yellow]  ⚠ Commit failed: {commit_result.stderr.strip()}[/]")

        con.print(f"[bold cyan][GIT][/] Pushing '{branch}' to origin...")
        push_result = subprocess.run(
            ["git", "push", "origin", branch],
            cwd=repo_root, capture_output=True, text=True,
        )
        if push_result.returncode == 0:
            con.print(f"[green]  ✓ Pushed → origin/{branch}[/]")
            con.print(f"[dim]  Check GitHub: branch '{branch}' should now show updated commit time[/]")
        else:
            con.print(f"[red]  ✗ Push failed: {push_result.stderr.strip()}[/]")
    else:
        con.print("[dim]  No staged changes — all files already match the reverted state in git.[/]")

    # ── Step 3: Redeploy to Databricks ────────────────────────────────────
    con.print("\n[bold cyan][DATABRICKS][/] Redeploying bundle with reverted notebooks...")
    import shutil as _shutil
    import os as _os

    bundle_root = repo_root / "project"   # databricks.yml lives in project/

    if not _shutil.which("databricks"):
        con.print("[yellow]  ⚠ 'databricks' CLI not found — skipping redeploy.[/]")
        con.print(f"  Run manually: cd {bundle_root} && databricks bundle deploy")
        return

    # Auto-detect local Terraform to avoid openpgp key expired checksum error
    if not _os.environ.get("DATABRICKS_TF_EXEC_PATH"):
        tf = _shutil.which("terraform")
        if tf:
            _os.environ["DATABRICKS_TF_EXEC_PATH"] = tf

    # Stream validate output live
    con.print("[dim]  databricks bundle validate...[/]")
    validate = subprocess.run(
        ["databricks", "bundle", "validate"],
        cwd=bundle_root,
    )
    if validate.returncode != 0:
        con.print("[red]  ✗ Bundle validate failed — skipping deploy[/]")
        return

    # Stream deploy output live
    con.print("[dim]  databricks bundle deploy...[/]")
    deploy = subprocess.run(
        ["databricks", "bundle", "deploy"],
        cwd=bundle_root,
    )
    if deploy.returncode == 0:
        con.print("[green]  ✓ Databricks bundle deployed with reverted notebooks[/]")
    else:
        con.print("[red]  ✗ Databricks deploy failed[/]")


@main.command(name="change")
@click.argument("instruction")
def change_cmd(instruction: str):
    """
    Propose a code change across the codebase via the knowledge agent.

    INSTRUCTION is a natural language description of the change, e.g.:
      sml change "rename column principal_amount to face_value_amount"

    The agent finds all affected files, generates diffs, and applies them
    after your confirmation.
    """
    from demo.config import load
    from demo.knowledge.retriever import Retriever
    from demo.knowledge.knowledge_agent import KnowledgeAgent, _handle_change
    from demo.tools.metrics import MetricsTracker
    from rich.console import Console
    conf = load()
    metrics = MetricsTracker()
    agent = KnowledgeAgent(Retriever(conf.chroma_path), conf.repo_root, metrics)
    _handle_change(agent, instruction)
