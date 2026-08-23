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
    from rich import print as rprint
    conf = load()
    total = index_run(repo_root=conf.repo_root, chroma_path=conf.chroma_path)
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
