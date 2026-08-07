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
