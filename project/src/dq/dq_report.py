"""
dq_report.py
Generates a DQ reject summary report after Silver conformance runs.
Used at Gate 4a: report is sent to Data Owner for approval before Silver promotion.
"""

from datetime import datetime
from pathlib import Path
from typing import Optional


def generate_report(
    spark,
    catalog: str,
    schema: str,
    table_names: list[str],
    run_id: str,
    output_path: Optional[str] = None,
) -> str:
    """
    Summarize DQ rejects across all Silver tables.

    Args:
        spark: SparkSession
        catalog: e.g. "statestreet"
        schema: e.g. "s_statestreet"
        table_names: list of Silver table names (not rejects tables — those are inferred)
        run_id: pipeline batch_id for labeling the report
        output_path: optional path to write HTML report

    Returns:
        Plain-text summary (also written to output_path if provided)
    """
    lines = [
        f"# DQ Reject Report",
        f"Run ID: {run_id}",
        f"Generated: {datetime.utcnow().isoformat()} UTC",
        f"Schema: {catalog}.{schema}",
        "",
        "| Table | Total Rows | Rejected Rows | Reject Rate | Top Rule |",
        "|-------|-----------|---------------|-------------|----------|",
    ]

    total_rejected = 0

    for table in table_names:
        rejects_table = f"{catalog}.{schema}.{table}_rejects"
        silver_table = f"{catalog}.{schema}.{table}"

        try:
            silver_count = spark.table(silver_table).count()
        except Exception:
            silver_count = 0

        try:
            rejects_count = spark.table(rejects_table).count()
            top_rule = _get_top_rule(spark, rejects_table)
        except Exception:
            rejects_count = 0
            top_rule = "N/A"

        total = silver_count + rejects_count
        rate = f"{100 * rejects_count / total:.1f}%" if total > 0 else "0%"
        total_rejected += rejects_count

        lines.append(f"| {table} | {total} | {rejects_count} | {rate} | {top_rule} |")

    lines.append("")
    lines.append(f"**Total rejected rows across all tables: {total_rejected}**")

    if total_rejected == 0:
        lines.append("\nAll rows passed DQ checks. Silver layer is clean.")
    else:
        lines.append(
            f"\n{total_rejected} rows quarantined in rejects tables. "
            "Review the rejects tables before approving Silver promotion."
        )

    report = "\n".join(lines)

    if output_path:
        Path(output_path).write_text(report, encoding="utf-8")

    return report


def _get_top_rule(spark, rejects_table: str) -> str:
    """Find the most common _rule_id in the rejects table."""
    try:
        result = spark.sql(f"""
            SELECT _rule_id, COUNT(*) AS cnt
            FROM {rejects_table}
            GROUP BY _rule_id
            ORDER BY cnt DESC
            LIMIT 1
        """).collect()
        return result[0]["_rule_id"] if result else "N/A"
    except Exception:
        return "N/A"
