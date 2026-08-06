"""
catalog_utils.py
Unity Catalog helper functions for generated notebooks.
Provides table existence checks, schema creation, and TBLPROPERTIES helpers.
"""

from pyspark.sql import SparkSession


def ensure_schema_exists(spark: SparkSession, catalog: str, schema: str):
    """Create schema if it doesn't exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")


def table_exists(spark: SparkSession, full_table_name: str) -> bool:
    """Check if a Delta table exists in Unity Catalog."""
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return True
    except Exception:
        return False


def enable_iceberg_uniform(spark: SparkSession, full_table_name: str):
    """Enable Delta UniForm Iceberg compatibility on an existing table."""
    spark.sql(f"""
        ALTER TABLE {full_table_name}
        SET TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')
    """)


def get_table_row_count(spark: SparkSession, full_table_name: str) -> int:
    """Return row count for a table."""
    return spark.table(full_table_name).count()


def add_table_comment(spark: SparkSession, full_table_name: str, comment: str):
    """Set a table-level comment for Unity Catalog / Genie."""
    # Escape single quotes in comment
    safe_comment = comment.replace("'", "\\'")
    spark.sql(f"COMMENT ON TABLE {full_table_name} IS '{safe_comment}'")


def add_column_comment(spark: SparkSession, full_table_name: str, column: str, comment: str):
    """Set a column-level comment for Unity Catalog / Genie."""
    safe_comment = comment.replace("'", "\\'")
    spark.sql(f"COMMENT ON COLUMN {full_table_name}.{column} IS '{safe_comment}'")


def get_dq_rule_version(catalog: str, schema: str, table: str, spark: SparkSession) -> str:
    """
    Get the most common _dq_rule_version in a Silver table.
    Used to detect if a rescan is needed.
    """
    try:
        result = spark.sql(f"""
            SELECT _dq_rule_version, COUNT(*) AS cnt
            FROM {catalog}.{schema}.{table}
            GROUP BY _dq_rule_version
            ORDER BY cnt DESC
            LIMIT 1
        """).collect()
        return result[0]["_dq_rule_version"] if result else None
    except Exception:
        return None
