from __future__ import annotations
"""
source_reader.py
Source-agnostic reader — routes to the correct reader based on request.yaml source.type.

Supported source types (defined in request.yaml → source.type):
  volume   — CSV/Parquet/JSON/Delta from Databricks Volume (default for file-based)
  jdbc     — Relational database (SQL Server, Oracle, PostgreSQL, MySQL via JDBC)
  api      — REST API (JSON response → DataFrame)
  kafka    — Kafka topic (streaming → batch microbatch read)
  delta    — Another Delta table in Unity Catalog (bronze-to-bronze or cross-domain)
  s3       — AWS S3 / Azure ADLS Gen2 / GCS path (external storage)

Usage in generated Bronze notebooks:
    from src.ingestion.source_reader import read_source
    spec = load_spec("securities-master", "bronze")
    table_spec = spec.get_table("product")
    df = read_source(spark, table_spec, request_config)
"""

from pyspark.sql import SparkSession, DataFrame
from typing import Any


def read_source(spark: SparkSession, table_spec: dict, source_config: dict) -> DataFrame:
    """
    Read a DataFrame from any source based on source_config.type.

    Args:
        spark:         Active SparkSession
        table_spec:    Bronze table spec (from bronze/tables.yaml) for this table
        source_config: The 'source:' block from request.yaml

    Returns:
        Raw DataFrame (no metadata columns yet — those are added by bronze_loader.py)
    """
    source_type = source_config.get("type", "volume").lower()

    readers = {
        "volume": _read_volume,
        "jdbc":   _read_jdbc,
        "api":    _read_api,
        "kafka":  _read_kafka,
        "delta":  _read_delta,
        "s3":     _read_cloud_storage,
        "adls":   _read_cloud_storage,
        "gcs":    _read_cloud_storage,
    }

    reader_fn = readers.get(source_type)
    if reader_fn is None:
        raise ValueError(
            f"Unsupported source type: '{source_type}'. "
            f"Supported: {list(readers.keys())}"
        )

    return reader_fn(spark, table_spec, source_config)


# ─────────────────────────────────────────────────────────────
# READER IMPLEMENTATIONS
# ─────────────────────────────────────────────────────────────

def _read_volume(spark: SparkSession, table_spec: dict, source_config: dict) -> DataFrame:
    """
    Read from Databricks Volume (CSV, Parquet, JSON, Avro, ORC).
    source.type: volume
    source.path: /Volumes/<catalog>/<schema>/<volume>/
    source.format: csv | parquet | json | avro | orc
    source.delimiter: , (for CSV)
    source.header: true (for CSV)
    """
    table_name  = table_spec["name"]
    base_path   = source_config.get("path", "").rstrip("/")
    fmt         = source_config.get("format", "csv").lower()
    source_file = table_spec.get("source_file", f"{table_name}.{fmt}")
    full_path   = f"{base_path}/{source_file}"

    reader = spark.read

    if fmt == "csv":
        reader = (reader
                  .option("header",    str(source_config.get("header", True)).lower())
                  .option("delimiter", source_config.get("delimiter", ","))
                  .option("inferSchema", "true")
                  .option("nullValue", ""))
    elif fmt == "parquet":
        pass  # no special options needed
    elif fmt == "json":
        reader = reader.option("multiLine", "true")
    elif fmt in ("avro", "orc"):
        pass

    return reader.format(fmt).load(full_path)


def _read_jdbc(spark: SparkSession, table_spec: dict, source_config: dict) -> DataFrame:
    """
    Read from relational database via JDBC.
    source.type: jdbc
    source.jdbc.url: jdbc:sqlserver://...  | jdbc:oracle:thin:@... | jdbc:postgresql://...
    source.jdbc.driver: com.microsoft.sqlserver.jdbc.SQLServerDriver
    source.jdbc.user: (from secret scope)
    source.jdbc.password_secret: scope/key  (Databricks secret scope reference)
    source.jdbc.dbtable: schema.table_name  OR
    source.jdbc.query: SELECT * FROM ...
    source.jdbc.fetch_size: 10000
    source.jdbc.partition_column: id_column  (for parallelism)
    source.jdbc.num_partitions: 8
    source.jdbc.lower_bound: 1
    source.jdbc.upper_bound: 1000000

    Best practice: use secret scope for credentials, never hardcode passwords.
    """
    jdbc_config = source_config.get("jdbc", {})
    table_name  = table_spec["name"]

    url      = jdbc_config["url"]
    driver   = jdbc_config.get("driver", "")
    user     = jdbc_config.get("user", "")
    fetch_sz = str(jdbc_config.get("fetch_size", 10000))

    # Resolve password from Databricks secret scope
    password_secret = jdbc_config.get("password_secret", "")
    if password_secret and "/" in password_secret:
        scope, key = password_secret.split("/", 1)
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            password = dbutils.secrets.get(scope=scope, key=key)
        except Exception:
            raise RuntimeError(
                f"Cannot read Databricks secret '{password_secret}'. "
                "Ensure the secret scope and key exist."
            )
    else:
        password = jdbc_config.get("password", "")

    # Build JDBC options
    options = {
        "url":          url,
        "fetchsize":    fetch_sz,
    }
    if driver:
        options["driver"] = driver
    if user:
        options["user"] = user
    if password:
        options["password"] = password

    # Prefer query over dbtable (more flexible)
    if "query" in jdbc_config:
        options["query"] = jdbc_config["query"]
    else:
        options["dbtable"] = jdbc_config.get("dbtable", table_name)

    # Parallel read partitioning (optional, for large tables)
    if "partition_column" in jdbc_config:
        options.update({
            "partitionColumn": jdbc_config["partition_column"],
            "numPartitions":   str(jdbc_config.get("num_partitions", 8)),
            "lowerBound":      str(jdbc_config.get("lower_bound", 0)),
            "upperBound":      str(jdbc_config.get("upper_bound", 1000000)),
        })

    return spark.read.format("jdbc").options(**options).load()


def _read_api(spark: SparkSession, table_spec: dict, source_config: dict) -> DataFrame:
    """
    Read from a REST API endpoint.
    source.type: api
    source.api.url: https://api.example.com/securities
    source.api.method: GET | POST
    source.api.headers: {Authorization: Bearer <token>}
    source.api.auth_secret: scope/key  (Databricks secret for bearer token)
    source.api.params: {limit: 1000, page: 1}
    source.api.body: {...}  (for POST)
    source.api.pagination.type: page | offset | cursor | link_header
    source.api.pagination.page_param: page
    source.api.pagination.page_size_param: limit
    source.api.pagination.max_pages: 100
    source.api.response_path: data.items  (JSONPath to the array within response)

    Best practice: handle pagination, rate limiting, and error retries.
    """
    import requests
    import json
    from pyspark.sql import Row

    api_config   = source_config.get("api", {})
    url          = api_config["url"]
    method       = api_config.get("method", "GET").upper()
    headers      = dict(api_config.get("headers", {}))
    params       = dict(api_config.get("params", {}))
    response_path = api_config.get("response_path", "")

    # Resolve auth token from Databricks secret scope
    auth_secret = api_config.get("auth_secret", "")
    if auth_secret and "/" in auth_secret:
        scope, key = auth_secret.split("/", 1)
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            token = dbutils.secrets.get(scope=scope, key=key)
            headers["Authorization"] = f"Bearer {token}"
        except Exception as e:
            raise RuntimeError(f"Cannot read API auth secret '{auth_secret}': {e}")

    # Pagination support
    pagination  = api_config.get("pagination", {})
    page_type   = pagination.get("type", "none")
    max_pages   = pagination.get("max_pages", 100)

    all_records = []
    page = 1

    while page <= max_pages:
        if page_type == "page":
            params[pagination.get("page_param", "page")] = page
            params[pagination.get("page_size_param", "limit")] = pagination.get("page_size", 100)
        elif page_type == "offset":
            params[pagination.get("offset_param", "offset")] = (page - 1) * pagination.get("page_size", 100)

        if method == "GET":
            resp = requests.get(url, headers=headers, params=params, timeout=30)
        else:
            resp = requests.post(url, headers=headers, json=api_config.get("body", {}), timeout=30)

        resp.raise_for_status()
        data = resp.json()

        # Navigate to the records array via response_path (e.g. "data.items")
        records = data
        for part in response_path.split(".") if response_path else []:
            records = records.get(part, [])

        if not isinstance(records, list):
            records = [records]

        all_records.extend(records)

        # Stop conditions
        if not records or page_type == "none":
            break
        if page_type == "link_header":
            next_link = resp.links.get("next", {}).get("url")
            if not next_link:
                break
            url = next_link
        page += 1

    if not all_records:
        return spark.createDataFrame([], schema=None)

    # Flatten to Spark DataFrame via JSON round-trip (handles nested objects)
    rdd = spark.sparkContext.parallelize([json.dumps(r) for r in all_records])
    return spark.read.json(rdd)


def _read_kafka(spark: SparkSession, table_spec: dict, source_config: dict) -> DataFrame:
    """
    Read from Kafka topic as a batch (readStream → micro-batch or static batch).
    source.type: kafka
    source.kafka.bootstrap_servers: broker1:9092,broker2:9092
    source.kafka.topic: securities.master.product
    source.kafka.group_id: sml-bronze-reader
    source.kafka.starting_offsets: latest | earliest | {"topic":{"0":123}}
    source.kafka.auth_secret: scope/key  (for SASL credentials)
    source.kafka.value_format: json | avro | string
    source.kafka.schema_registry_url: https://schema-registry:8081  (for Avro)

    Returns a static batch DataFrame (for Bronze landing).
    For continuous streaming, use Delta Live Tables (see skills/streaming_cdc.md).
    """
    kafka_config = source_config.get("kafka", {})

    bootstrap  = kafka_config["bootstrap_servers"]
    topic      = kafka_config.get("topic", table_spec["name"])
    start_off  = kafka_config.get("starting_offsets", "latest")
    value_fmt  = kafka_config.get("value_format", "json")

    options = {
        "kafka.bootstrap.servers": bootstrap,
        "subscribe":               topic,
        "startingOffsets":         start_off,
        "maxOffsetsPerTrigger":    str(kafka_config.get("max_offsets_per_trigger", 100000)),
    }

    # SASL authentication (e.g. Confluent Cloud)
    auth_secret = kafka_config.get("auth_secret", "")
    if auth_secret and "/" in auth_secret:
        scope, key = auth_secret.split("/", 1)
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
            sasl_creds = dbutils.secrets.get(scope=scope, key=key)
            options.update({
                "kafka.sasl.mechanism":              "PLAIN",
                "kafka.security.protocol":           "SASL_SSL",
                "kafka.sasl.jaas.config":            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_config.get("sasl_username", "")}" password="{sasl_creds}";',
            })
        except Exception as e:
            raise RuntimeError(f"Cannot read Kafka auth secret '{auth_secret}': {e}")

    # Read as batch (not streaming)
    raw_df = spark.read.format("kafka").options(**options).load()

    # Deserialize value column
    from pyspark.sql import functions as F
    if value_fmt == "json":
        return raw_df.select(
            F.col("key").cast("string").alias("_kafka_key"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
            F.col("topic").alias("_kafka_topic"),
            F.from_json(F.col("value").cast("string"), _infer_kafka_schema(spark, options, topic)).alias("value"),
        ).select("_kafka_key", "_kafka_offset", "_kafka_timestamp", "_kafka_topic", "value.*")
    elif value_fmt == "string":
        return raw_df.select(
            F.col("value").cast("string").alias("value"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
        )
    else:
        # Avro — requires schema registry; return raw bytes for now
        return raw_df


def _read_delta(spark: SparkSession, table_spec: dict, source_config: dict) -> DataFrame:
    """
    Read from an existing Delta table in Unity Catalog.
    source.type: delta
    source.delta.full_table_name: catalog.schema.table
    source.delta.filter: "status = 'ACTIVE'"  (optional WHERE clause)
    source.delta.as_of_version: 42  (optional time travel)
    source.delta.as_of_timestamp: "2024-01-01 00:00:00"  (optional)

    Use case: cross-domain Bronze reads, or reading a Silver table from another catalog.
    """
    delta_config = source_config.get("delta", {})
    full_table   = delta_config.get("full_table_name", "")

    if not full_table:
        raise ValueError("source.delta.full_table_name is required for delta source type")

    df = spark.table(full_table)

    # Optional time travel
    if "as_of_version" in delta_config:
        df = spark.read.format("delta").option("versionAsOf", delta_config["as_of_version"]).table(full_table)
    elif "as_of_timestamp" in delta_config:
        df = spark.read.format("delta").option("timestampAsOf", delta_config["as_of_timestamp"]).table(full_table)

    # Optional filter pushdown
    if "filter" in delta_config:
        df = df.filter(delta_config["filter"])

    return df


def _read_cloud_storage(spark: SparkSession, table_spec: dict, source_config: dict) -> DataFrame:
    """
    Read from cloud object storage (S3, ADLS Gen2, GCS).
    source.type: s3 | adls | gcs
    source.path: s3://bucket/prefix/ | abfss://container@account.dfs.core.windows.net/path/
    source.format: parquet | delta | csv | json | avro
    source.options: {key: value}  (passthrough Spark reader options)

    Best practice: configure access via Unity Catalog External Location,
    not by hardcoding storage credentials in code.
    """
    fmt          = source_config.get("format", "parquet").lower()
    path         = source_config.get("path", "")
    extra_opts   = source_config.get("options", {})
    table_name   = table_spec["name"]
    source_file  = table_spec.get("source_file", "")

    full_path = f"{path.rstrip('/')}/{source_file}" if source_file else path

    reader = spark.read.format(fmt).options(**extra_opts)

    if fmt == "csv":
        reader = (reader
                  .option("header", "true")
                  .option("inferSchema", "true"))

    return reader.load(full_path)


def _infer_kafka_schema(spark: SparkSession, options: dict, topic: str):
    """Sample a few Kafka messages to infer the JSON schema."""
    from pyspark.sql import functions as F
    sample = (spark.read.format("kafka")
              .options(**options)
              .option("endingOffsets", "latest")
              .option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", "100")
              .load()
              .select(F.col("value").cast("string"))
              .limit(10))

    if sample.count() == 0:
        from pyspark.sql.types import StringType
        return StringType()

    return spark.read.json(sample.rdd.map(lambda r: r[0])).schema
