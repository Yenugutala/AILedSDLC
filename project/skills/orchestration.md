# Orchestration — DAB Jobs, Scheduling, Retry Logic, and Monitoring

Complete reference for Databricks Asset Bundle (DAB) job orchestration.

---

## 1. DAB Bundle Structure

```
project/
├── databricks.yml              # Bundle root — targets, variables
└── resources/
    ├── jobs/
    │   ├── bronze_ingest_job.yml
    │   ├── silver_conform_job.yml
    │   ├── gold_mart_job.yml
    │   └── orchestrate_pipeline_job.yml    ← run this one
    └── clusters/
        └── shared_cluster.yml
```

---

## 2. databricks.yml — Root Configuration

```yaml
bundle:
  name: securities-master-lakehouse

variables:
  databricks_host:
    description: Databricks workspace URL (https://...)
  catalog:
    default: statestreet
  env:
    default: dev

targets:
  dev:
    workspace:
      host: ${var.databricks_host}
    default: true
    variables:
      env: dev

  prod:
    workspace:
      host: ${var.databricks_host}
    run_as:
      service_principal_name: svc-pipeline@statestreet.com
    variables:
      env: prod

resources:
  jobs:
    # Individual jobs (included from resources/jobs/)
    bronze_ingest_job:
      include: resources/jobs/bronze_ingest_job.yml
    silver_conform_job:
      include: resources/jobs/silver_conform_job.yml
    gold_mart_job:
      include: resources/jobs/gold_mart_job.yml
    orchestrate_pipeline_job:
      include: resources/jobs/orchestrate_pipeline_job.yml
```

---

## 3. Orchestrator Job (Full Pipeline)

```yaml
# resources/jobs/orchestrate_pipeline_job.yml
resources:
  jobs:
    orchestrate_pipeline_job:
      name: "[SML] Securities Master — Full Pipeline"
      description: "Bronze → Silver → Gold full pipeline for securities master data"

      # Schedule: run daily at 6am UTC (adjust to your SLA)
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "UTC"
        pause_status: UNPAUSED

      # Email notifications
      email_notifications:
        on_failure:
          - data-engineers@statestreet.com
        on_success:
          - data-owners@statestreet.com
        on_start: []

      # Retry logic: 2 retries with 5-minute delay
      max_concurrent_runs: 1     # prevent overlapping runs

      tasks:
        # Task 1: Bronze ingestion (Python PySpark)
        - task_key: bronze_ingest
          description: "Ingest all 29 source CSVs to b_statestreet Bronze layer"
          notebook_task:
            notebook_path: /Workspace/securities-master-lakehouse/notebooks/03_bronze_ingest.py
            base_parameters:
              use_case: securities-master
              batch_id: "{{job.start_time.iso_date}}_{{job.run_id}}"
          job_cluster_key: pipeline_cluster
          timeout_seconds: 3600      # 1 hour max
          max_retries: 2
          min_retry_interval_millis: 300000   # 5 minutes between retries
          retry_on_timeout: false

        # Task 2: Silver conformance + DQ (SQL)
        - task_key: silver_conform
          description: "128 DQ rules + SCD2 conformance to s_statestreet Silver layer"
          depends_on:
            - task_key: bronze_ingest
          notebook_task:
            notebook_path: /Workspace/securities-master-lakehouse/notebooks/04_silver_conform.sql
          job_cluster_key: pipeline_cluster
          timeout_seconds: 7200      # 2 hours max
          max_retries: 1
          min_retry_interval_millis: 60000    # 1 minute

        # Task 3: Gold mart build (SQL)
        - task_key: gold_build
          description: "Build 4 dimensional marts in g_statestreet Gold layer"
          depends_on:
            - task_key: silver_conform
          notebook_task:
            notebook_path: /Workspace/securities-master-lakehouse/notebooks/05_gold_build.sql
          job_cluster_key: pipeline_cluster
          timeout_seconds: 3600
          max_retries: 2

      job_clusters:
        - job_cluster_key: pipeline_cluster
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 2
            autotermination_minutes: 30
            spark_conf:
              spark.databricks.delta.schema.autoMerge.enabled: "true"
              spark.sql.adaptive.enabled: "true"
              spark.sql.adaptive.coalescePartitions.enabled: "true"
            azure_attributes:
              availability: ON_DEMAND_AZURE
              first_on_demand: 1
              spot_bid_max_price: -1
```

---

## 4. Individual Job Definitions

### Bronze Ingest Job

```yaml
# resources/jobs/bronze_ingest_job.yml
resources:
  jobs:
    bronze_ingest_job:
      name: "[SML] Bronze Ingest — Securities Master"
      tasks:
        - task_key: bronze_ingest
          notebook_task:
            notebook_path: /Workspace/securities-master-lakehouse/notebooks/03_bronze_ingest.py
            base_parameters:
              use_case: securities-master
          job_cluster_key: pipeline_cluster
          max_retries: 2
          min_retry_interval_millis: 300000

      job_clusters:
        - job_cluster_key: pipeline_cluster
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 2
```

### Silver Conform Job (with Gate 4a approval webhook)

```yaml
# resources/jobs/silver_conform_job.yml
resources:
  jobs:
    silver_conform_job:
      name: "[SML] Silver Conform — Securities Master"
      tasks:
        - task_key: run_dq_checks
          notebook_task:
            notebook_path: /Workspace/securities-master-lakehouse/notebooks/04_silver_conform.sql
          job_cluster_key: pipeline_cluster
          max_retries: 1

        # Gate 4a: wait for data owner approval before Silver is considered "done"
        # In practice, implement via a webhook task or manual step
        - task_key: notify_data_owner
          depends_on:
            - task_key: run_dq_checks
          notebook_task:
            notebook_path: /Workspace/securities-master-lakehouse/notebooks/99_notify_gate4a.py
          job_cluster_key: pipeline_cluster
```

---

## 5. DAB CLI Commands

```bash
# Validate bundle YAML before deploying
databricks bundle validate

# Deploy to default target (dev)
databricks bundle deploy

# Deploy to specific target
databricks bundle deploy --target prod

# Run a specific job
databricks bundle run orchestrate_pipeline_job

# Run with parameters
databricks bundle run bronze_ingest_job \
  --python-named-params '{"use_case": "securities-master", "batch_id": "manual_20240115"}'

# Check run status
databricks bundle run --refresh orchestrate_pipeline_job

# Destroy (careful — removes all jobs from workspace)
databricks bundle destroy --target dev
```

---

## 6. Retry Strategy

| Job Type | Max Retries | Retry Interval | Retry On Timeout |
|----------|-------------|----------------|-----------------|
| Bronze ingest | 2 | 5 min | No |
| Silver conform | 1 | 1 min | No |
| Gold mart | 2 | 5 min | No |
| Schema drift detect | 0 | N/A | No (fail fast) |
| Maintenance (OPTIMIZE) | 1 | 10 min | Yes |

**Key principle:** Never retry a broken-schema batch — it will fail again. Fix the schema first.

---

## 7. Job Monitoring and Alerting

### Databricks Job Notifications

```yaml
# In job YAML
email_notifications:
  on_failure:
    - data-engineers@statestreet.com
    - oncall-data@statestreet.com
  on_success:
    - data-owners@statestreet.com    # notify owners when Gold is ready
  no_alert_for_skipped_runs: true
```

### Webhook Notifications (PagerDuty / Slack)

```yaml
webhook_notifications:
  on_failure:
    - id: "pagerduty-integration-id"    # configure in Databricks workspace settings
  on_success:
    - id: "slack-channel-id"
```

### SLA Monitoring (Custom)

Add a check notebook that runs after Gold build to verify SLA:

```python
# notebooks/99_sla_check.py
from datetime import datetime, timezone, timedelta

# Check: Bronze must complete within 2 hours of scheduled start
bronze_ts = spark.sql("""
    SELECT MAX(completed_at) FROM statestreet.g_statestreet.audit_log
    WHERE stage = 'bronze' AND status = 'SUCCESS'
    AND started_at > current_timestamp() - INTERVAL 1 DAY
""").first()[0]

if bronze_ts < datetime.now(timezone.utc) - timedelta(hours=2):
    raise RuntimeError(f"SLA BREACH: Bronze completed at {bronze_ts} — more than 2h late")
```

---

## 8. Maintenance Job

```yaml
# resources/jobs/maintenance_job.yml
resources:
  jobs:
    maintenance_job:
      name: "[SML] Weekly Table Maintenance"
      schedule:
        quartz_cron_expression: "0 0 2 ? * SUN"   # Sunday 2am UTC
        timezone_id: "UTC"
      tasks:
        - task_key: optimize_bronze
          notebook_task:
            notebook_path: /Workspace/securities-master-lakehouse/notebooks/99_maintenance.sql
          job_cluster_key: maintenance_cluster

      job_clusters:
        - job_cluster_key: maintenance_cluster
          new_cluster:
            spark_version: 15.4.x-scala2.12
            node_type_id: Standard_DS3_v2
            num_workers: 1
            autotermination_minutes: 60
```

```sql
-- notebooks/99_maintenance.sql
-- Run OPTIMIZE + VACUUM on all tables weekly

-- Bronze layer
OPTIMIZE statestreet.b_statestreet.product;
OPTIMIZE statestreet.b_statestreet.bond;
-- ... (all 29 tables)

-- Silver layer
OPTIMIZE statestreet.s_statestreet.product ZORDER BY (status, issuer_legal_entity_id);
OPTIMIZE statestreet.s_statestreet.product_rating ZORDER BY (product_id, rating_date);

-- Gold layer
OPTIMIZE statestreet.g_statestreet.dim_product ZORDER BY (status);
OPTIMIZE statestreet.g_statestreet.fact_coupon_schedule ZORDER BY (product_id, payment_date);

-- VACUUM (7-day retention)
VACUUM statestreet.b_statestreet.product RETAIN 168 HOURS;
VACUUM statestreet.s_statestreet.product RETAIN 168 HOURS;
VACUUM statestreet.g_statestreet.dim_product RETAIN 168 HOURS;
```

---

## 9. Multi-Environment Strategy

| Environment | Catalog | Purpose | Deploy Trigger |
|-------------|---------|---------|----------------|
| Dev | `statestreet_dev` | Daily development + testing | PR merge to `develop` |
| Staging | `statestreet_staging` | Pre-prod validation | PR merge to `staging` |
| Production | `statestreet` | Live pipeline | PR merge to `main` |

```yaml
# Multiple targets in databricks.yml
targets:
  dev:
    workspace:
      host: ${var.dev_workspace_host}
    variables:
      catalog: statestreet_dev
    default: true

  staging:
    workspace:
      host: ${var.staging_workspace_host}
    variables:
      catalog: statestreet_staging

  prod:
    workspace:
      host: ${var.prod_workspace_host}
    run_as:
      service_principal_name: svc-pipeline@statestreet.com
    variables:
      catalog: statestreet
```
