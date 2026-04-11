# Databricks Platform -- A Complete Deep Dive

**From Architecture to Production Workflows**

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Cluster Types and Configuration](#2-cluster-types-and-configuration)
3. [Delta Lake Deep Dive](#3-delta-lake-deep-dive)
4. [Auto Loader](#4-auto-loader)
5. [Unity Catalog](#5-unity-catalog)
6. [Photon Engine](#6-photon-engine)
7. [Workflows and Jobs](#7-workflows-and-jobs)
8. [Delta Live Tables (DLT)](#8-delta-live-tables-dlt)
9. [Secrets Management](#9-secrets-management)
10. [Performance Optimization](#10-performance-optimization)
11. [MLflow in Databricks](#11-mlflow-in-databricks)
12. [SQL Warehouses and Analytics](#12-sql-warehouses-and-analytics)
13. [Databricks vs On-Prem Spark](#13-databricks-vs-on-prem-spark)
14. [Quick-Reference Cheat Sheet](#14-quick-reference-cheat-sheet)

---

## 1. Architecture Overview

### 1.1 Control Plane vs Data Plane

```
+---------------------------+          +---------------------------+
|      Control Plane         |          |       Data Plane          |
|   (Databricks-managed)     |          |   (Customer's cloud)      |
|                           |          |                           |
| - Web UI / Notebooks      |          | - Compute (VMs/clusters)  |
| - Job scheduler            | <------> | - Storage (S3/ADLS/GCS)   |
| - Cluster manager          |          | - Data never leaves your  |
| - Access control           |          |   cloud account           |
| - REST API                 |          |                           |
+---------------------------+          +---------------------------+
```

**Control Plane**: Managed by Databricks. Hosts the web application, notebook server, job scheduler, and REST API. No customer data is stored here (only metadata and notebooks).

**Data Plane**: Runs in the customer's cloud account (AWS, Azure, GCP). All compute (clusters) and storage (data files) remain in the customer's infrastructure. Data never leaves the customer's cloud.

### 1.2 Workspace

A workspace is the organizational unit in Databricks:
- **Notebooks**: Interactive development environment (Python, SQL, Scala, R)
- **Repos**: Git integration for version-controlled code
- **Clusters**: Compute resources
- **Jobs**: Scheduled workflows
- **DBFS**: Databricks File System (abstraction over cloud storage)

### 1.3 DBFS (Databricks File System)

DBFS provides a filesystem abstraction over cloud object storage:

```python
# These are equivalent:
dbutils.fs.ls("dbfs:/data/")
spark.read.parquet("dbfs:/data/")
spark.read.parquet("/data/")  # dbfs:/ is the default root

# Direct cloud storage access:
spark.read.parquet("s3://my-bucket/data/")
spark.read.parquet("abfss://container@account.dfs.core.windows.net/data/")
```

---

## 2. Cluster Types and Configuration

### 2.1 Cluster Types

| Type | Purpose | Scaling | Cost |
|------|---------|---------|------|
| **All-Purpose** | Interactive notebooks, exploration | Manual or auto-scale | Higher (always on) |
| **Job Cluster** | Scheduled jobs (created, runs, terminates) | Fixed or auto-scale | Lower (ephemeral) |
| **SQL Warehouse** | SQL queries, BI tools | Serverless or classic | Pay-per-query |

### 2.2 All-Purpose Clusters

```
Use for: Interactive development, ad-hoc analysis, notebook exploration
Configuration:
- Worker type: choose based on workload (memory-optimized, compute-optimized)
- Min/max workers: set auto-scaling range
- Auto-termination: set idle timeout (e.g., 120 minutes)
- Spark version: Databricks Runtime (includes Delta Lake, MLflow, etc.)
```

### 2.3 Job Clusters

```
Use for: Production pipelines, scheduled ETL, CI/CD
Configuration:
- Created automatically when a job starts
- Terminated automatically when the job completes
- No idle cost
- Same configuration options as all-purpose

Best practice: Always use job clusters for production workloads (cheaper, isolated)
```

### 2.4 Databricks Runtime Versions

| Runtime | Includes |
|---------|---------|
| Standard | Apache Spark + Delta Lake + libraries |
| ML Runtime | Standard + ML libraries (PyTorch, TensorFlow, scikit-learn, MLflow) |
| Photon Runtime | Standard + Photon engine (C++ vectorized execution) |
| GPU Runtime | ML Runtime + GPU support (CUDA, cuDNN) |

### 2.5 Cluster Sizing Guidelines

```
Light workload (< 10 GB data):    1-2 workers, Standard_DS3_v2 (4 cores, 14 GB)
Medium workload (10-100 GB):      4-8 workers, Standard_DS4_v2 (8 cores, 28 GB)
Heavy workload (100 GB - 1 TB):   8-20 workers, Standard_DS5_v2 (16 cores, 56 GB)
Very heavy (> 1 TB):              20+ workers, memory-optimized instances

For ML training: use GPU instances (Standard_NC6s_v3 or similar)
```

---

## 3. Delta Lake Deep Dive

### 3.1 What Is Delta Lake

Delta Lake is an open-source storage layer that provides:
- **ACID transactions** on data lakes
- **Schema enforcement** on write
- **Time travel** (query historical versions)
- **Efficient upserts** (MERGE)
- **Small file compaction** (OPTIMIZE)
- **Data skipping** (Z-ORDER)

### 3.2 Transaction Log (_delta_log)

The transaction log is the heart of Delta Lake:

```
/data/my_table/
├── _delta_log/
│   ├── 00000000000000000000.json    ← Version 0: CREATE TABLE + initial INSERT
│   ├── 00000000000000000001.json    ← Version 1: INSERT
│   ├── 00000000000000000002.json    ← Version 2: UPDATE (remove old files, add new)
│   ├── 00000000000000000003.json    ← Version 3: DELETE
│   └── 00000000000000000010.checkpoint.parquet  ← Checkpoint (snapshot of state)
├── part-00000-xxx.parquet
├── part-00001-xxx.parquet
└── ...
```

Each JSON log entry contains:
```json
{
    "add": {"path": "part-00005-xxx.parquet", "size": 104857600, "stats": {...}},
    "remove": {"path": "part-00002-xxx.parquet"},
    "commitInfo": {"timestamp": 1705312800, "operation": "UPDATE", "operationMetrics": {...}}
}
```

### 3.3 How Reads Work

```
1. Find the latest checkpoint (if exists) in _delta_log/
2. Read all JSON log entries after the checkpoint
3. Replay: build the current list of "active" files (add - remove)
4. Read only those Parquet files
```

### 3.4 How Writes Work (Optimistic Concurrency)

```
Writer 1:                          Writer 2:
1. Read current version (v5)       1. Read current version (v5)
2. Write new Parquet files         2. Write new Parquet files
3. Attempt to commit v6            3. Attempt to commit v6
4. SUCCESS (v6 committed)          4. CONFLICT (v6 already exists)
                                   5. Re-read v6, check for conflicts
                                   6. If no data conflict, retry as v7
                                   7. If data conflict, throw exception
```

### 3.5 Time Travel

```sql
-- Query a specific version
SELECT * FROM my_table VERSION AS OF 5;
SELECT * FROM delta.`/data/table/` VERSION AS OF 5;

-- Query at a specific timestamp
SELECT * FROM my_table TIMESTAMP AS OF '2024-01-15 10:00:00';

-- Show version history
DESCRIBE HISTORY my_table;
```

```python
df = spark.read.format("delta").option("versionAsOf", 5).load("/data/table/")
df = spark.read.format("delta").option("timestampAsOf", "2024-01-15").load("/data/table/")
```

### 3.6 OPTIMIZE and VACUUM

**OPTIMIZE**: Compacts small files into larger ones (target ~1 GB each):
```sql
OPTIMIZE my_table;
OPTIMIZE my_table WHERE date = '2024-01-15';  -- only specific partitions
```

**Z-ORDER**: Co-locates related data in the same files for faster lookups:
```sql
OPTIMIZE my_table ZORDER BY (city, date);
-- Records with the same city and date are stored in the same files
-- Queries filtering on city and/or date skip most files
```

**VACUUM**: Deletes old files no longer referenced by the transaction log:
```sql
VACUUM my_table RETAIN 168 HOURS;  -- keep files for 7 days (for time travel)
VACUUM my_table RETAIN 0 HOURS;    -- delete immediately (disables time travel!)
```

### 3.7 Delta Lake MERGE (Upsert)

```sql
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED AND s.operation = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET t.name = s.name, t.city = s.city
WHEN NOT MATCHED THEN INSERT (id, name, city) VALUES (s.id, s.name, s.city);
```

```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "/data/table/")
target.alias("t").merge(
    source.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

---

## 4. Auto Loader

### 4.1 What Is Auto Loader

Auto Loader incrementally processes new files as they arrive in cloud storage. It tracks which files have been processed using a checkpoint, so it never re-processes old files.

### 4.2 File Discovery Modes

**Directory listing** (default): Lists the directory to find new files.
```python
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.useIncrementalListing", "auto") \
    .load("/data/landing/")
```

**File notification** (recommended for large directories): Uses cloud events (S3 SQS, ADLS Event Grid) to discover new files without listing.
```python
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.useNotifications", True) \
    .load("/data/landing/")
```

### 4.3 Schema Evolution

```python
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("cloudFiles.inferColumnTypes", True) \
    .load("/data/landing/")
```

**Rescue data column**: Columns that don't match the schema go to `_rescued_data`:
```python
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .load("/data/landing/")
# Unknown fields stored as JSON in _rescued_data column
```

### 4.4 Trigger Modes

```python
# Process all available files, then stop (for scheduled runs)
query = df.writeStream \
    .trigger(availableNow=True) \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/bronze/") \
    .start("/data/bronze/")

# Continuous processing (every 1 minute)
query = df.writeStream \
    .trigger(processingTime="1 minute") \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/bronze/") \
    .start("/data/bronze/")
```

---

## 5. Unity Catalog

### 5.1 Three-Level Namespace

```
catalog.schema.table

examples:
production.sales.fact_orders
development.analytics.dim_customers
sandbox.team_a.experiments
```

**Catalog**: Top-level container (like a database server). Separate catalogs for production, development, sandbox.
**Schema** (database): Groups related tables. Equivalent to a traditional database.
**Table**: The actual data.

### 5.2 Key Features

**Centralized governance**: One place to manage access control for all data assets (tables, views, functions, models, files).

**Fine-grained access control**:
```sql
-- Grant table-level access
GRANT SELECT ON TABLE production.sales.fact_orders TO `data-analysts`;

-- Grant schema-level access
GRANT USAGE ON SCHEMA production.sales TO `data-analysts`;

-- Column-level masking
ALTER TABLE production.sales.customers ALTER COLUMN ssn SET MASK mask_ssn;
```

**Data lineage**: Automatically tracks which tables feed into which tables:
```
bronze.events → silver.events → gold.daily_stats → dashboard
```

**Data sharing**: Share data across Databricks workspaces or with external consumers using Delta Sharing (open protocol).

### 5.3 External Locations

Map Unity Catalog to cloud storage:
```sql
CREATE EXTERNAL LOCATION my_data
    URL 'abfss://container@account.dfs.core.windows.net/data/'
    WITH (STORAGE CREDENTIAL my_credential);

CREATE TABLE production.iot.readings
    LOCATION 'abfss://container@account.dfs.core.windows.net/data/readings/';
```

---

## 6. Photon Engine

### 6.1 What Is Photon

Photon is a **C++ vectorized query engine** that replaces Spark's JVM-based execution for compatible operations. It is Databricks' proprietary acceleration layer.

### 6.2 How It Works

```
Standard Spark:
SQL → Catalyst Optimizer → Physical Plan → JVM Bytecode (Tungsten WholeStageCodegen)

With Photon:
SQL → Catalyst Optimizer → Physical Plan → C++ Native Code (Photon)
```

Photon is faster because:
- **Native C++ execution**: No JVM overhead (no GC, no JIT warmup)
- **Vectorized processing**: Processes columns of data in CPU-register-sized batches
- **SIMD instructions**: Uses CPU vector instructions to process multiple values per cycle
- **Memory-efficient**: Columnar memory layout optimized for CPU cache

### 6.3 When Photon Helps

- **Scan-heavy queries**: Reading and filtering large Parquet/Delta tables
- **Aggregations**: GROUP BY, SUM, AVG, COUNT
- **Joins**: Hash joins, sort-merge joins
- **String operations**: LIKE, regex, string manipulation

### 6.4 When Photon Does NOT Help

- **Python UDFs**: Still run in Python workers
- **Complex ML workloads**: ML training is not Photon-accelerated
- **Unsupported operations**: Some operations fall back to JVM Spark

### 6.5 Enabling Photon

Select "Photon Runtime" when creating a cluster. No code changes needed.

---

## 7. Workflows and Jobs

### 7.1 Job Configuration

```python
# Databricks job definition (JSON)
{
    "name": "daily_etl_pipeline",
    "tasks": [
        {
            "task_key": "bronze_ingest",
            "notebook_task": {
                "notebook_path": "/Production/bronze_ingest",
                "base_parameters": {"date": "{{job.parameters.date}}"}
            },
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "Standard_DS3_v2",
                "num_workers": 4
            }
        },
        {
            "task_key": "silver_transform",
            "depends_on": [{"task_key": "bronze_ingest"}],
            "notebook_task": {"notebook_path": "/Production/silver_transform"}
        },
        {
            "task_key": "gold_aggregate",
            "depends_on": [{"task_key": "silver_transform"}],
            "notebook_task": {"notebook_path": "/Production/gold_aggregate"}
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 6 * * ?",
        "timezone_id": "Asia/Kolkata"
    },
    "email_notifications": {
        "on_failure": ["team@company.com"]
    },
    "max_retries": 2,
    "retry_on_timeout": true
}
```

### 7.2 Task Types

| Task Type | Use Case |
|-----------|----------|
| **Notebook** | Run a Databricks notebook |
| **Python script** | Run a Python file from workspace or Git |
| **Spark Submit** | Submit a Spark application JAR/Python |
| **SQL** | Execute a SQL query |
| **dbt** | Run dbt models |
| **Pipeline** | Run a Delta Live Tables pipeline |
| **If/else** | Conditional branching based on task output |
| **For each** | Iterate over a list (dynamic task generation) |

### 7.3 Parameterization

```python
# In the notebook, access job parameters:
dbutils.widgets.text("date", "")
date = dbutils.widgets.get("date")

# Or use task values (cross-task communication):
dbutils.jobs.taskValues.set(key="row_count", value=12345)
# In downstream task:
count = dbutils.jobs.taskValues.get(taskKey="bronze_ingest", key="row_count")
```

---

## 8. Delta Live Tables (DLT)

### 8.1 What Is DLT

Delta Live Tables is a declarative framework for building ETL pipelines. You define the desired state of your tables, and DLT handles execution, orchestration, and monitoring.

### 8.2 DLT Syntax

```python
import dlt
from pyspark.sql.functions import *

@dlt.table(comment="Raw events from landing zone")
def bronze_events():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/data/landing/events/")

@dlt.table(comment="Cleaned and validated events")
@dlt.expect_or_drop("valid_device", "device_id IS NOT NULL")
@dlt.expect_or_drop("valid_value", "value BETWEEN 0 AND 100000")
def silver_events():
    return dlt.read_stream("bronze_events") \
        .withColumn("date", to_date("timestamp")) \
        .dropDuplicates(["device_id", "timestamp"])

@dlt.table(comment="Daily device statistics")
def gold_daily_stats():
    return dlt.read("silver_events") \
        .groupBy("device_id", "date") \
        .agg(avg("value").alias("avg_value"), count("*").alias("count"))
```

### 8.3 DLT Expectations (Data Quality)

```python
# Drop bad rows
@dlt.expect_or_drop("valid_id", "id IS NOT NULL")

# Keep bad rows but flag them
@dlt.expect("valid_range", "value > 0")

# Fail the pipeline on bad data
@dlt.expect_or_fail("critical_field", "amount > 0")
```

### 8.4 DLT vs Manual Pipelines

| Feature | DLT | Manual Pipeline |
|---------|-----|----------------|
| **Dependency management** | Automatic | Manual (Airflow/Workflows) |
| **Incremental processing** | Built-in | Manual watermarks/checkpoints |
| **Data quality** | Expectations framework | Custom code |
| **Error handling** | Automatic retries, quarantine | Custom code |
| **Monitoring** | Built-in dashboard | Custom metrics/logging |
| **Schema evolution** | Automatic | Manual |
| **Complexity** | Lower (declarative) | Higher (imperative) |

---

## 9. Secrets Management

### 9.1 Databricks Secrets

Store sensitive values (passwords, API keys, connection strings) securely:

```python
# Create a secret scope (via CLI or API)
# databricks secrets create-scope --scope my-scope

# Store a secret
# databricks secrets put --scope my-scope --key db-password

# Access in code
password = dbutils.secrets.get(scope="my-scope", key="db-password")
# The value is redacted in notebook output: [REDACTED]
```

### 9.2 Azure Key Vault Integration

```python
# Create a scope backed by Azure Key Vault
# databricks secrets create-scope --scope kv-scope \
#   --scope-backend-type AZURE_KEYVAULT \
#   --resource-id /subscriptions/.../vaults/my-vault

secret = dbutils.secrets.get(scope="kv-scope", key="storage-key")
```

---

## 10. Performance Optimization

### 10.1 Cluster Auto-Scaling

```
Min workers: 2 (always available for quick starts)
Max workers: 20 (scales up for heavy workloads)
Scale-down: remove idle workers after 5 minutes
```

### 10.2 Spot Instances

Use spot/preemptible instances for workers (60-90% cost savings):
```
Driver: on-demand (must not be interrupted)
Workers: spot instances (can be interrupted, tasks will be retried)
```

### 10.3 Delta Cache

Databricks Delta Cache automatically caches frequently-read remote data on local SSDs:
```python
# Enable (default on some cluster types)
spark.conf.set("spark.databricks.io.cache.enabled", True)
spark.conf.set("spark.databricks.io.cache.maxDiskUsage", "50g")
```

### 10.4 Z-ORDER for Query Acceleration

```sql
-- Co-locate data by frequently-filtered columns
OPTIMIZE my_table ZORDER BY (date, device_id);

-- Queries that filter on date AND/OR device_id will skip most files
SELECT * FROM my_table WHERE date = '2024-01-15' AND device_id = 'D001';
-- Without Z-ORDER: scans 1000 files
-- With Z-ORDER: scans ~10 files
```

### 10.5 Liquid Clustering (Databricks-specific, newer)

```sql
-- Replaces static partitioning and Z-ORDER
CREATE TABLE my_table CLUSTER BY (date, device_id);
-- Databricks automatically manages data layout for optimal query performance
```

---

## 11. MLflow in Databricks

### 11.1 Experiment Tracking

```python
import mlflow

mlflow.set_experiment("/Experiments/anomaly_detection")

with mlflow.start_run(run_name="random_forest_v1"):
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 10)
    
    model = train_model(params)
    accuracy = evaluate(model, test_data)
    
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("f1_score", f1)
    
    mlflow.sklearn.log_model(model, "model")
```

### 11.2 Model Registry

```python
# Register a model
mlflow.register_model("runs:/abc123/model", "anomaly_detector")

# Transition to production
client = mlflow.tracking.MlflowClient()
client.transition_model_version_stage("anomaly_detector", version=3, stage="Production")
```

### 11.3 Model Serving

```python
# Load production model for batch inference
model = mlflow.pyfunc.load_model("models:/anomaly_detector/Production")
predictions = model.predict(feature_df)
```

---

## 12. SQL Warehouses and Analytics

### 12.1 SQL Warehouse Types

| Type | Startup | Cost | Best For |
|------|---------|------|----------|
| **Classic** | 2-5 minutes | Compute + storage | Heavy, long-running queries |
| **Serverless** | Seconds | Per-query | Ad-hoc queries, BI dashboards |

### 12.2 Connecting BI Tools

SQL Warehouses provide a JDBC/ODBC endpoint for BI tools:
```
Tableau → ODBC → SQL Warehouse → Delta Lake
Power BI → ODBC → SQL Warehouse → Delta Lake
```

### 12.3 Query Optimization

```sql
-- Use EXPLAIN to understand query plans
EXPLAIN SELECT * FROM my_table WHERE date = '2024-01-15';

-- Check table statistics
DESCRIBE EXTENDED my_table;
ANALYZE TABLE my_table COMPUTE STATISTICS;
ANALYZE TABLE my_table COMPUTE STATISTICS FOR COLUMNS date, device_id;
```

---

## 13. Databricks vs On-Prem Spark

### 13.1 Feature Comparison

| Feature | On-Prem Spark | Databricks |
|---------|--------------|-----------|
| **Setup** | Manual (install, configure, maintain) | Managed (click to create cluster) |
| **Scaling** | Fixed hardware, manual scaling | Auto-scaling, spot instances |
| **Delta Lake** | Open-source version | Full version + optimizations |
| **Photon** | Not available | Included |
| **Unity Catalog** | Not available | Included |
| **Auto Loader** | Not available | Included |
| **DLT** | Not available | Included |
| **Monitoring** | Custom (Ganglia, Prometheus) | Built-in |
| **Cost** | Hardware + ops team | Pay-per-use compute + storage |
| **Updates** | Manual Spark upgrades | Automatic runtime updates |

### 13.2 Migration Considerations

```
On-prem to Databricks migration:
1. Storage: HDFS → Cloud storage (S3/ADLS) → Delta Lake
2. Compute: YARN clusters → Databricks clusters
3. Jobs: Airflow/Oozie → Databricks Workflows (or keep Airflow calling Databricks)
4. Tables: Hive metastore → Unity Catalog
5. Code: spark-submit → Notebooks or Repos + Jobs
6. Security: Ranger/Sentry → Unity Catalog ACLs
```

---

## 14. Quick-Reference Cheat Sheet

### Cluster Selection

```
Interactive exploration → All-Purpose cluster
Production ETL → Job cluster (ephemeral)
SQL/BI queries → SQL Warehouse (serverless)
ML training → ML Runtime + GPU instances
```

### Delta Lake Commands

```sql
-- Table management
OPTIMIZE table_name;                          -- compact small files
OPTIMIZE table_name ZORDER BY (col1, col2);   -- co-locate data
VACUUM table_name RETAIN 168 HOURS;           -- delete old files
DESCRIBE HISTORY table_name;                  -- show version history
RESTORE TABLE table_name TO VERSION AS OF 5;  -- rollback

-- Upsert
MERGE INTO target USING source ON condition
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- Schema evolution
ALTER TABLE t ADD COLUMNS (new_col STRING);
ALTER TABLE t SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');
ALTER TABLE t RENAME COLUMN old_name TO new_name;
```

### Auto Loader Quick Reference

```python
spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json|parquet|csv")
    .option("cloudFiles.schemaLocation", "/checkpoint/schema/")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns|rescue|failOnNewColumns|none")
    .option("cloudFiles.useNotifications", True)  # event-based (faster for large dirs)
    .load("/landing/path/")
```

### Unity Catalog Hierarchy

```
Catalog → Schema → Table/View/Function/Model
             └→ Volume (for unstructured files)

Access: GRANT <privilege> ON <object> TO <principal>
Privileges: SELECT, MODIFY, CREATE TABLE, USAGE, ALL PRIVILEGES
```

### Cost Optimization

```
1. Use job clusters (not all-purpose) for production → auto-terminate
2. Use spot instances for workers → 60-90% savings
3. Auto-scale clusters → pay only for what you use
4. Use serverless SQL warehouses → no idle cost
5. OPTIMIZE + VACUUM regularly → reduce storage + improve reads
6. Use Z-ORDER on frequently filtered columns → skip files
7. Enable Delta Cache → reduce remote storage reads
```
