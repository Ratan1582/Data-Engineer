# Databricks Complete Interview Guide

> Tailored for: **Vemula Ratan** | Data Engineer | ~3 months Databricks experience
> Background: 2+ years PySpark/Spark, battery analytics + smart meter pipelines at Jio
> Goal: Prepare for Data Engineer interviews at product-based companies using Databricks

---

## Table of Contents

1. [Section 1 — Databricks Fundamentals](#section-1--databricks-fundamentals)
2. [Section 2 — Knowledge Assessment Matrix](#section-2--knowledge-assessment-matrix)
3. [Section 3 — Interview Questions (Beginner to Advanced)](#section-3--interview-questions-beginner-to-advanced)
4. [Section 4 — Answer Segmentation (Can Answer vs Ideal Answer)](#section-4--answer-segmentation)
5. [Section 5 — Real-World Scenarios](#section-5--real-world-scenarios)
6. [Section 6 — Structured Learning Roadmap](#section-6--structured-learning-roadmap)

---

# Section 1 — Databricks Fundamentals

## 1.1 What Is Databricks?

Databricks is a **unified data analytics platform** built on top of Apache Spark. It was founded by the original creators of Spark, Delta Lake, and MLflow.

**In simple terms:** Databricks = Managed Spark + Delta Lake + Collaboration + Orchestration + ML Platform

**What it solves that raw Spark doesn't:**
- You don't manage Spark clusters yourself (no YARN, no Mesos, no Hadoop)
- Auto-scaling: clusters grow/shrink based on workload
- Delta Lake: ACID transactions on data lakes (your Parquet files get superpowers)
- Built-in scheduling, monitoring, alerting
- Collaborative notebooks with version control

**Your context:** You've been running Spark on self-managed Hadoop clusters with HDFS. Databricks replaces that infrastructure layer while keeping your PySpark code largely unchanged. Your migration path: HDFS Parquet → Delta Lake on cloud storage, shell-script scheduling → Databricks Workflows.

## 1.2 Databricks Architecture

```
┌──────────────────────────────────────────────────────┐
│                   CONTROL PLANE                       │
│         (Managed by Databricks)                       │
│  ┌──────────┐ ┌──────────┐ ┌──────────────────────┐  │
│  │ Web UI / │ │ Workflow │ │ Cluster Manager      │  │
│  │ Notebooks│ │ Scheduler│ │ (creates/destroys)   │  │
│  └──────────┘ └──────────┘ └──────────────────────┘  │
│  ┌──────────┐ ┌──────────┐ ┌──────────────────────┐  │
│  │ Unity    │ │ Repos /  │ │ Secret Manager       │  │
│  │ Catalog  │ │ Git      │ │                      │  │
│  └──────────┘ └──────────┘ └──────────────────────┘  │
└──────────────────────────────────────────────────────┘
                         │
                    API / REST
                         │
┌──────────────────────────────────────────────────────┐
│                    DATA PLANE                         │
│        (Runs in YOUR cloud account)                   │
│  ┌──────────────────────────────────────────────────┐ │
│  │  Spark Cluster (Driver + Executors)              │ │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐   │ │
│  │  │ Driver │ │Worker 1│ │Worker 2│ │Worker N│   │ │
│  │  └────────┘ └────────┘ └────────┘ └────────┘   │ │
│  └──────────────────────────────────────────────────┘ │
│  ┌──────────────────────────────────────────────────┐ │
│  │  Cloud Storage (S3 / ADLS / GCS)                 │ │
│  │  Delta Tables, Parquet, CSV, etc.                │ │
│  └──────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────┘
```

### Key Architectural Points:

**Control Plane** (managed by Databricks):
- Web UI, notebook execution environment
- Cluster lifecycle management (create, scale, terminate)
- Workflow scheduling and monitoring
- Unity Catalog (data governance)
- This lives in DATABRICKS' cloud account

**Data Plane** (in YOUR cloud account):
- Actual Spark clusters that process data
- Cloud storage where your data lives (S3, ADLS, GCS)
- Your data NEVER leaves your cloud account
- This is critical for security/compliance interviews

### Why this matters in interviews:
"Where does my data live?" → "In our cloud account, in the data plane. Databricks' control plane only sends instructions; it never sees or stores our raw data."

## 1.3 Key Databricks Components

### Workspace
The Databricks UI where you write code, manage clusters, and schedule jobs. Contains:
- **Notebooks** — interactive documents mixing code (Python, SQL, Scala, R), markdown, and visualizations
- **Repos** — Git integration for version-controlled code
- **Jobs/Workflows** — scheduled pipeline execution
- **Clusters** — compute resources

### Clusters (Two Types)

| Feature | All-Purpose Cluster | Job Cluster |
|---------|---------------------|-------------|
| Use case | Development, exploration | Production pipeline execution |
| Lifecycle | You start/stop manually | Auto-created when job starts, auto-destroyed when done |
| Cost | More expensive (runs until stopped) | Cost-optimized (runs only for job duration) |
| Sharing | Multiple users can attach | Single job only |

**Your context:** For development (notebook exploration), use all-purpose clusters. For production pipelines (daily aggregation, SLA computation), use job clusters — they start, run the job, and terminate. This saves significant cost.

### DBFS (Databricks File System)
An abstraction layer on top of cloud storage. `dbfs:/mnt/data/` might map to `s3://my-bucket/data/` or `abfss://container@account.dfs.core.windows.net/data/`.

**Your context:** Your current HDFS paths like `hdfs://hadoop-master:8020/user/hive/warehouse/smart_battery.db/` would become `dbfs:/mnt/smart_battery/` or a Unity Catalog managed location.

### Delta Lake
The most important Databricks feature for interviews. See Section 3 for detailed coverage.

**One-liner:** Delta Lake = Parquet + Transaction Log + ACID guarantees

### Unity Catalog
Centralized governance layer for all data assets. Manages:
- **Access control** — who can read/write which tables
- **Data lineage** — where data came from and where it goes
- **Audit logging** — who accessed what and when
- **Data discovery** — search and browse all tables/views

### Photon Engine
Databricks' native vectorized query engine written in C++. Replaces Spark's JVM-based execution for supported operations. 2-8x faster for many workloads, especially:
- Scans and filters on Delta tables
- Aggregations
- Joins

**Your context:** Your aggregation pipelines (block, daily, monthly) would likely see significant speedup on Photon because they're heavy on groupBy-agg operations.

## 1.4 How Databricks Extends Apache Spark

| Feature | Apache Spark (What You Use Now) | Databricks (What It Adds) |
|---------|-------------------------------|---------------------------|
| Storage | HDFS + Parquet | Delta Lake (ACID, time travel, MERGE) |
| Compute | Self-managed clusters | Auto-scaling managed clusters |
| Scheduling | Shell scripts + cron | Databricks Workflows |
| Monitoring | Spark UI, custom logs | Built-in monitoring + alerting |
| File Ingestion | Manual `spark.read.parquet()` | Auto Loader (incremental, schema evolution) |
| Data Governance | None / manual | Unity Catalog |
| Optimization | Manual tuning | Photon + Auto-optimize + Z-ordering |
| ML | Custom integration (your PyTorch UDF) | MLflow built-in |
| Collaboration | SSH to server, share scripts | Notebooks, Git repos, shared clusters |

---

# Section 2 — Knowledge Assessment Matrix

## 2.1 What You Already Know (Strong Foundation)

These are skills you use DAILY. In interviews, you can speak confidently about these with real examples.

| Topic | Your Experience Level | Evidence from Your Code |
|-------|----------------------|------------------------|
| PySpark DataFrames | Expert | All pipelines — transformations, aggregations, joins |
| `groupBy` + `agg` | Expert | Block/daily/monthly aggregation, SLA metrics |
| Spark Joins (inner, left_anti, broadcast) | Expert | Daily aggregation (inner), dedup (left_anti), SBD (broadcast) |
| `applyInPandas` / `pandas_udf` | Expert | Battery cycle detection, PyTorch model inference |
| Reading/Writing Parquet | Expert | All pipelines — HDFS parquet I/O |
| Spark Configurations | Strong | `ignoreCorruptFiles`, `arrow.pyspark.enabled`, StorageLevel |
| `persist` / `unpersist` | Strong | Memory management across all pipelines |
| Broadcast Variables | Strong | Data quality report, metadata, PyTorch model |
| Window Functions | Strong | Monthly billing (row_number), battery preprocessing (last fill) |
| Pivot Operations | Strong | Block aggregation with pre-aggregation optimization |
| Python / Pandas | Expert | Cycle detection, energy estimation, data quality |
| ETL Pipeline Design | Strong | Incremental ingestion, quality gates, multi-sink |

## 2.2 What You Partially Know (~3 Months Databricks)

These you've started using but need to deepen for interviews.

| Topic | What You Know | What You Need to Learn |
|-------|--------------|----------------------|
| Databricks Notebooks | Basic usage: writing PySpark code, running cells | Magic commands (`%sql`, `%md`, `%run`), widgets, dashboards, notebook workflows |
| Cluster Configuration | Creating clusters, basic settings | Instance types, auto-scaling policies, Spark config tuning, cluster pools, init scripts |
| Databricks Jobs | Running notebooks as jobs | Multi-task workflows, dependencies, retry policies, alerts, parameters |
| DBFS | Reading/writing files | Mount points, Unity Catalog volumes, managed vs external tables |
| Delta Lake (Basics) | Reading/writing Delta tables | MERGE, time travel, Z-ordering, VACUUM, OPTIMIZE, change data feed |

## 2.3 What You Must Learn for Interviews (Critical Gaps)

These are heavily asked in Databricks-focused interviews and you need to learn them.

| Topic | Priority | Why It's Asked | Estimated Study Time |
|-------|----------|---------------|---------------------|
| **Delta Lake (Advanced)** | CRITICAL | Asked in 90%+ of Databricks interviews | 1 week |
| **Medallion Architecture** | CRITICAL | Standard Databricks design pattern (Bronze/Silver/Gold) | 2-3 days |
| **Auto Loader** | HIGH | Databricks' answer to incremental file ingestion (replaces YOUR last-read-path pattern) | 2-3 days |
| **MERGE (Upsert)** | HIGH | How to update + insert data in Delta tables | 2 days |
| **Z-Ordering** | HIGH | Data skipping optimization for Delta tables | 1 day |
| **VACUUM + OPTIMIZE** | HIGH | Delta table maintenance | 1 day |
| **Unity Catalog** | MEDIUM | Data governance, access control | 2-3 days |
| **Databricks Workflows** | MEDIUM | Pipeline orchestration (replaces your shell scripts) | 2 days |
| **Structured Streaming** | MEDIUM | Real-time processing on Databricks | 3-4 days |
| **Photon Engine** | MEDIUM | Databricks' native execution engine | 1 day |
| **Delta Live Tables (DLT)** | LOW-MEDIUM | Declarative pipeline framework | 2-3 days |
| **MLflow** | LOW | ML experiment tracking (relevant given your PyTorch work) | 1-2 days |
| **Cost Optimization** | MEDIUM | DBU pricing, spot instances, cluster sizing | 1 day |

---

# Section 3 — Interview Questions (Beginner to Advanced)

## 3.1 Beginner Level

### Q1: "What is Databricks and how is it different from Apache Spark?"

**What you can say now:**
"Databricks is a managed platform built on top of Apache Spark. I've been using Spark on Hadoop for 2 years, and recently started using Databricks. The main difference is that Databricks handles infrastructure — cluster management, auto-scaling, scheduling — so I focus on writing the pipeline logic, not managing servers."

**Ideal answer (learn this):**
"Databricks is a unified data analytics platform founded by the creators of Apache Spark. While Spark is just the compute engine, Databricks adds:
1. **Delta Lake** — ACID transactions on your data lake, converting it into a lakehouse
2. **Managed infrastructure** — auto-scaling clusters, spot instance management
3. **Collaboration** — shared notebooks, Git integration, access controls
4. **Orchestration** — Workflows for scheduling and monitoring pipelines
5. **Photon** — a native C++ execution engine that's 2-8x faster than Spark for many operations
6. **Unity Catalog** — centralized data governance

The key concept is the **lakehouse architecture** — combining the best of data lakes (cheap storage, schema flexibility) with data warehouses (ACID transactions, schema enforcement, performance)."

### Q2: "What is Delta Lake?"

**What you can say now:**
"Delta Lake is Databricks' storage layer that adds reliability to data lakes. It's like an improved version of Parquet with transaction support."

**Ideal answer (learn this):**
"Delta Lake is an open-source storage layer that brings ACID transactions to data lakes. It stores data as Parquet files plus a **transaction log** (`_delta_log/` directory) that records every change.

Key capabilities:
1. **ACID Transactions** — concurrent reads and writes are safe. No more corrupted files from failed writes.
2. **Time Travel** — query data as it existed at any point in time: `SELECT * FROM table VERSION AS OF 5`
3. **Schema Enforcement** — prevents writing data that doesn't match the table schema
4. **Schema Evolution** — safely add new columns: `.option('mergeSchema', 'true')`
5. **MERGE (Upsert)** — update existing rows and insert new ones in a single atomic operation
6. **OPTIMIZE + Z-ORDER** — compact small files and co-locate related data for faster queries
7. **VACUUM** — remove old files that are no longer needed

In my context, my existing Parquet-on-HDFS pipelines would benefit from Delta Lake because: right now, if my daily aggregation fails mid-write, I get partially written Parquet files. With Delta Lake, the write is atomic — either all data is written or none is."

### Q3: "What is a Databricks cluster? What types exist?"

**What you can say now:**
"A cluster is a set of machines that run Spark. Databricks manages them — I just specify the size and configuration."

**Ideal answer:**
"A Databricks cluster consists of a driver node and zero or more worker nodes. There are two types:

1. **All-Purpose Cluster** — for interactive development. Stays running until manually stopped. Multiple users can attach notebooks. More expensive because it runs continuously.

2. **Job Cluster** — for production workloads. Created automatically when a job starts, terminated when it finishes. Single job only. Cost-optimized because you only pay for compute time.

For my pipelines, I'd use:
- All-purpose cluster during development (testing my aggregation logic on a sample)
- Job clusters for production (daily aggregation job creates its cluster, runs, terminates)

Key configuration decisions:
- **Instance type** — affects CPU, memory, disk. For my I/O-heavy parquet processing, I'd choose storage-optimized instances.
- **Auto-scaling** — set min/max workers. Databricks adds workers during heavy computation stages and removes them during light stages.
- **Spot instances** — use cloud provider's excess capacity at 60-90% discount. Risk: instances can be reclaimed. Good for fault-tolerant batch jobs (Spark retries failed tasks)."

### Q4: "What is DBFS?"

**Answer:**
"DBFS (Databricks File System) is an abstraction layer that lets you access cloud storage using a familiar file system interface. When I write `dbfs:/data/output.parquet`, it's actually stored in the underlying cloud storage (S3, ADLS, GCS).

Three ways to access data:
1. **DBFS paths** — `dbfs:/mnt/data/` (abstracted)
2. **Direct cloud paths** — `s3://bucket/data/` or `abfss://container@account/data/`
3. **Unity Catalog** — `catalog.schema.table` (recommended for governance)

For my HDFS-based pipelines, the migration would be:
- Replace `hdfs://hadoop-master:8020/user/hive/warehouse/...` with `abfss://container@storageaccount/warehouse/...` or Unity Catalog table references
- The PySpark read/write code stays almost identical"

## 3.2 Intermediate Level

### Q5: "Explain the Medallion Architecture (Bronze/Silver/Gold)."

**What you can say now (limited):**
"I've heard of it — it's about organizing data into layers of increasing quality."

**Ideal answer (LEARN THIS — asked very frequently):**
"The Medallion Architecture organizes a data lakehouse into three layers:

**Bronze (Raw):**
- Raw data as ingested from sources, minimal transformation
- Append-only, preserves history
- Schema might be messy — nulls, duplicates, different formats
- In MY context: the raw parquet files from HDFS — vendor data as received, partitioned by date

**Silver (Cleansed/Conformed):**
- Cleaned, deduplicated, schema-enforced data
- Business logic applied: joins with metadata, type conversions, null handling
- In MY context: after my preprocessing layer — BMS/Zabbix data normalized to canonical schema, data quality checks applied, bad records filtered

**Gold (Business-Level Aggregates):**
- Aggregated, business-ready data for dashboards and reporting
- In MY context: the daily/monthly aggregation results, SLA completeness metrics, battery KPIs — ready for Cassandra/dashboard consumption

My existing pipeline ALREADY follows this pattern implicitly:
```
Raw HDFS Parquet → Preprocessing/Normalization → Aggregation → Cassandra/Reports
     Bronze              Silver                      Gold
```
Databricks formalizes this with Delta tables at each layer."

### Q6: "What is Auto Loader? How does it work?"

**What you can say now:**
"I'm not sure about Auto Loader specifically, but I built my own incremental file tracking system."

**Ideal answer (LEARN THIS — it directly replaces YOUR last-read-path pattern):**
"Auto Loader is Databricks' mechanism for incrementally and efficiently processing new files as they arrive in cloud storage. It uses `cloudFiles` as the format:

```python
df = spark.readStream.format('cloudFiles') \
    .option('cloudFiles.format', 'parquet') \
    .option('cloudFiles.schemaLocation', '/schema/path') \
    .load('/data/landing/')

df.writeStream \
    .option('checkpointLocation', '/checkpoints/') \
    .trigger(availableNow=True) \
    .toTable('bronze_table')
```

How it works:
1. **File discovery** — uses cloud provider's notification service (S3 events, ADLS events) or directory listing to detect new files
2. **Checkpoint** — tracks which files have been processed (stored in `checkpointLocation`)
3. **Schema inference** — automatically infers and evolves schema as new files arrive
4. **Exactly-once processing** — each file is processed exactly once

**Why this matters for me:** I built a manual version of this — my last-read-path tracking DataFrame that records the last parquet file processed per vendor per date. Auto Loader does this automatically, more reliably, and with built-in schema evolution. Migrating my `data_fetch_pyspark.py` to Auto Loader would eliminate ~200 lines of custom tracking code."

### Q7: "Explain MERGE in Delta Lake."

**What you can say now:**
"I use Cassandra's upsert behavior for this, but I haven't used Delta MERGE specifically."

**Ideal answer (LEARN THIS):**
"MERGE is Delta Lake's upsert operation — it atomically updates, inserts, and optionally deletes rows in a single statement:

```sql
MERGE INTO target_table AS target
USING source_data AS source
ON target.device_id = source.device_id AND target.date = source.date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

How it works:
1. Joins source and target on the match condition
2. For matching rows: updates them (WHEN MATCHED)
3. For non-matching source rows: inserts them (WHEN NOT MATCHED)
4. Optional: delete matched rows that meet a condition (WHEN MATCHED AND condition THEN DELETE)

**In my context:** I currently use `left_anti` join + append to avoid duplicates:
```python
new_df = daily_agg_df.join(existing_devices, on='IMEI', how='left_anti')
new_df.write.mode('append').parquet(path)
```

With Delta MERGE, this becomes cleaner:
```python
target_table.alias('t').merge(
    source_df.alias('s'),
    't.IMEI = s.IMEI AND t.date = s.date'
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

Advantages: atomic (no partial writes), handles both updates and inserts, no need for manual dedup logic."

### Q8: "What is Z-Ordering?"

**Answer:**
"Z-ordering is a data layout optimization in Delta Lake that co-locates related data in the same set of files. It's like creating a multi-column index.

```sql
OPTIMIZE my_table ZORDER BY (vendor, date)
```

How it works:
- Re-arranges data so that rows with similar `vendor` and `date` values are stored in the same files
- When you query `WHERE vendor = 'VendorA' AND date = '2025-01-15'`, Spark skips files that don't contain these values (data skipping via min/max statistics)

**In my context:** My SLA tables are queried by vendor + date. Without Z-ordering, data for VendorA might be scattered across 1000 files. With Z-ordering, it's concentrated in 5-10 files. A dashboard query that used to scan 1000 files now scans 5.

When to use:
- Columns frequently used in WHERE clauses or join conditions
- High-cardinality columns (device_id, vendor, date)
- NOT low-cardinality columns (boolean flags) — Z-ordering won't help

Limitation: you can only Z-order by 2-4 columns effectively. More columns dilute the benefit."

### Q9: "What are OPTIMIZE and VACUUM in Delta Lake?"

**Answer:**
"**OPTIMIZE** compacts small files into larger ones:
```sql
OPTIMIZE my_table
```
This merges many small Parquet files (common after streaming or frequent appends) into fewer, larger files (~1GB each). Fewer files = faster queries (less metadata, less file-open overhead).

**VACUUM** removes old files that are no longer referenced by the Delta transaction log:
```sql
VACUUM my_table RETAIN 168 HOURS  -- keep 7 days of history
```
Delta Lake keeps old file versions for time travel. VACUUM deletes files older than the retention period.

Why this matters:
- Without OPTIMIZE: after 30 days of daily appends, a table might have 30,000 small files. Queries slow down.
- Without VACUUM: storage costs grow indefinitely as old versions accumulate.

**In my context:** My daily aggregation appends ~4M rows per day. After a month, the Delta table would have 30 daily append files. Running `OPTIMIZE` weekly compacts them. Running `VACUUM` monthly removes files older than the retention window."

## 3.3 Advanced Level

### Q10: "How would you optimize a slow Databricks job?"

**Answer:**
"My systematic approach:

1. **Check the Spark UI** (via Databricks cluster's Spark UI tab):
   - Stages with high shuffle read/write → join/groupBy optimization needed
   - Tasks with high GC time → memory pressure → increase executor memory or reduce partition size
   - Skewed tasks (one task 10x longer) → data skew → salting or AQE

2. **Enable Photon** — switch to Photon-enabled cluster. For scan/filter/aggregate-heavy jobs (like my block aggregation), Photon gives 2-5x speedup for free.

3. **Use Delta Lake optimizations:**
   - Z-order on frequently filtered columns
   - OPTIMIZE to compact small files
   - Enable auto-compaction: `delta.autoOptimize.autoCompact = true`

4. **Cluster tuning:**
   - Right-size instances (memory-optimized for join-heavy, storage-optimized for scan-heavy)
   - Enable auto-scaling for bursty workloads
   - Use spot instances for cost savings with fault tolerance

5. **Code-level optimizations** (same as my existing Spark knowledge):
   - Pre-aggregate before pivot
   - Broadcast small tables
   - Persist and unpersist strategically
   - Reduce shuffle by repartitioning once

6. **Use `EXPLAIN`** to check the physical plan:
   ```python
   df.explain(True)
   ```
   Look for: unnecessary shuffles, suboptimal join strategies, missing predicate pushdown."

### Q11: "Explain Delta Lake's transaction log. How does ACID work on a data lake?"

**Answer:**
"Delta Lake stores a transaction log in the `_delta_log/` directory alongside the Parquet data files. Each transaction (write, update, delete, merge) creates a new JSON log entry (e.g., `_delta_log/000000001.json`).

The log records:
- **add** — new files added to the table
- **remove** — files logically removed (not physically deleted until VACUUM)
- **metadata** — schema changes, table properties
- **commitInfo** — timestamp, operation type, who did it

**ACID guarantees:**
- **Atomicity** — a write either adds ALL its files to the log or NONE. If a job crashes mid-write, the partially written files are never visible.
- **Consistency** — schema enforcement prevents writing incompatible data
- **Isolation** — readers see a consistent snapshot (Serializable isolation by default). A reader that started before a write completes sees the pre-write data.
- **Durability** — once a commit is in the transaction log, it's durable (stored in cloud storage with replication)

**How it solves MY problems:**
Right now, if my daily aggregation job fails during `df.write.mode('overwrite').parquet(path)`, I might get partially written files. The next run overwrites everything, but there's a window where downstream readers see corrupt data. With Delta Lake, the write is atomic — either the entire DataFrame is committed or nothing is."

### Q12: "What is Structured Streaming in Databricks? How does it compare to batch?"

**Answer:**
"Structured Streaming treats a stream as an unbounded table that keeps growing. You write the same DataFrame/SQL operations as batch, but they execute incrementally on new data.

```python
# Batch (what I do now):
df = spark.read.parquet('/data/meters/')
result = df.groupBy('vendor').agg(count('*'))
result.write.parquet('/output/')

# Streaming equivalent:
df = spark.readStream.format('cloudFiles').load('/data/meters/')
result = df.groupBy('vendor').agg(count('*'))
result.writeStream.format('delta').toTable('metrics')
```

Key concepts:
- **Trigger** — how often to process new data:
  - `trigger(availableNow=True)` — process all available data, then stop (batch-like)
  - `trigger(processingTime='1 minute')` — micro-batch every minute
  - `trigger(continuous='1 second')` — true continuous (experimental)
- **Checkpoint** — stores processing state so the stream can resume after failure
- **Output modes** — append (new rows only), complete (full result), update (changed rows only)

**For my pipelines:** I'd use `trigger(availableNow=True)` — this gives me the incremental processing of streaming (only process new files) with the batch-like behavior of running on a schedule. It's essentially what my last-read-path pattern does, but built into Spark."

### Q13: "What is Unity Catalog and why does it matter?"

**Answer:**
"Unity Catalog is Databricks' centralized data governance solution. It provides:

1. **Three-level namespace** — `catalog.schema.table` (like a database server → database → table)
2. **Fine-grained access control** — grant SELECT on specific tables/columns to specific users/groups
3. **Data lineage** — automatically tracks which tables were read/written by which jobs
4. **Audit logging** — who accessed what data and when
5. **Data discovery** — search and browse all tables with descriptions and tags

Why it matters:
- Before Unity Catalog: every user could read every file on DBFS. No governance.
- With Unity Catalog: tables have owners, permissions, and audit trails.

**For my pipelines:** Unity Catalog would give me:
- Separate catalogs for dev/staging/prod
- Permissions: the SLA dashboard can read `gold.sla_metrics` but not `bronze.raw_meter_data`
- Lineage: know that `gold.daily_aggregation` depends on `silver.meter_readings`
- Migration path from HDFS/Hive: `spark.sql('SELECT * FROM smart_battery.daily_summaries')` → `spark.sql('SELECT * FROM prod.smart_battery.daily_summaries')`"

### Q14: "Explain Delta Live Tables (DLT)."

**Answer:**
"Delta Live Tables is a declarative framework for building data pipelines. Instead of writing imperative Spark code (read → transform → write), you declare the desired state of each table:

```python
import dlt

@dlt.table
def bronze_meter_data():
    return spark.readStream.format('cloudFiles').load('/raw/meters/')

@dlt.table
def silver_meter_data():
    return dlt.read_stream('bronze_meter_data') \
        .filter(col('value').isNotNull()) \
        .dropDuplicates(['IMEI', 'Timestamp'])

@dlt.table
def gold_daily_aggregation():
    return dlt.read('silver_meter_data') \
        .groupBy('IMEI', 'Vendor', 'date') \
        .agg(...)
```

DLT handles:
- **Dependency resolution** — knows gold depends on silver depends on bronze
- **Incremental processing** — only processes new data
- **Data quality** — `@dlt.expect` for quality checks
- **Error handling** — quarantines bad records
- **Auto-scaling** — adjusts compute based on data volume

**For my pipeline:** My current architecture (raw HDFS → preprocessing → aggregation → Cassandra) maps naturally to DLT:
```
Bronze (Auto Loader) → Silver (normalization + quality) → Gold (aggregation + KPIs)
```

The difference: I currently manage the orchestration (which job runs when, error handling, reprocessing) manually. DLT handles it declaratively."

## 3.4 Scenario-Based Questions

### Q15: "Your daily pipeline processes 4M meter readings and is timing out on Databricks. How do you debug and fix it?"

**Answer:**
"Step-by-step:

1. **Check the job run details** — Databricks shows run duration, cluster utilization, and Spark UI for each run.

2. **Check for auto-scaling issues** — is the cluster scaling up to handle the load? If max workers is too low, increase it. If it's not scaling fast enough, check auto-scaling settings.

3. **Check for small file problem** — if the Delta table has thousands of small files from daily appends, reads are slow. Run `OPTIMIZE`.

4. **Check for skewed tasks** in Spark UI — one executor processing 2M meters while others process 200K. Fix: repartition, enable AQE skew handling.

5. **Enable Photon** — if not already enabled, switch to a Photon-optimized cluster. For my aggregation workload, this could give 2-4x speedup.

6. **Apply code optimizations:**
   - Pre-aggregate before pivot
   - Broadcast metadata tables
   - Z-order on frequently queried columns

7. **Consider Auto Loader** — if the bottleneck is file discovery (listing thousands of files on cloud storage), Auto Loader with file notifications is more efficient than directory listing.

8. **Right-size the cluster** — for 4M meters:
   - Estimate data size: ~100GB/day
   - Target: 45 min processing window
   - Need: ~16 cores, 128GB total memory (4-8 worker nodes depending on instance type)
   - Use auto-scaling: min 2, max 8 workers"

### Q16: "How would you migrate your existing HDFS-based Spark pipeline to Databricks?"

**Answer:**
"This is literally what I'm doing. My migration plan:

**Phase 1: Lift and Shift (minimal changes)**
1. Move data from HDFS to cloud storage (ADLS/S3)
2. Replace HDFS paths with cloud storage paths in code
3. Run existing PySpark code on Databricks clusters
4. Use Databricks Jobs to replace shell script scheduling

**Phase 2: Adopt Delta Lake**
1. Convert Parquet tables to Delta: `spark.read.parquet(path).write.format('delta').save(delta_path)`
2. Replace `write.mode('overwrite').parquet()` with `write.format('delta').mode('overwrite')`
3. Implement MERGE for upsert patterns (replacing my left_anti + append)
4. Enable time travel for data debugging

**Phase 3: Optimize for Databricks**
1. Replace my last-read-path tracking with Auto Loader
2. Enable Photon for aggregation-heavy jobs
3. Implement Z-ordering on frequently queried columns (vendor, date)
4. Set up OPTIMIZE and VACUUM schedules
5. Move to Unity Catalog for governance

**Phase 4: Modernize Architecture**
1. Formalize Bronze/Silver/Gold with Delta tables
2. Consider Delta Live Tables for declarative pipeline definition
3. Use MLflow for the disaggregation model management
4. Set up proper monitoring and alerting via Databricks Workflows"

---

# Section 4 — Answer Segmentation (Can Answer vs Ideal Answer)

> For each question, this section shows: (a) what you can confidently answer TODAY based on your experience, (b) the GAP you need to fill, and (c) the ideal complete answer.

## 4.1 Delta Lake Questions

### "How does Delta Lake handle concurrent writes?"

| Aspect | Your Current Knowledge | Gap to Fill |
|--------|----------------------|-------------|
| **Can say** | "I know Delta Lake has transaction support, which prevents the partial-write problem I face with Parquet." | You lack specifics on the mechanism. |
| **LEARN** | Delta uses **Optimistic Concurrency Control (OCC)**. Multiple writers can write simultaneously. On commit, Delta checks if the files being modified were changed by another writer since the transaction started. If no conflict, both commits succeed. If conflict, the later writer retries. Conflicts only happen when two transactions modify the SAME files (same partition). |
| **Ideal answer** | "Delta Lake uses optimistic concurrency control. Writers operate independently, and conflict detection happens at commit time by checking the transaction log. Two appends to different partitions never conflict. Two updates to the same partition will serialize — the second writer retries. This is vastly better than my current Parquet setup where concurrent writes can corrupt data. For my daily aggregation, since each vendor's data goes to different partitions, multiple vendor pipelines can run concurrently without conflicts." |

### "Explain Delta Lake time travel."

| Aspect | Your Current Knowledge | Gap to Fill |
|--------|----------------------|-------------|
| **Can say** | "I know Delta keeps version history so you can query old data." | You haven't used the syntax or understood retention policies. |
| **LEARN** | Every commit creates a new version. Query old versions with: `df = spark.read.format('delta').option('versionAsOf', 5).load(path)` or `spark.sql("SELECT * FROM table TIMESTAMP AS OF '2025-01-15'")`. Default retention: 30 days. Controlled by `delta.logRetentionDuration` and `VACUUM`. |
| **Ideal answer** | "Time travel lets you query any historical version of a Delta table. Each write creates a new version in the transaction log. I can query by version number or timestamp. Practical use cases: (1) debugging — 'what did the daily aggregation output look like yesterday before the re-run?', (2) auditing — 'show me the SLA metrics as they were reported last Monday', (3) rollback — `RESTORE TABLE t TO VERSION AS OF 5` if a bad write corrupted data. In my pipeline, this would replace my current approach of keeping date-partitioned snapshots on HDFS." |

## 4.2 Architecture Questions

### "Design a data pipeline on Databricks for processing IoT data from 4M devices."

| Aspect | Your Current Knowledge | Gap to Fill |
|--------|----------------------|-------------|
| **Can say** | "I can explain the complete pipeline I built: incremental HDFS ingestion, preprocessing, aggregation, SLA monitoring, Cassandra writes. I know Spark optimization deeply." | You need to frame it using Databricks-native services instead of custom components. |
| **LEARN** | Replace your custom components with Databricks equivalents: last-read-path → Auto Loader, shell scripts → Workflows, Parquet → Delta, HDFS → cloud storage, manual monitoring → built-in alerting. |
| **Ideal answer** | "**Ingestion:** Auto Loader monitors the landing zone in ADLS/S3. As new parquet files arrive from vendors, Auto Loader picks them up incrementally with exactly-once semantics and writes to the Bronze Delta table. No custom file tracking needed. **Processing (Silver):** A Databricks Workflow triggers the preprocessing notebook — schema normalization (BMS/Zabbix unification), data quality checks, deduplication. Results are MERGEd into the Silver Delta table. **Aggregation (Gold):** Separate jobs for block, daily, monthly aggregation read from Silver, compute metrics, and MERGE into Gold tables. Z-ordered by (vendor, date) for fast dashboard queries. **SLA Monitoring:** Separate job computes completeness metrics and writes to a Gold SLA table. **Orchestration:** Multi-task Databricks Workflow with dependencies: Bronze → Silver → Gold (block, daily, SLA in parallel). Job clusters for each task. Alerts on failure. **Governance:** Unity Catalog for access control. Bronze readable by data engineers only. Gold readable by dashboards and analysts." |

### "How would you implement the medallion architecture for your meter data?"

| Aspect | Your Current Knowledge | Gap to Fill |
|--------|----------------------|-------------|
| **Can say** | "My pipeline already has raw → cleaned → aggregated layers. I can explain each stage in detail." | Frame it with Delta Lake specifics: table formats, MERGE patterns, quality constraints. |
| **LEARN** | Bronze uses Auto Loader append. Silver uses MERGE with quality expectations (`@dlt.expect`). Gold uses aggregation into Delta tables with Z-ordering. |
| **Ideal answer** | "**Bronze:** `CREATE TABLE bronze.meter_readings USING DELTA` — Auto Loader appends raw vendor data. Schema: (IMEI, Vendor, Timestamp, Profile, Register, Value). No transformations. Retain 90 days. **Silver:** MERGE from Bronze with transformations: pivot registers to columns, normalize vendor schemas, apply data quality checks (non-null IMEI, valid timestamps, non-negative energy values). Partitioned by (vendor, date). **Gold — Block Aggregation:** Read from Silver, pre-aggregate + pivot by hour, write to `gold.block_energy`. Z-ordered by (IMEI, date). **Gold — Daily Aggregation:** Read two days from Silver, compute deltas, MERGE into `gold.daily_consumption`. **Gold — SLA:** Compute completeness metrics, write to `gold.sla_metrics`. Z-ordered by (vendor, date). All Gold tables are the source for dashboards and APIs." |

## 4.3 Optimization Questions

### "How would you handle a small files problem in Delta Lake?"

| Aspect | Your Current Knowledge | Gap to Fill |
|--------|----------------------|-------------|
| **Can say** | "I know small files slow down queries because of file-open overhead and metadata scanning." | Learn the Delta-specific solutions. |
| **LEARN** | (1) `OPTIMIZE` compacts small files into ~1GB files. (2) Auto-compaction: `delta.autoOptimize.autoCompact = true` — runs OPTIMIZE automatically after writes. (3) `OPTIMIZE ZORDER BY (col)` — compacts AND co-locates data. (4) Streaming writes: `trigger(processingTime='10 minutes')` batches micro-batches to reduce file count. |
| **Ideal answer** | "Small files are common with append-heavy workloads like my daily pipeline (each run appends a new set of files). My strategy: (1) Enable auto-compaction on the Delta table: `ALTER TABLE t SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')`. (2) Run scheduled `OPTIMIZE` with Z-ordering weekly: `OPTIMIZE gold.daily_consumption ZORDER BY (vendor, date)`. (3) For streaming ingestion, use `trigger(processingTime='10 minutes')` to batch files. (4) Target file size: `delta.targetFileSize = 134217728` (128MB). Combined, this keeps file count manageable and queries fast." |

## 4.4 Gap Summary — What to Prioritize

| Topic | Current Level | Target Level | Action Required |
|-------|--------------|-------------|-----------------|
| Delta Lake MERGE | Never used | Must explain with examples | Practice writing MERGE statements. Implement one for your daily aggregation upsert. |
| Time Travel | Concept only | Syntax + use cases | Run `SELECT * FROM t VERSION AS OF 5` on a test table. Know retention policies. |
| Z-Ordering | Never used | Explain + know when to use | Run `OPTIMIZE t ZORDER BY (vendor, date)` on a test table. Measure query speed difference. |
| Auto Loader | Never used (built own equivalent) | Explain how it replaces your pattern | Convert your `data_fetch_pyspark.py` to use `cloudFiles` format. |
| Medallion Architecture | Implicit in your code | Name and explain explicitly | Map your pipeline stages to Bronze/Silver/Gold. Practice drawing it on a whiteboard. |
| Unity Catalog | Minimal | Explain namespace + permissions | Set up a dev catalog with schemas. Grant permissions to a test user. |
| Databricks Workflows | Basic | Multi-task with dependencies | Create a workflow: task1 (bronze) → task2 (silver) → task3a (gold_block) + task3b (gold_daily) |
| Photon | Heard of it | Explain when/why to enable | Run same job with and without Photon. Compare Spark UI metrics. |
| Cost Optimization | Minimal | Spot instances, sizing, pools | Learn DBU pricing model. Practice right-sizing a cluster for your workload. |
| Structured Streaming | Concept only | Write a basic streaming pipeline | Convert one batch pipeline to `readStream` + `writeStream` with `trigger(availableNow=True)` |

---

# Section 5 — Real-World Scenarios

## 5.1 Cluster Tuning

### Scenario: "You need to process 100GB of smart meter data daily within 45 minutes. Design the cluster."

**Analysis:**

```
Data: 100GB Parquet (4M meters, 96 readings/day, 5 registers)
Operations: Read → Pre-aggregate → Pivot → Join → Write to Delta
Bottlenecks: Shuffle (groupBy, pivot), I/O (read 100GB, write ~10GB result)
```

**Cluster Design:**

| Parameter | Value | Reasoning |
|-----------|-------|-----------|
| Driver | 1x Standard_D8s_v3 (8 cores, 32GB) | Needs memory for broadcast variables and collecting small results |
| Workers | 4-8x Standard_D8s_v3 (auto-scaling) | 32-64 cores total, 128-256GB total memory |
| Instance type | Storage-optimized | Heavy Parquet I/O benefits from fast local SSDs |
| Spot instances | 70% spot, 30% on-demand | Batch job can tolerate retries from spot eviction |
| Photon | Enabled | Aggregation-heavy workload benefits significantly |
| Auto-scaling | Min 4, Max 8 | Scales up during heavy shuffle stages, scales down during I/O |

**Spark Configuration:**
```
spark.sql.shuffle.partitions = 500       # 100GB / 200MB target per partition
spark.sql.adaptive.enabled = true        # AQE for runtime optimization
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.files.maxPartitionBytes = 128MB
spark.databricks.delta.optimizeWrite.enabled = true
```

**Cost Estimate (Azure):**
- Standard_D8s_v3 on-demand: ~$0.40/hr
- Spot: ~$0.12/hr
- 6 workers avg × 0.75 hrs × $0.20/hr (blended) + 1 driver × 0.75 × $0.40 = ~$1.20 per run
- Monthly (30 runs): ~$36 compute + DBU costs

## 5.2 Job Failures

### Scenario: "Your nightly Databricks job failed at 3 AM. The SLA dashboard shows stale data. Walk through your troubleshooting."

**Step-by-step:**

1. **Check Databricks Job Runs page:**
   - View run status, error message, and which task failed
   - Check if it's a recurring failure or one-time

2. **Common failure modes and fixes:**

| Failure | Symptom | Fix |
|---------|---------|-----|
| **Driver OOM** | `java.lang.OutOfMemoryError` on driver | Increase driver memory. Check for `collect()`, `toPandas()`, large broadcasts. In my pipeline: the `results_dict = final_df.rdd.map(lambda row: row.asDict(recursive=True)).collect()` pattern pulls everything to the driver — replace with direct Delta write. |
| **Executor OOM** | Tasks failing with `Container killed by YARN` or `ExecutorLostFailure` | Increase executor memory or `spark.executor.memoryOverhead`. Check for skewed partitions. My `applyInPandas` functions consume off-heap memory for numpy/pandas — need adequate overhead. |
| **Spot instance preemption** | `SpotInstanceEvicted` | Enable spot fall-back to on-demand. Or increase on-demand ratio for critical jobs. Add retry policy (Databricks Workflows support automatic retries). |
| **File not found** | `FileNotFoundException` on cloud storage | Vendor data delayed or path changed. Add `spark.sql.files.ignoreMissingFiles = true`. Add alerting for missing input data. |
| **Schema mismatch** | `AnalysisException: cannot resolve column` | New vendor or changed schema. Use `mergeSchema` for Delta reads, or add schema validation in Bronze layer. My `match_columns()` function handles this. |
| **Cluster startup timeout** | Job waits >30 min for cluster | Cloud provider capacity issue. Use cluster pools (pre-warmed instances). Or switch instance type/region. |

3. **Recovery:**
   - If idempotent (Delta MERGE): just re-run
   - If append-only: check if partial data was written. With Delta, partial writes are atomic — either all or nothing
   - Re-run the failed task only (Databricks Workflows support task-level retry)

4. **Prevention:**
   - Add retry policy: 2 retries with 5-minute delay
   - Add email/Slack alerts on failure
   - Add data quality checks before processing (fail fast)
   - Monitor cluster metrics: set alerts on high memory usage

## 5.3 Cost Optimization

### Scenario: "Management says your Databricks bill is too high. How do you reduce costs by 40%?"

**Cost reduction strategies:**

| Strategy | Savings | Implementation |
|----------|---------|---------------|
| **Job clusters instead of all-purpose** | 30-50% | Production jobs should auto-create and auto-terminate clusters. No idle time charges. |
| **Spot instances** | 60-90% per instance | Use 70% spot / 30% on-demand for batch jobs. Spark retries handle spot evictions. |
| **Auto-scaling** | 20-40% | Scale down during I/O-bound stages. Set appropriate min/max workers. |
| **Right-sizing** | 15-30% | Profile actual CPU/memory usage. Most jobs over-provision. If peak memory is 60GB, don't allocate 256GB. |
| **Auto-termination** | Variable | Set auto-terminate after 30 min idle for all-purpose clusters. Developers forget to stop clusters. |
| **Cluster pools** | 5-10% | Pre-warm instances to avoid paying for cluster startup time on every run. |
| **Photon** | Indirect | Faster jobs = less compute time = lower cost, even though Photon DBU rate is higher. Net savings for aggregation-heavy jobs. |
| **Delta OPTIMIZE** | 10-20% I/O cost | Compacted files read faster → shorter job runtime. |
| **Partition pruning** | Significant | Z-order and partition by frequently filtered columns. Queries read only relevant data. |

**For your specific pipelines:**
- Block aggregation (runs 10+ times/day for different vendors): use job clusters + spot instances
- SLA monitoring (runs daily): auto-scaling 2-6 workers, spot instances
- Disaggregation (GPU-intensive): check if Photon or GPU instances are more cost-effective

## 5.4 Delta Lake Migration

### Scenario: "Convert your existing Parquet pipeline to Delta Lake."

**Your current pipeline (Parquet on HDFS):**
```python
# Read
df = spark.read.parquet(f"hdfs://.../eid={eid}/year={year}/month={month}/day={day}")

# Write results
result_df.write.mode("overwrite").parquet(f"{parent_folder}/{formatted_date}/{filename}")

# Write tracking
last_read_path_df.write.mode("overwrite").parquet(last_read_path)
```

**Migrated pipeline (Delta on cloud storage):**
```python
# Read from Delta (Bronze table)
df = spark.read.format("delta").load("abfss://container@account/bronze/meter_readings")
# Or: df = spark.table("prod.smart_meter.bronze_readings")

# Write results (Gold table) — MERGE instead of overwrite
from delta.tables import DeltaTable
gold_table = DeltaTable.forPath(spark, "abfss://container@account/gold/daily_aggregation")

gold_table.alias("target").merge(
    result_df.alias("source"),
    "target.IMEI = source.IMEI AND target.date = source.date"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# Auto Loader replaces last-read-path tracking entirely
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "/checkpoints/schema") \
    .load("abfss://container@account/landing/") \
    .writeStream \
    .option("checkpointLocation", "/checkpoints/bronze") \
    .trigger(availableNow=True) \
    .toTable("prod.smart_meter.bronze_readings")
```

**Key changes:**
1. `spark.read.parquet()` → `spark.read.format("delta").load()` or `spark.table()`
2. `write.mode("overwrite").parquet()` → `DeltaTable.merge()` for upserts
3. Custom last-read-path tracking → Auto Loader with checkpoints
4. Manual dedup (`left_anti` join) → Delta MERGE handles it atomically
5. Add `OPTIMIZE` and `VACUUM` maintenance jobs

## 5.5 Pipeline Orchestration

### Scenario: "Design a Databricks Workflow for your entire smart meter pipeline."

**Workflow design:**

```
┌──────────────────────────────────────────────────────────────────┐
│                  DAP Daily Workflow                               │
│  Trigger: Daily at 3:00 AM                                       │
│  Cluster: Job cluster, 4-8 workers, auto-scaling, spot           │
│                                                                   │
│  Task 1: Bronze Ingestion (Auto Loader)                          │
│     ├── Reads new files from landing zone                        │
│     └── Writes to bronze.meter_readings                          │
│              │                                                    │
│  Task 2: Silver Processing (depends on Task 1)                   │
│     ├── Schema normalization                                     │
│     ├── Data quality checks                                      │
│     └── MERGE into silver.meter_readings                         │
│              │                                                    │
│     ┌───────┴───────┬──────────────┐                             │
│     ▼               ▼              ▼                             │
│  Task 3a:       Task 3b:      Task 3c:                           │
│  Block Agg      Daily Agg     SLA Monitoring                     │
│  (parallel)     (parallel)    (parallel)                         │
│     │               │              │                             │
│     └───────┬───────┘              │                             │
│             ▼                      ▼                             │
│  Task 4: OPTIMIZE + VACUUM    Task 4b: Blob Reports              │
│              │                      │                            │
│              └──────────┬───────────┘                            │
│                         ▼                                        │
│               Task 5: Validation                                 │
│               (row counts, quality checks)                       │
└──────────────────────────────────────────────────────────────────┘
```

**Databricks Workflow configuration:**
```json
{
  "name": "DAP_Daily_Pipeline",
  "schedule": { "cron": "0 0 3 * * ?" },
  "tasks": [
    {
      "task_key": "bronze_ingestion",
      "notebook_task": { "notebook_path": "/Repos/dap/notebooks/bronze_ingestion" },
      "new_cluster": { "num_workers": 4, "spark_version": "14.3.x-scala2.12", "node_type_id": "Standard_D8s_v3" }
    },
    {
      "task_key": "silver_processing",
      "depends_on": [{"task_key": "bronze_ingestion"}],
      "notebook_task": { "notebook_path": "/Repos/dap/notebooks/silver_processing" }
    },
    {
      "task_key": "gold_block_aggregation",
      "depends_on": [{"task_key": "silver_processing"}],
      "notebook_task": { "notebook_path": "/Repos/dap/notebooks/block_aggregation" }
    },
    {
      "task_key": "gold_daily_aggregation",
      "depends_on": [{"task_key": "silver_processing"}],
      "notebook_task": { "notebook_path": "/Repos/dap/notebooks/daily_aggregation" }
    },
    {
      "task_key": "sla_monitoring",
      "depends_on": [{"task_key": "silver_processing"}],
      "notebook_task": { "notebook_path": "/Repos/dap/notebooks/sla_monitoring" }
    }
  ],
  "email_notifications": { "on_failure": ["team@company.com"] },
  "max_retries": 2,
  "retry_on_timeout": true
}
```

**Compared to your current approach (shell scripts):**
- Shell scripts: linear execution, manual error handling, no dependency management, no UI
- Databricks Workflows: parallel task execution, automatic retries, dependency DAG, monitoring UI, email alerts

---

# Section 6 — Structured Learning Roadmap

## 6.1 Overview

| Week | Focus Area | Priority | Goal |
|------|-----------|----------|------|
| 1 | Delta Lake Core | CRITICAL | Explain ACID, transaction log, MERGE, time travel |
| 2 | Delta Lake Optimization | CRITICAL | OPTIMIZE, VACUUM, Z-ordering, auto-compaction |
| 3 | Auto Loader + Medallion Architecture | HIGH | Replace your ingestion pattern, explain Bronze/Silver/Gold |
| 4 | Databricks Workflows + Unity Catalog | HIGH | Orchestration and governance |
| 5 | Structured Streaming + Advanced Topics | MEDIUM | Micro-batch, DLT, Photon |
| 6 | Mock Interviews + Practice | CRITICAL | Synthesize everything into confident answers |

## 6.2 Week 1: Delta Lake Core

### Goals
- Explain Delta Lake architecture (transaction log, Parquet + metadata)
- Write MERGE statements confidently
- Use time travel (version + timestamp)
- Explain ACID guarantees with real examples

### Resources

| Resource | Type | Time | What to Focus On |
|----------|------|------|-----------------|
| **[Databricks Academy — Delta Lake Fundamentals](https://www.databricks.com/learn/training/lakehouse-fundamentals)** | Free course | 3 hours | Architecture, transaction log, basic operations |
| **[Advancing Analytics — Delta Lake Deep Dive](https://youtube.com/@AdvancingAnalytics)** | YouTube | 4 videos, ~2 hrs | "What is Delta Lake", "Delta Lake Transaction Log", "MERGE", "Time Travel" |
| **[Databricks Documentation — Delta Lake Guide](https://docs.databricks.com/en/delta/index.html)** | Docs | Reference | Read the "Quick Start", "MERGE", "Time Travel" sections |
| **[Mr. K Talks Tech — Delta Lake Interview Questions](https://youtube.com/@mr.k_talks_tech)** | YouTube | 1 hr | Practice common interview questions |

### Hands-On Practice (Do These)

1. **Create a Delta table from your existing parquet data:**
   ```python
   spark.read.parquet("/existing/data") \
       .write.format("delta") \
       .save("/delta/meter_readings")
   ```

2. **Practice MERGE (upsert) — rewrite your dedup logic:**
   ```python
   from delta.tables import DeltaTable
   target = DeltaTable.forPath(spark, "/delta/daily_agg")
   target.alias("t").merge(
       new_data.alias("s"),
       "t.IMEI = s.IMEI AND t.date = s.date"
   ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
   ```

3. **Time travel — query yesterday's version:**
   ```python
   spark.read.format("delta").option("versionAsOf", 0).load("/delta/daily_agg")
   spark.sql("DESCRIBE HISTORY delta.`/delta/daily_agg`")
   ```

## 6.3 Week 2: Delta Lake Optimization

### Goals
- Run OPTIMIZE and measure improvement
- Implement Z-ordering on your SLA tables
- Understand and configure VACUUM retention
- Enable auto-optimizations

### Resources

| Resource | Type | Time | What to Focus On |
|----------|------|------|-----------------|
| **[Databricks Blog — Delta Lake Performance Guide](https://www.databricks.com/blog/2023/02/01/best-practices-delta-lake)** | Article | 30 min | OPTIMIZE, Z-order, file sizing, auto-compact |
| **[Data Savvy — Delta Lake Optimization](https://youtube.com/@datasavvy)** | YouTube | 2 videos | "OPTIMIZE and VACUUM", "Z-Ordering Explained" |
| **[Databricks Docs — Optimization](https://docs.databricks.com/en/delta/optimize.html)** | Docs | Reference | OPTIMIZE, Z-ORDER, liquid clustering (new) |

### Hands-On Practice

1. **OPTIMIZE a table and measure:**
   ```sql
   -- Check current file count
   DESCRIBE DETAIL delta.`/delta/daily_agg`
   -- Optimize
   OPTIMIZE delta.`/delta/daily_agg` ZORDER BY (vendor, date)
   -- Check file count again — should be dramatically fewer
   ```

2. **Set up auto-optimization:**
   ```sql
   ALTER TABLE daily_agg SET TBLPROPERTIES (
       'delta.autoOptimize.optimizeWrite' = 'true',
       'delta.autoOptimize.autoCompact' = 'true'
   );
   ```

3. **VACUUM old files:**
   ```sql
   VACUUM delta.`/delta/daily_agg` RETAIN 168 HOURS -- 7 days
   ```

## 6.4 Week 3: Auto Loader + Medallion Architecture

### Goals
- Convert your last-read-path ingestion to Auto Loader
- Design and explain Bronze/Silver/Gold for your meter pipeline
- Understand checkpointing and schema evolution

### Resources

| Resource | Type | Time | What to Focus On |
|----------|------|------|-----------------|
| **[Databricks Academy — Data Engineering with Databricks](https://www.databricks.com/learn/training/data-engineering-with-databricks)** | Course (free) | 8 hours | Medallion architecture, Auto Loader, workflows |
| **[Databricks Docs — Auto Loader](https://docs.databricks.com/en/ingestion/auto-loader/index.html)** | Docs | Reference | Configuration options, file notification vs directory listing, schema evolution |
| **[Advancing Analytics — Medallion Architecture](https://youtube.com/@AdvancingAnalytics)** | YouTube | 2 videos | "Bronze Silver Gold", "Building a Lakehouse Pipeline" |

### Hands-On Practice

1. **Auto Loader ingestion:**
   ```python
   df = spark.readStream.format("cloudFiles") \
       .option("cloudFiles.format", "parquet") \
       .option("cloudFiles.schemaLocation", "/checkpoints/schema") \
       .load("/landing/meter_data/") \
       .writeStream \
       .format("delta") \
       .option("checkpointLocation", "/checkpoints/bronze") \
       .trigger(availableNow=True) \
       .toTable("bronze.meter_readings")
   ```

2. **Build a simple Bronze → Silver → Gold pipeline** with 3 notebooks and a Workflow.

## 6.5 Week 4: Workflows + Unity Catalog

### Goals
- Create a multi-task Workflow with dependencies
- Set up Unity Catalog namespace (catalog.schema.table)
- Implement permissions and understand lineage

### Resources

| Resource | Type | Time | What to Focus On |
|----------|------|------|-----------------|
| **[Databricks Docs — Workflows](https://docs.databricks.com/en/workflows/index.html)** | Docs | Reference | Job creation, task dependencies, parameters, retries |
| **[Databricks Docs — Unity Catalog](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)** | Docs | Reference | Namespace, permissions, managed vs external tables |
| **[Mr. K Talks Tech — Databricks Workflows](https://youtube.com/@mr.k_talks_tech)** | YouTube | 1 hr | Practical workflow creation |

### Hands-On Practice

1. Create a 3-task workflow: bronze → silver → gold
2. Set up a catalog with dev and prod schemas
3. Grant SELECT on gold tables to a "analyst" group

## 6.6 Week 5: Advanced Topics

### Goals
- Understand Structured Streaming with `trigger(availableNow=True)`
- Know when to use Delta Live Tables
- Explain Photon engine benefits

### Resources

| Resource | Type | Time | What to Focus On |
|----------|------|------|-----------------|
| **[Databricks Academy — Structured Streaming](https://www.databricks.com/learn/training)** | Course | 4 hours | Micro-batch, checkpointing, output modes |
| **[Databricks Blog — Photon](https://www.databricks.com/product/photon)** | Article | 20 min | What Photon accelerates, when to enable |
| **[Databricks Docs — Delta Live Tables](https://docs.databricks.com/en/delta-live-tables/index.html)** | Docs | Reference | Declarative pipelines, expectations, monitoring |

## 6.7 Week 6: Mock Interview Practice

### Daily Practice Routine (1-2 hours/day)

**Day 1-2: Delta Lake Questions**
- Explain Delta Lake to a non-technical person (2 min)
- Write MERGE from memory (no looking)
- Explain transaction log and ACID (5 min)
- Draw medallion architecture on paper

**Day 3-4: Pipeline Design**
- Design your meter pipeline using Databricks (whiteboard style)
- Explain Auto Loader vs your last-read-path pattern
- Design a Workflow with error handling

**Day 5-6: Optimization**
- Explain how you optimized your Spark jobs (real examples)
- Z-ordering: when to use, how it works
- Cluster sizing: design a cluster for 100GB data, 45-min SLA

**Day 7: Full Mock Interview**
- Have a friend or use a mirror. 45-minute mock covering:
  - "Tell me about your project" (5 min)
  - "How does Delta Lake work?" (5 min)
  - "Design a pipeline for X" (15 min)
  - "How would you optimize Y?" (10 min)
  - "What is medallion architecture?" (5 min)
  - Q&A (5 min)

## 6.8 Quick Reference Card (Print This)

### Delta Lake Commands Cheat Sheet
```sql
-- Create Delta table
CREATE TABLE t USING DELTA AS SELECT * FROM parquet.`/path`

-- MERGE (upsert)
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

-- Time travel
SELECT * FROM t VERSION AS OF 5
SELECT * FROM t TIMESTAMP AS OF '2025-01-15'
DESCRIBE HISTORY t

-- Maintenance
OPTIMIZE t ZORDER BY (col1, col2)
VACUUM t RETAIN 168 HOURS

-- Properties
ALTER TABLE t SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
```

### Auto Loader Template
```python
spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "/schema") \
    .load("/landing/") \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoint") \
    .trigger(availableNow=True) \
    .toTable("catalog.schema.table")
```

### Databricks Certification Path
If you want formal certification:
1. **Databricks Certified Data Engineer Associate** (you already have this)
2. **Databricks Certified Data Engineer Professional** — next goal, covers: Delta Lake advanced, DLT, Unity Catalog, production pipeline design

---

**END OF DOCUMENT 2**

> Both documents are designed to be used together:
> - Document 1 (`Interview_Prep_Guide.md`): covers your CORE technical skills mapped to real projects
> - Document 2 (`Databricks_Interview_Guide.md`): covers the Databricks-specific layer you need to add on top
>
> Start with Document 1 (Sections 1-6 for core skills, Section 7 for vocabulary).
> Then move to Document 2 (Week 1-2 Delta Lake, then follow the roadmap).
> Use the Terminology Mapping Table (Section 7 of Doc 1) throughout your preparation.