# Data Engineer Interview Guide -- Concepts, Questions & System Design

> A comprehensive, conceptual interview preparation guide for Data Engineers with 2+ years of experience.
> Covers: Python, Spark, PySpark, ETL, HDFS, Data Modeling, Databricks, UDFs, SQL, System Design.
> Not tied to any specific project -- purely conceptual and interview-focused.

---

## Table of Contents

### Part 1 -- Core Concepts
1. [Python for Data Engineering](#11-python-for-data-engineering)
2. [Apache Spark Architecture](#12-apache-spark-architecture)
3. [PySpark Internals](#13-pyspark-internals)
4. [Spark Performance Tuning](#14-spark-performance-tuning)
5. [ETL Pipeline Design](#15-etl-pipeline-design)
6. [HDFS and Distributed Storage](#16-hdfs-and-distributed-storage)
7. [Data Modeling for Large-Scale Systems](#17-data-modeling-for-large-scale-systems)
8. [Databricks](#18-databricks)
9. [Pandas UDFs and applyInPandas](#19-pandas-udfs-and-applyinpandas)
10. [Schema Variability](#110-schema-variability)
11. [ML Integration in Data Pipelines](#111-ml-integration-in-data-pipelines)

### Part 2 -- Scenario-Based Interview Questions
12. [Spark Architecture & Internals](#21-spark-architecture--internals)
13. [Performance Optimization](#22-performance-optimization)
14. [ETL & Pipeline Design](#23-etl--pipeline-design)
15. [Data Modeling & Storage](#24-data-modeling--storage)
16. [Databricks-Specific](#25-databricks-specific)
17. [Debugging & Troubleshooting](#26-debugging--troubleshooting)

### Part 3 -- System Design
18. [System Design Framework](#31-system-design-framework)
19. [Design 1: Large-Scale IoT Data Pipeline](#32-design-1-large-scale-iot-data-pipeline)
20. [Design 2: KPI Aggregation Pipeline](#33-design-2-kpi-aggregation-pipeline)
21. [Design 3: SLA / Data Quality Monitoring](#34-design-3-sla--data-quality-monitoring)
22. [Design 4: Lambda / Kappa Architecture](#35-design-4-lambda--kappa-architecture)

---

# PART 1 -- CORE CONCEPTS

---

## 1.1 Python for Data Engineering

### Memory Model

Python objects carry significant overhead. An `int` in C is 4 bytes; in Python it is **28 bytes** (object header, reference count, type pointer, value). A list of 1 million integers uses ~28 MB in Python vs ~4 MB in a C array.

**Why this matters**: When you process millions of rows in Python, memory bloats. This is one reason Pandas hits limits around 5-10 GB of data on a single machine.

### The GIL (Global Interpreter Lock)

Python's GIL allows only one thread to execute Python bytecode at a time. This means:
- **Multithreading** in Python does NOT give true parallelism for CPU-bound tasks.
- **Multiprocessing** (separate processes) bypasses the GIL but has inter-process communication overhead.
- **Spark's approach**: Each executor runs a separate Python process per task, sidestepping the GIL entirely.

**Interview tip**: "Python's GIL limits single-process parallelism. For CPU-bound data work, we either use multiprocessing, NumPy/Pandas (which release the GIL for C-level operations), or distribute with Spark."

### Generators vs Lists

```python
# List -- loads everything into memory
data = [row for row in read_file("big.csv")]  # 10 GB in memory

# Generator -- streams one item at a time
data = (row for row in read_file("big.csv"))  # almost zero memory
```

Generators are essential for large-file processing, streaming ETL, and memory-efficient data pipelines in pure Python.

### Vectorization

NumPy and Pandas operations run in **compiled C code**, not Python loops:

```python
# Slow (Python loop): ~10 seconds for 10M rows
result = [x * 2 + 1 for x in data]

# Fast (vectorized): ~0.01 seconds for 10M rows
result = data * 2 + 1  # NumPy/Pandas vectorized operation
```

**Interview answer**: "I always prefer vectorized Pandas operations over iterating with `.iterrows()` or `apply()`. When even vectorized Pandas is too slow (data > memory), I switch to PySpark."

### When Python Breaks Down

| Symptom | Cause | Solution |
|---------|-------|----------|
| `MemoryError` or machine swaps | Data exceeds RAM | Move to Spark, use chunked reading, use Dask |
| Single-core bottleneck | GIL, no parallelism | Use multiprocessing, or distribute with Spark |
| Processing takes hours | Sequential row-by-row logic | Vectorize with Pandas/NumPy, or parallelize with Spark |
| Need fault tolerance | No retry, no checkpointing | Use Spark (lineage-based recovery) or workflow orchestrators |

### Serialization

- **Pickle**: Python's default. Slow, Python-only, not cross-language.
- **Apache Arrow**: Columnar in-memory format. Fast, cross-language (Python ↔ JVM). Used by PySpark for `pandas_udf` data transfer.
- **JSON**: Human-readable but slow and verbose. Use for configs, not data.
- **Parquet**: Columnar on-disk format. Best for analytical workloads.

---

## 1.2 Apache Spark Architecture

### What Spark Is

Spark is a **distributed compute engine** that splits data into partitions and processes them in parallel across a cluster. It keeps intermediate data in memory (unlike MapReduce which writes to disk between steps).

### Components

```
spark-submit your_script.py
        |
        v
+-------------------+          +---------------------+
|      Driver       |--------->|   Cluster Manager   |
| (plans the work,  |<---------|   (YARN / K8s)      |
|  runs your main())|          +---------------------+
+-------------------+                  |
        |                         launches
        | sends tasks                  |
        v                              v
+---------------+    +---------------+    +---------------+
|  Executor 1   |    |  Executor 2   |    |  Executor 3   |
|  [Task][Task] |    |  [Task][Task] |    |  [Task][Task] |
|  [Cache]      |    |  [Cache]      |    |  [Cache]      |
+---------------+    +---------------+    +---------------+
```

**Driver**: Runs your `main()`, creates SparkSession, builds the execution plan (DAG), schedules tasks, collects results. It is the brain. If it dies, the entire job fails.

**Cluster Manager** (YARN, Kubernetes, Standalone, Mesos): Allocates resources (containers/pods) for driver and executors. Does not run Spark code.

**Executors**: JVM processes on worker nodes. Run tasks, cache data, report status. Each executor has N task slots (= `--executor-cores`).

### DAG (Directed Acyclic Graph)

When you write transformations, Spark builds a DAG -- a graph of operations with no cycles. It does NOT execute them until you call an **action**.

```python
df = spark.read.parquet("data")       # recorded in DAG
filtered = df.filter(col("x") > 10)   # recorded in DAG
grouped = filtered.groupBy("y").count() # recorded in DAG
grouped.show()                          # ACTION -- triggers execution of the entire DAG
```

### Lazy Evaluation

Spark records transformations as a plan. Only actions trigger execution. This allows Spark to:
1. **Optimize the entire plan** end-to-end (Catalyst optimizer).
2. **Skip unnecessary work** (e.g., if you select 2 columns, Spark reads only those 2 from Parquet).
3. **Pipeline narrow operations** (filter + select + withColumn run in a single pass, no intermediate materialization).

### Transformations vs Actions

**Transformations** (lazy, return a new DataFrame):
`filter`, `select`, `withColumn`, `groupBy`, `join`, `orderBy`, `distinct`, `repartition`, `coalesce`, `pivot`, `explode`, `union`

**Actions** (trigger execution):
`show()`, `count()`, `collect()`, `take()`, `first()`, `write.*`, `toPandas()`, `foreach()`

### Stages, Tasks, and Shuffles

- A **job** is triggered by one action.
- A job is split into **stages** at **shuffle boundaries** (wide transformations like `groupBy`, `join`).
- Each stage runs **tasks** in parallel -- one task per partition.
- **Narrow transformations** (filter, select, map): no data movement, processed within each partition.
- **Wide transformations** (groupBy, join, orderBy): require **shuffle** -- data redistribution across the network. Expensive.

### Fault Tolerance

If an executor dies and a partition is lost, Spark recomputes it from the DAG lineage. No data replication needed -- just replay the recipe.

---

## 1.3 PySpark Internals

### How PySpark Talks to the JVM

PySpark uses **Py4J** -- a bridge that lets Python call Java/Scala methods on the JVM. When you write `df.filter(col("x") > 10)`, Python sends this as a JVM method call. The actual data processing happens in the JVM, not in Python.

**Key implication**: DataFrame operations (filter, select, join, groupBy) run at JVM speed. Python UDFs break this -- they serialize data from JVM to Python and back, which is slow.

### Execution Plan

Every DataFrame query goes through:

```
Your Code (Python)
    |
    v
Unresolved Logical Plan  (raw operations from your code)
    |  [Analysis -- resolve column names, types]
    v
Resolved Logical Plan
    |  [Optimization -- predicate pushdown, column pruning, join reorder]
    v
Optimized Logical Plan
    |  [Physical Planning -- choose join strategy, scan method]
    v
Physical Plan
    |  [Code Generation -- compile to JVM bytecode]
    v
Executed on Cluster
```

Use `df.explain(True)` to see all four plans.

### Catalyst Optimizer

Catalyst is Spark SQL's rule-based and cost-based optimizer:

1. **Analysis**: Resolves column names, types, table references. Throws `AnalysisException` if a column doesn't exist.
2. **Logical Optimization**:
   - **Predicate pushdown**: Move filters as close to the data source as possible. `Scan -> Join -> Filter(x>10)` becomes `Scan(filter: x>10) -> Join`.
   - **Column pruning**: If you `select("a", "b")`, Spark only reads columns a and b from Parquet.
   - **Constant folding**: `col("x") > 20 + 5` becomes `col("x") > 25` at plan time.
   - **Join reordering**: With CBO enabled, picks the join order that minimizes data shuffled.
3. **Physical Planning**: Chooses algorithms -- Broadcast Hash Join vs Sort Merge Join, Hash Aggregation vs Sort Aggregation, vectorized Parquet reader.
4. **Code Generation (Tungsten)**: Compiles the plan into optimized JVM bytecode. Eliminates virtual function calls, uses primitive types, CPU-cache-friendly.

### Tungsten Engine

- Stores DataFrame data in **compact binary format** (not JVM objects), reducing memory 3-5x and eliminating GC overhead.
- **Whole-Stage Code Generation**: Fuses operators into a single tight loop instead of calling separate functions per operator per row.
- This is why DataFrames are much faster than raw Python or even RDD operations.

---

## 1.4 Spark Performance Tuning

### Partitioning

- Every DataFrame is split into **partitions**. Each partition = one task = one CPU core.
- **Too few partitions**: Cores sit idle, tasks process too much data and may OOM.
- **Too many partitions**: Scheduling overhead, too many small files on output.
- **Target**: 100-200 MB per partition, 2-4 partitions per core.

```python
df.rdd.getNumPartitions()   # Check
df.repartition(200)         # Increase (shuffle)
df.coalesce(50)             # Decrease (no shuffle)
df.repartition("key_col")   # Partition by column (useful before joins)
```

### Shuffle Partitions

After `groupBy`, `join`, `distinct`, data is reshuffled into `spark.sql.shuffle.partitions` partitions (default: 200).

- For 10 GB data: 200 partitions = 50 MB each. Fine.
- For 1 TB data: 200 partitions = 5 GB each. Tasks will OOM. Set to 2000-5000.
- **Best approach**: Enable AQE and let Spark auto-coalesce.

### Broadcast Joins

When one side of a join is small (< 10 MB by default), Spark broadcasts it to all executors. No shuffle needed.

```python
from pyspark.sql.functions import broadcast
result = big_df.join(broadcast(small_df), "key")
```

**When to force broadcast**: If you know a table is small but Spark's statistics are stale. Increase threshold with `spark.sql.autoBroadcastJoinThreshold`.

### Sort Merge Join

Default for large-large joins. Shuffles both sides by join key, sorts each partition, then merges. Expensive but works for any data size.

### Data Skew

**What**: One partition has much more data than others (e.g., one city has 90% of rows).

**Detection**: Spark UI shows one task 10-100x slower than others.

**Solutions**:
1. **Salting**: Add random suffix to key, aggregate in two passes.
2. **AQE Skew Join**: `spark.sql.adaptive.skewJoin.enabled=true` -- Spark splits skewed partitions automatically.
3. **Broadcast**: If one side is small, broadcast eliminates the shuffle entirely.
4. **Isolate**: Process the skewed key separately with broadcast, combine with `unionByName`.

### Caching / Persistence

If you reuse a DataFrame multiple times, persist it:

```python
from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK)
df.count()   # materializes cache
# ... use df multiple times ...
df.unpersist()  # free memory
```

**Storage levels**: `MEMORY_ONLY` (fast, drops if full), `MEMORY_AND_DISK` (spills to disk), `DISK_ONLY` (slowest).

**When NOT to cache**: If the DataFrame is used only once. Caching has overhead (serialization, memory pressure).

### Adaptive Query Execution (AQE)

Spark 3.0+ feature. Re-optimizes at runtime based on actual data statistics:
- **Coalesces small partitions** after shuffle.
- **Switches join strategy** at runtime (e.g., Sort Merge → Broadcast if one side is small after filtering).
- **Handles skew** by splitting large partitions.

Always enable: `spark.sql.adaptive.enabled=true`.

### Executor and Driver Sizing

| Parameter | Guideline |
|-----------|-----------|
| `--executor-cores` | 3-5 cores per executor |
| `--executor-memory` | 4-20 GB per executor |
| `--executor-memoryOverhead` | 10-20% of executor memory (for Python workers) |
| `--driver-memory` | 2-8 GB (increase if collecting data to driver) |
| `--num-executors` | Or use dynamic allocation |

**Rule**: Don't make executors too large (GC pauses, one OOM kills all tasks). Don't make them too small (overhead per executor).

---

## 1.5 ETL Pipeline Design

### Batch vs Streaming vs Micro-Batch

| Model | Latency | Complexity | Use Case |
|-------|---------|-----------|----------|
| **Batch** | Minutes to hours | Low | Daily/monthly aggregations, historical reprocessing |
| **Micro-batch** | Seconds to minutes | Medium | Near-real-time dashboards, Spark Structured Streaming |
| **Streaming** | Milliseconds | High | Fraud detection, live alerts, event-driven |

Most data engineering pipelines are **batch** or **micro-batch**. True streaming (Kafka Streams, Flink) is used for sub-second latency.

### Incremental Pipelines

Instead of reprocessing all data every run, process only what's new:

**Pattern 1 -- Watermark / High-Water Mark**:
```
1. Read the last processed timestamp from a state store.
2. Query source for records where timestamp > last_processed.
3. Process new records.
4. Update the state store with the new max timestamp.
```

**Pattern 2 -- Change Data Capture (CDC)**:
Track inserts, updates, deletes from the source. Tools: Debezium, Kafka Connect, Delta Lake `MERGE`.

**Pattern 3 -- File-based incremental**:
Track which files have been processed (e.g., by comparing HDFS file lists against a last-read pointer). Process only new files.

### Idempotency

A pipeline is **idempotent** if running it multiple times with the same input produces the same output. Critical for retries and reprocessing.

**How to achieve**:
- Use `overwrite` mode for partition-level writes: `df.write.mode("overwrite").partitionBy("date").parquet(path)`.
- Use `MERGE` (upsert) for row-level deduplication.
- Avoid append-only writes to non-partitioned tables (duplicates on rerun).

### Error Handling

- **Dead Letter Queue (DLQ)**: Bad records go to a separate location instead of failing the job. Process them later.
- **Try/Except in UDFs**: Wrap UDF logic in try/except, return null or a flag for bad records.
- **Data Quality Gates**: Validate row counts, null percentages, schema before proceeding. Fail fast if quality drops below threshold.

### Schema Evolution

When source data adds new columns:
- **Parquet**: Set `mergeSchema=true` to union schemas across files.
- **Delta Lake**: `ALTER TABLE ADD COLUMNS` or enable schema evolution on write.
- **Defensive coding**: Use `unionByName(allowMissingColumns=True)`.

### Backfill Strategies

When you need to reprocess historical data:
1. **Full reprocess**: Simplest but slowest. Read all source data, write output.
2. **Partition-level reprocess**: Reprocess only specific date partitions.
3. **Incremental backfill**: Process in date-range chunks (e.g., one month at a time) to control resource usage.

---

## 1.6 HDFS and Distributed Storage

### How HDFS Works

HDFS splits files into **blocks** (default 128 MB) and distributes them across **DataNodes**. The **NameNode** stores metadata (which blocks belong to which file, their locations).

```
File: data.parquet (512 MB)
  -> Block 1 (128 MB): DataNode A, DataNode B, DataNode C  (3 replicas)
  -> Block 2 (128 MB): DataNode B, DataNode D, DataNode A
  -> Block 3 (128 MB): DataNode C, DataNode A, DataNode D
  -> Block 4 (128 MB): DataNode D, DataNode C, DataNode B
```

**Replication factor** (default 3): Each block is stored on 3 different nodes. If one node dies, data is still available.

**Rack awareness**: HDFS places replicas on different racks to survive rack failures.

### NameNode vs DataNode

| | NameNode | DataNode |
|---|---|---|
| **Role** | Stores metadata (file → block mappings) | Stores actual data blocks |
| **Count** | 1 (or 2 with HA) | Many (10s to 1000s) |
| **Failure impact** | Cluster unusable (single point of failure, use HA) | Data on that node is re-replicated from other nodes |

### Small File Problem

HDFS is optimized for large files. Each file (even 1 KB) consumes NameNode memory for metadata. 10 million small files = NameNode memory exhaustion.

**Solutions**: Compact small files into larger ones, use `coalesce`/`repartition` before writing, use Delta Lake `OPTIMIZE`, use HAR (Hadoop Archive).

### HDFS vs Cloud Object Storage

| | HDFS | S3 / ADLS / GCS |
|---|---|---|
| **Location** | On-prem cluster | Cloud |
| **Cost** | Hardware + ops | Pay per use |
| **Scaling** | Add nodes | Infinite, auto |
| **Performance** | Data locality (fast) | Network-based (slower per-request but massively parallel) |
| **Best for** | On-prem Hadoop | Cloud-native pipelines |

---

## 1.7 Data Modeling for Large-Scale Systems

### Star Schema vs Snowflake Schema

**Star Schema**: Fact table at center, dimension tables around it. Denormalized dimensions -- fewer joins, faster queries.

```
        dim_product
            |
dim_date -- fact_sales -- dim_customer
            |
        dim_store
```

**Snowflake Schema**: Dimensions are normalized (broken into sub-tables). Saves storage but requires more joins.

**Interview answer**: "For analytical workloads (data warehouses, dashboards), I prefer star schema because it minimizes joins and is optimized for read-heavy queries."

### Denormalization

In OLTP (transactions), you normalize to avoid redundancy. In OLAP (analytics), you **denormalize** to avoid expensive joins at query time.

Example: Instead of joining `orders` + `customers` + `products` every query, create a wide `fact_orders` table with customer name, product name already embedded.

### Partition Key Design

**Hive / Parquet**: Partition by columns you frequently filter on (e.g., `date`, `region`). Each partition = a directory on HDFS/S3.

```python
df.write.partitionBy("year", "month").parquet("output")
# Creates: output/year=2024/month=01/part-00000.parquet
#          output/year=2024/month=02/part-00000.parquet
```

**Cassandra**: Partition key determines which node stores the data. Choose a key with:
- High cardinality (many distinct values → even distribution).
- Matching your query pattern (Cassandra requires partition key in every query).

### Medallion Architecture (Bronze / Silver / Gold)

```
Raw Data → [Bronze] → [Silver] → [Gold]
           (as-is)    (cleaned)   (aggregated, business-ready)
```

- **Bronze**: Raw ingestion. Append-only, no transformations. Source of truth.
- **Silver**: Cleaned, deduplicated, typed. Business entities (customers, orders).
- **Gold**: Aggregated, business-specific views. Ready for dashboards and ML.

### Slowly Changing Dimensions (SCD)

When dimension data changes (e.g., customer moves to a new city):

| Type | Approach | Tradeoff |
|------|----------|----------|
| **SCD Type 1** | Overwrite the old value | Loses history |
| **SCD Type 2** | Add new row with effective dates | Preserves history, complex queries |
| **SCD Type 3** | Add `previous_value` column | Limited history (only one previous value) |

**SCD Type 2 is the most common** in data warehouses. Delta Lake's `MERGE` makes this easy.

---

## 1.8 Databricks

### Architecture

```
+-------------------------------+     +-------------------------------+
|       CONTROL PLANE           |     |         DATA PLANE            |
|     (Managed by Databricks)   |     |    (Runs in YOUR cloud)       |
| - Web UI, Notebooks           |     | - Spark clusters              |
| - Workflow scheduler           |     | - VMs running your code       |
| - Cluster manager              |     | - Access your S3/ADLS/GCS     |
| - Unity Catalog               |     |                               |
+-------------------------------+     +-------------------------------+
```

**Key insight**: Your data never leaves your cloud account. Databricks manages the orchestration layer.

### Delta Lake

An open-source storage layer that adds reliability to data lakes:

- **ACID transactions**: Safe concurrent reads and writes.
- **Time travel**: Query any previous version: `spark.read.option("versionAsOf", 5).format("delta").load(path)`.
- **Schema enforcement**: Rejects writes that don't match the table schema.
- **Schema evolution**: Add columns over time without breaking readers.
- **OPTIMIZE**: Compacts small files into larger ones.
- **VACUUM**: Removes old files no longer referenced by the transaction log.
- **Z-ordering**: Colocates related data within files for faster multi-column queries.

**Storage**: Delta = Parquet files + a `_delta_log/` transaction log (JSON files tracking every change).

### Unity Catalog

Centralized governance: one place to manage permissions, data lineage, and auditing across all workspaces.

### Auto Loader

Incrementally ingests new files as they arrive in cloud storage. Uses file notification (event-based) or directory listing. Handles schema evolution automatically.

```python
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .load("/incoming/data/")
```

### Cluster Types

| Type | Purpose | Cost |
|------|---------|------|
| **All-Purpose** | Interactive (notebooks, exploration) | Higher (stays running) |
| **Job Cluster** | Automated jobs (created per job, destroyed after) | Lower (pay only during run) |

### Photon Engine

Native C++ execution engine. Replaces Spark's JVM-based processing for supported operations. 2-8x faster for SQL/DataFrame workloads. Transparent -- no code changes needed.

---

## 1.9 Pandas UDFs and applyInPandas

### The UDF Performance Hierarchy

```
Built-in functions (F.upper, F.sum, F.when)    → Fastest (JVM-native)
SQL expressions (expr("..."))                   → Same as built-in
pandas_udf (vectorized)                         → 10-100x faster than regular UDF
applyInPandas (grouped map)                     → Good for grouped computations
Regular Python UDF (udf())                      → Slowest (row-by-row serialization)
```

### Why Regular UDFs Are Slow

For each row, Spark:
1. Serializes the row from JVM binary to Python (via socket).
2. Calls your Python function.
3. Serializes the result back to JVM.

This happens **row by row**. For 100 million rows, that's 100 million serialization round trips.

### pandas_udf (Vectorized UDF)

Processes data in **batches** (Arrow columnar format), not row by row:

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def multiply_by_two(s: pd.Series) -> pd.Series:
    return s * 2

df.withColumn("doubled", multiply_by_two(col("value")))
```

**How it works**: Spark sends a batch of rows (as an Arrow RecordBatch) to Python, your function processes the entire batch with Pandas/NumPy, the result batch goes back. Far fewer serialization round trips.

### applyInPandas (Grouped Map UDF)

Apply a Pandas function to each group independently:

```python
def normalize(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["normalized"] = (pdf["value"] - pdf["value"].mean()) / pdf["value"].std()
    return pdf

df.groupBy("device_id").applyInPandas(normalize, schema=output_schema)
```

**Use cases**: Per-group ML inference, time-series operations per device, custom statistical computations per group.

**Caution**: The entire group must fit in one executor's memory. If one group is huge, it can OOM.

### When to Use What

| Scenario | Best Choice |
|----------|------------|
| Simple column operations (upper, concat, math) | Built-in functions |
| Element-wise custom logic | `pandas_udf` (scalar) |
| Per-group custom logic (time-series, ML) | `applyInPandas` |
| Last resort, complex row logic | Regular `udf` |

---

## 1.10 Schema Variability

### Schema-on-Read vs Schema-on-Write

| | Schema-on-Write | Schema-on-Read |
|---|---|---|
| **When schema is enforced** | At write time | At read time |
| **Example** | Delta Lake, RDBMS | Raw Parquet/JSON on data lake |
| **Pros** | Consistent data, catches errors early | Flexible, fast ingestion |
| **Cons** | Schema changes require migration | Garbage-in-garbage-out risk |

### Handling Multi-Vendor Data

When different vendors send data in different schemas:

1. **Schema normalization**: Map each vendor's columns to a common schema.
2. **Union by name**: `df1.unionByName(df2, allowMissingColumns=True)` fills missing columns with null.
3. **Flexible parsing**: Read with `mergeSchema=True` or use a superset schema.
4. **Config-driven mapping**: Maintain a config file per vendor that maps their column names to your standard names.

### Schema Evolution Strategies

- **Additive only**: Only add new columns, never remove or rename. Backward compatible.
- **Versioned schemas**: Include a schema version in data. Reader selects logic based on version.
- **Delta Lake**: `ALTER TABLE ADD COLUMNS`, or set `.option("mergeSchema", "true")` on write.

### Null Handling

```python
from pyspark.sql.functions import coalesce, lit, when

df.filter(col("x").isNotNull())
df.fillna(0, subset=["value"])
df.withColumn("x", coalesce(col("x"), col("fallback"), lit(0)))
```

---

## 1.11 ML Integration in Data Pipelines

### Feature Engineering at Scale

Build features using Spark, train models externally (or in Spark MLlib):

```python
features = df.groupBy("user_id").agg(
    count("*").alias("total_orders"),
    avg("amount").alias("avg_amount"),
    max("amount").alias("max_amount"),
    countDistinct("product_id").alias("unique_products")
)
```

### Batch Inference with pandas_udf

```python
import torch
import pandas as pd
from pyspark.sql.functions import pandas_udf

model = load_model("model.pt")
model_broadcast = spark.sparkContext.broadcast(model)

@pandas_udf("double")
def predict(data: pd.Series) -> pd.Series:
    model = model_broadcast.value
    inputs = torch.tensor(data.values).float()
    with torch.no_grad():
        preds = model(inputs).numpy()
    return pd.Series(preds.flatten())

df.withColumn("prediction", predict(col("features")))
```

**Key pattern**: Broadcast the model to avoid loading it on every task invocation.

### Batch vs Real-Time Inference

| | Batch | Real-Time |
|---|---|---|
| **Latency** | Minutes to hours | Milliseconds |
| **Tool** | Spark + pandas_udf | REST API (Flask, FastAPI), or Kafka Streams |
| **Scale** | Massive (billions of rows) | Per-request |
| **Use case** | Daily predictions, scoring pipelines | Live recommendations, fraud detection |

### MLflow Basics

MLflow tracks experiments, logs models, and deploys them:
- **Tracking**: Log parameters, metrics, artifacts for each training run.
- **Model Registry**: Version and stage models (staging → production).
- **Serving**: Deploy models as REST endpoints.
- Integrated into Databricks natively.

---

# PART 2 -- SCENARIO-BASED INTERVIEW QUESTIONS

Each question includes the question, a structured answer, what the interviewer is looking for, and common follow-ups.

---

## 2.1 Spark Architecture & Internals

### Q1. Explain the Spark execution model. What happens when you call `.show()` on a DataFrame?

**Answer**:
1. Spark's Catalyst optimizer takes the logical plan (chain of transformations) and resolves column names/types (analysis phase).
2. It optimizes the plan -- pushes filters down, prunes unused columns, reorders joins (logical optimization).
3. It generates a physical plan -- choosing algorithms like Broadcast Hash Join vs Sort Merge Join.
4. The physical plan is compiled into optimized JVM bytecode (Tungsten code generation).
5. The DAG Scheduler splits the plan into stages at shuffle boundaries.
6. The Task Scheduler assigns tasks (one per partition per stage) to executors.
7. Executors run tasks in parallel, return results to the driver.
8. `.show()` collects the first 20 rows to the driver and prints them.

**What they're looking for**: Understanding of lazy evaluation, optimization pipeline, stage/task model.

**Follow-ups**: "What if you call `.show()` twice? Does it recompute?" (Yes, unless cached.) "What triggers a new stage?" (Shuffle/wide transformation.)

---

### Q2. What is the difference between narrow and wide transformations? Give examples.

**Answer**:
- **Narrow**: Each input partition produces exactly one output partition. No data movement. Examples: `filter`, `select`, `withColumn`, `map`, `union`. These are **pipelined** within a stage -- processed in a single pass.
- **Wide**: Input partitions contribute to multiple output partitions. Requires **shuffle** (network transfer). Examples: `groupBy`, `join`, `distinct`, `orderBy`, `repartition`. Each wide transformation creates a **stage boundary**.

**What they're looking for**: Shuffle awareness. Wide = expensive, narrow = cheap.

**Follow-ups**: "Is `coalesce` narrow or wide?" (Narrow -- it merges partitions without a full shuffle.) "What about `repartition`?" (Wide -- full shuffle.)

---

### Q3. Explain lazy evaluation in Spark. Why is it beneficial?

**Answer**:
Spark records transformations as a logical plan (DAG) without executing them. Execution only happens when an action is called. Benefits:
1. **End-to-end optimization**: Catalyst can optimize the entire chain (push filters down, prune columns).
2. **No intermediate materialization**: Narrow transforms are pipelined -- no temp DataFrames in memory.
3. **Efficient resource use**: Spark computes only what's needed for the action.

**What they're looking for**: Not just "it's lazy" but WHY lazy helps performance.

**Follow-ups**: "What if you want to force evaluation without an action?" (Use `.cache()` + `.count()`.)

---

### Q4. What is the Catalyst optimizer? What optimizations does it perform?

**Answer**:
Catalyst is Spark SQL's query optimizer. Key optimizations:
- **Predicate pushdown**: Pushes filters to the data source (e.g., Parquet reader skips non-matching row groups).
- **Column pruning**: Reads only required columns from columnar formats.
- **Constant folding**: Evaluates constant expressions at plan time.
- **Join reordering**: With CBO, picks the join order minimizing intermediate data.
- **Broadcast detection**: Automatically broadcasts small tables in joins.

**What they're looking for**: Practical understanding, not just naming the 4 phases.

---

### Q5. What is Tungsten? How does it improve Spark performance?

**Answer**:
Tungsten is Spark's execution engine optimizations:
1. **Binary memory format**: Stores data in compact binary rows instead of JVM objects. Reduces memory 3-5x, eliminates GC overhead.
2. **Whole-Stage Code Generation**: Compiles query operators into a single Java method. Eliminates virtual function call overhead per row.
3. **Cache-aware computation**: Processes data in CPU-cache-friendly sequential patterns.

**What they're looking for**: Understanding that Spark doesn't just "run Java" -- it generates custom code.

---

### Q6. How does Spark achieve fault tolerance?

**Answer**:
Through **lineage**. Every DataFrame/RDD tracks the chain of transformations that created it. If a partition is lost (executor failure), Spark recomputes only that partition by replaying transformations from the source. No data replication needed (unlike HDFS replication). For long lineage chains, use `.checkpoint()` to truncate lineage by writing data to reliable storage.

**Follow-ups**: "What if the source data is also lost?" (Spark cannot recover. Source data must be on reliable storage like HDFS/S3.) "What about shuffle files?" (If shuffle output is lost, Spark re-runs the map stage to regenerate them.)

---

### Q7. Explain the difference between `repartition()` and `coalesce()`.

**Answer**:
- `repartition(n)`: Creates exactly n partitions via a **full shuffle**. Can increase or decrease. Distributes data evenly (round-robin or by column hash).
- `coalesce(n)`: Reduces partitions to n **without a shuffle**. Merges adjacent partitions. Can only decrease. Faster but may produce uneven partitions.

**When to use**:
- `coalesce` before writing to reduce output files (no shuffle overhead).
- `repartition` when you need even distribution or partitioning by a specific column.

---

### Q8. What is a shuffle in Spark? Why is it expensive?

**Answer**:
A shuffle redistributes data across partitions by key. It involves:
1. **Serialization**: Convert in-memory data to bytes.
2. **Disk write**: Map-side data written to local disk as shuffle files.
3. **Network transfer**: Reduce-side tasks fetch their data from all map tasks.
4. **Deserialization**: Convert bytes back to objects.
5. **Sorting**: Data is sorted/hashed by key.

It's the most expensive operation because it involves disk I/O + network I/O + serialization. It also creates a **barrier** -- all map tasks must finish before reduce tasks start.

---

### Q9. What are the different deploy modes in Spark? When would you use each?

**Answer**:
- **Client mode**: Driver runs on the submitting machine. Use for interactive debugging, notebooks. Machine must stay connected.
- **Cluster mode**: Driver runs inside the cluster (YARN container, K8s pod). Use for production jobs. Submitting machine can disconnect.

**Follow-ups**: "What happens in cluster mode if the driver fails?" (Job fails. YARN can retry the ApplicationMaster a configurable number of times.)

---

### Q10. How does Spark handle memory management?

**Answer**:
Each executor's JVM heap is divided into:
- **Reserved Memory** (300 MB): Spark internal overhead.
- **User Memory** (~40% of remaining): For user data structures, UDFs.
- **Spark Memory** (~60% of remaining): Split dynamically between:
  - **Execution Memory**: Shuffles, sorts, joins, aggregations.
  - **Storage Memory**: Cached DataFrames, broadcast variables.

Execution can borrow from Storage (and evict cached data). Storage cannot force-evict Execution data. When memory is exhausted, Spark spills to disk (slower) or throws OOM.

---

## 2.2 Performance Optimization

### Q11. Your Spark job is running slow. How do you debug it?

**Answer**:
1. **Check the Spark UI** -- Jobs tab: which job is slow? Stages tab: which stage?
2. **Look at task distribution**: If one task takes 10x longer → data skew.
3. **Check shuffle metrics**: Large shuffle read/write → reduce data before shuffle, or broadcast.
4. **Check spill metrics**: Spill to disk → increase executor memory or reduce partition size.
5. **Check GC time**: > 10% of task time → reduce executor heap size, use G1GC.
6. **Check the physical plan** (`df.explain()`): Are filters pushed down? Is the right join strategy chosen?
7. **Check partition count**: Too few → cores idle. Too many → scheduling overhead.

**What they're looking for**: Systematic approach, not guesswork.

---

### Q12. How do you handle data skew in Spark?

**Answer**:
1. **Identify**: Spark UI shows one task much slower than others, with much larger shuffle read size.
2. **Salting**: Add random suffix to skewed key, aggregate in two passes (first with salted key, then by original key).
3. **AQE Skew Join**: Enable `spark.sql.adaptive.skewJoin.enabled=true`. Spark auto-splits skewed partitions.
4. **Broadcast join**: If one side is small enough, broadcast eliminates the shuffle entirely.
5. **Isolate**: Process skewed keys separately (with broadcast), combine results with `unionByName`.

**Follow-ups**: "Which approach do you prefer?" (AQE first because it's automatic. Salting for aggregation skew. Broadcast when applicable.)

---

### Q13. When would you use a broadcast join vs a sort merge join?

**Answer**:
- **Broadcast Hash Join**: One side is small (fits in memory on each executor). Broadcasts the small side to all executors. No shuffle needed. Very fast.
- **Sort Merge Join**: Both sides are large. Shuffles both by join key, sorts, then merges. Works for any size but requires shuffling both datasets.

Spark auto-selects broadcast if one side < `spark.sql.autoBroadcastJoinThreshold` (10 MB default). Force broadcast with `broadcast()` hint if statistics are stale.

---

### Q14. How does Adaptive Query Execution (AQE) work?

**Answer**:
AQE re-optimizes the query plan at runtime, after each shuffle stage:
1. **Coalescing partitions**: Merges too-small post-shuffle partitions into larger ones.
2. **Switching join strategy**: If a table turns out small after filtering, switches Sort Merge → Broadcast Hash Join.
3. **Skew handling**: Splits overly large partitions and replicates the matching partition from the other join side.

AQE uses actual shuffle statistics (not estimates), so it makes better decisions than compile-time optimization alone.

---

### Q15. How do you decide the number of shuffle partitions?

**Answer**:
- **Default is 200** -- often wrong.
- **Formula**: `total_shuffle_data_size / 200MB`.
- **For small data** (< 1 GB): Set to 10-50.
- **For large data** (TB scale): Set to 2000-10000.
- **Best approach**: Enable AQE (`spark.sql.adaptive.enabled=true`) and let Spark auto-coalesce small partitions.

---

### Q16. What is the difference between `cache()` and `persist()`?

**Answer**:
- `cache()` = `persist(StorageLevel.MEMORY_AND_DISK)`. Stores in memory, spills to disk if full.
- `persist(level)` lets you choose: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `DISK_ONLY`, `MEMORY_ONLY_SER` (serialized, saves memory but slower access).

Cache when a DataFrame is reused in multiple actions. Always `unpersist()` when done to free memory.

---

### Q17. Why are Python UDFs slow in Spark? What alternatives exist?

**Answer**:
Regular Python UDFs serialize data from JVM to Python **row by row** via a socket. Each row incurs serialization overhead. For 100M rows, that's 100M round trips.

**Alternatives** (fastest to slowest):
1. Built-in functions (`pyspark.sql.functions`) -- JVM native, no Python.
2. `pandas_udf` -- processes batches via Arrow (10-100x faster than regular UDF).
3. `applyInPandas` -- for grouped operations.
4. Regular `udf` -- last resort.

---

### Q18. How do you optimize a Spark job that writes too many small files?

**Answer**:
- `df.coalesce(N).write.parquet(path)` -- reduce partitions before write.
- `df.repartition(N).write.parquet(path)` -- if even distribution is needed.
- For Delta Lake: Run `OPTIMIZE` to compact small files post-write.
- Set `spark.sql.shuffle.partitions` appropriately (fewer partitions = fewer files).
- For streaming: Use trigger `availableNow` or `processingTime` with appropriate intervals.

---

### Q19. Explain `explain()` output. What do you look for?

**Answer**:
`df.explain(True)` shows four plans: Parsed, Analyzed, Optimized, Physical.

In the **Physical Plan**, I look for:
- `FileScan parquet [columns] PushedFilters: [...]` -- are filters and column pruning working?
- `BroadcastHashJoin` vs `SortMergeJoin` -- is the right strategy chosen?
- `Exchange hashpartitioning` -- shuffle. How much data?
- `WholeStageCodegen` -- are operators fused?

**Red flags**: All columns in FileScan (no pruning), SortMergeJoin on a small table (should be broadcast), no PushedFilters.

---

### Q20. What is speculative execution in Spark?

**Answer**:
If a task is significantly slower than other tasks in the same stage (a "straggler"), Spark launches a speculative copy on another executor. Whichever finishes first wins; the other is killed.

Enable with `spark.speculation=true`. Useful for hardware-caused stragglers. Not useful for data skew (the speculative task will be just as slow).

---

## 2.3 ETL & Pipeline Design

### Q21. How would you design an incremental data pipeline?

**Answer**:
1. **Track state**: Store a high-water mark (last processed timestamp, last file path) in a state store (database, file, Delta table).
2. **Read incrementally**: Query source for records newer than the watermark.
3. **Transform**: Apply business logic.
4. **Write**: Upsert (MERGE) or overwrite the target partition.
5. **Update state**: Write the new watermark.
6. **Idempotency**: If the job fails after writing but before updating the watermark, the next run reprocesses the same data -- so writes must be idempotent (overwrite, not append).

---

### Q22. What is idempotency? Why is it important in data pipelines?

**Answer**:
A pipeline is idempotent if running it multiple times with the same input produces the same output. It's critical because:
- Jobs can fail and be retried.
- Schedulers may accidentally trigger a job twice.
- Backfills reprocess existing data.

**How to achieve**: Use `mode("overwrite")` at the partition level. Use `MERGE` (upsert) for row-level. Avoid blind `append` to non-deduplicated tables.

---

### Q23. How do you handle late-arriving data in a batch pipeline?

**Answer**:
1. **Reprocessing window**: Re-aggregate the last N days/hours on each run to catch late data.
2. **Separate pipeline**: A "late data handler" that processes only late records and merges into existing output.
3. **Event time vs processing time**: Partition by event time, not processing time. Late data goes into the correct partition.
4. **Delta Lake MERGE**: Upsert late records into the correct target partition.

---

### Q24. Explain the difference between ETL and ELT.

**Answer**:
- **ETL** (Extract, Transform, Load): Transform data before loading into the target. Traditional approach. Transform happens in the pipeline tool (Spark, Python).
- **ELT** (Extract, Load, Transform): Load raw data first, transform inside the target system (data warehouse SQL). Modern approach with cloud warehouses (BigQuery, Snowflake, Databricks).

**Interview answer**: "In modern data engineering, ELT is preferred because cloud warehouses are powerful enough to transform data in-place. The medallion architecture (Bronze → Silver → Gold) is essentially ELT -- raw data lands in Bronze, transformations happen within the lake."

---

### Q25. How do you ensure data quality in a pipeline?

**Answer**:
1. **Schema validation**: Check column names, types, nullability before processing.
2. **Row count checks**: Compare input count vs output count. Alert if mismatch exceeds threshold.
3. **Null percentage checks**: Flag if a usually-populated column suddenly has > 10% nulls.
4. **Value range checks**: Ensure numeric columns stay within expected ranges.
5. **Uniqueness checks**: Verify primary key uniqueness.
6. **Freshness checks**: Alert if no new data arrives within the expected window.
7. **Dead letter queue**: Route bad records to a separate location instead of failing the job.

---

### Q26. What is a dead letter queue (DLQ)?

**Answer**:
A DLQ captures records that fail processing (malformed, schema mismatch, business rule violation). Instead of failing the entire job, bad records go to a separate table/topic for later investigation. The main pipeline continues.

Implementation: Wrap transformation logic in try/except (in UDFs), flag bad records, filter them out, write to a separate location.

---

### Q27. How do you handle schema evolution in a data pipeline?

**Answer**:
- **Additive changes** (new columns): Use `mergeSchema=True` on reads, or Delta Lake `ALTER TABLE ADD COLUMNS`.
- **Breaking changes** (rename, delete, type change): Version the schema, maintain a mapping layer, and transform at read time.
- **Defensive coding**: Use `unionByName(allowMissingColumns=True)`, `coalesce` for fallback values, and explicit casting.

---

### Q28. What is backfilling? How do you approach it?

**Answer**:
Backfilling = reprocessing historical data (after a bug fix, new logic, or new column).

**Approach**:
1. Parameterize your pipeline by date range.
2. Run it for each historical date/partition.
3. Use `mode("overwrite")` per partition to replace old data.
4. Process in chunks (e.g., one month at a time) to control resource usage.
5. Validate output: compare row counts and sample data against expectations.

---

### Q29. What are SCD Type 1 and Type 2? When would you use each?

**Answer**:
- **Type 1**: Overwrite the old value. Simple, no history. Use when you don't need to track changes (e.g., fixing typos).
- **Type 2**: Add a new row with `effective_start_date`, `effective_end_date`, and an `is_current` flag. Preserves complete history. Use for tracking changes over time (e.g., customer address changes).

Delta Lake `MERGE` makes Type 2 straightforward: update the old row's end date, insert the new row.

---

### Q30. How do you orchestrate a pipeline with multiple dependent steps?

**Answer**:
Use a workflow orchestrator:
- **Databricks Workflows**: Native, tight integration with notebooks and jobs.
- **Apache Airflow**: DAG-based scheduling. Define task dependencies in Python.
- **Shell scripts + cron**: Simplest but no retry logic, no dependency management, no monitoring.

**Key features needed**: Dependency management, retry on failure, alerting, idempotent tasks, parameterized runs.

---

## 2.4 Data Modeling & Storage

### Q31. Why use Parquet instead of CSV for big data?

**Answer**:
| | Parquet | CSV |
|---|---|---|
| Format | Columnar | Row-based |
| Column pruning | Yes (reads only selected columns) | No (reads all columns) |
| Predicate pushdown | Yes (skips non-matching row groups) | No |
| Compression | Excellent (similar values together) | Poor |
| Schema | Embedded | None (must infer or specify) |
| Size | 3-10x smaller | Baseline |

---

### Q32. What is partition pruning? How does it improve query performance?

**Answer**:
If data is stored partitioned by `date`, and you query `WHERE date = '2024-01-15'`, Spark reads only the `date=2024-01-15` directory. All other date directories are skipped entirely.

Combined with **predicate pushdown** (Parquet column statistics), Spark can skip data at both the directory level and the row-group level.

---

### Q33. Explain the medallion architecture (Bronze/Silver/Gold).

**Answer**:
- **Bronze**: Raw data, ingested as-is. Append-only, no transformations. The recovery point.
- **Silver**: Cleaned, deduplicated, schema-enforced. Business entities (one table per entity).
- **Gold**: Aggregated, business-specific views. Ready for dashboards, ML, reporting.

This layered approach gives you data lineage, easy reprocessing (just replay Silver from Bronze), and clean separation of concerns.

---

### Q34. How do you design a partition key for a Hive/Parquet table?

**Answer**:
Choose columns that:
1. Are frequently used in `WHERE` clauses (enables partition pruning).
2. Have moderate cardinality (too few = large partitions, too many = too many small files).
3. Match your query patterns.

Common choices: `date` (daily), `year/month` (monthly), `region`.

**Avoid**: Partitioning by high-cardinality columns (user_id with millions of values → millions of directories).

---

### Q35. What is the difference between a data lake and a data warehouse?

**Answer**:
| | Data Lake | Data Warehouse |
|---|---|---|
| Storage | Cheap object storage (S3, ADLS) | Dedicated compute + storage (Snowflake, Redshift) |
| Schema | Schema-on-read (flexible) | Schema-on-write (strict) |
| Data types | Structured, semi-structured, unstructured | Structured only |
| Cost | Low storage, pay for compute | Higher |
| Query performance | Variable (depends on format, partitioning) | Optimized (indexes, caching) |

**Lakehouse** (Databricks approach): Combines both -- cheap storage with warehouse-like features (ACID, schema enforcement, indexing via Delta Lake).

---

### Q36. When would you denormalize data?

**Answer**:
In **analytical/OLAP** workloads where:
- Queries join the same tables repeatedly.
- Query latency matters more than storage cost.
- Data is mostly read, rarely updated.

Example: Pre-join `orders` + `customers` + `products` into a single `fact_orders_enriched` table. Eliminates joins at query time. Trade-off: data duplication, more complex updates.

---

### Q37. What is Z-ordering in Delta Lake?

**Answer**:
Z-ordering colocates related data within files based on multiple columns. If you Z-order by `(region, date)`, rows with similar region+date values are stored in the same files. This maximizes the effectiveness of data skipping (Parquet min/max statistics).

Use when you frequently filter on multiple columns. Run `OPTIMIZE ... ZORDER BY (col1, col2)`.

---

### Q38. What are the consistency levels in Cassandra?

**Answer**:
- **ONE**: Write/read to/from one replica. Fastest, lowest consistency.
- **QUORUM**: Majority of replicas (2 out of 3). Balanced.
- **ALL**: All replicas. Slowest, highest consistency.

For write + read to guarantee consistency: `write_CL + read_CL > replication_factor`. E.g., `QUORUM + QUORUM > 3` → strong consistency.

---

## 2.5 Databricks-Specific

### Q39. What is Delta Lake? How is it different from Parquet?

**Answer**:
Delta Lake = Parquet files + a transaction log (`_delta_log/`). The log records every change (add file, remove file), enabling:
- **ACID transactions**: Safe concurrent writes.
- **Time travel**: Read any previous version.
- **Schema enforcement**: Rejects bad writes.
- **MERGE (upsert)**: Insert/update/delete in one operation.

Plain Parquet has none of these -- it's just files. No concurrency control, no upsert, no time travel.

---

### Q40. Explain the Databricks architecture.

**Answer**:
- **Control Plane** (managed by Databricks): Web UI, notebooks, scheduler, cluster manager, Unity Catalog.
- **Data Plane** (in your cloud account): VMs running Spark, accessing your S3/ADLS storage.

Your data never leaves your cloud account. Databricks manages orchestration.

---

### Q41. What is Auto Loader? How does it work?

**Answer**:
Auto Loader incrementally ingests new files from cloud storage using Structured Streaming:
- **File notification mode**: Cloud storage events (S3 events, ADLS events) trigger processing. Scales to millions of files.
- **Directory listing mode**: Periodically lists the directory. Simpler but slower for many files.

It auto-detects schema, handles schema evolution, and tracks which files have been processed via checkpoints.

---

### Q42. What is Unity Catalog?

**Answer**:
Centralized governance layer for Databricks:
- **Access control**: Row-level, column-level security.
- **Data lineage**: Track which tables were used to build which views.
- **Auditing**: Who accessed what data, when.
- **Single namespace**: `catalog.schema.table` across all workspaces.

---

### Q43. What are the Databricks cluster types?

**Answer**:
- **All-Purpose clusters**: Interactive, long-running. For notebooks and exploration. More expensive.
- **Job clusters**: Created for a specific job, destroyed after. For automated production jobs. Cheaper.
- **SQL Warehouses**: Dedicated SQL compute for BI queries and dashboards.

**Best practice**: Use job clusters for production, all-purpose for development.

---

### Q44. What is Photon?

**Answer**:
Photon is Databricks' native C++ execution engine that replaces Spark's JVM-based execution for supported operations. It's 2-8x faster for SQL/DataFrame workloads. No code changes needed -- just enable it on the cluster. It's particularly fast for scans, filters, aggregations, and joins.

---

### Q45. How does Delta Lake's MERGE work?

**Answer**:
```sql
MERGE INTO target USING source ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Under the hood: Spark joins target and source, identifies matched/unmatched rows, writes new Parquet files for the affected partitions, and updates the transaction log. Old files are retained for time travel until `VACUUM` is run.

---

## 2.6 Debugging & Troubleshooting

### Q46. You get "Container killed by YARN for exceeding memory limits." What do you do?

**Answer**:
1. **Increase `--executor-memoryOverhead`**: Python workers and JVM overhead use off-heap memory. Default is often too low for PySpark.
2. **Increase `--executor-memory`**: More heap for shuffle buffers and cached data.
3. **Reduce data per task**: Increase partition count with `repartition` or `spark.sql.shuffle.partitions`.
4. **Check for data skew**: One partition may be much larger. Use salting or AQE.
5. **Reduce broadcast size**: If broadcasting a large table, it consumes driver + executor memory.

---

### Q47. A Spark job succeeds but produces wrong results. How do you debug?

**Answer**:
1. **Check join conditions**: Missing or wrong join keys cause data duplication or loss.
2. **Check for data skew**: Skewed partitions may overflow aggregation buffers.
3. **Check null handling**: Joins on null keys produce unexpected results (null != null in SQL).
4. **Check partition overwrite**: `mode("overwrite")` with partitioning may overwrite more data than intended.
5. **Validate intermediate steps**: `.show()` or `.count()` at each transformation to isolate where data goes wrong.
6. **Compare with a small sample**: Run the logic on a tiny dataset and verify manually.

---

### Q48. Your Spark job processes 4 million devices daily. One day it's 3x slower. What happened?

**Answer**:
1. **Data volume spike**: More data than usual? Check input file sizes.
2. **Data skew**: A new device or category dominates? Check task duration distribution.
3. **Cluster issues**: Fewer executors available? Check YARN resource utilization.
4. **Upstream changes**: Source data schema changed? New columns, different formats?
5. **Small files**: Too many small input files? Consolidate with `OPTIMIZE` or `coalesce`.
6. **Shuffle spill**: Check Spark UI for spill metrics. Increase memory or reduce partition size.

---

### Q49. How do you monitor a production Spark pipeline?

**Answer**:
1. **Job duration tracking**: Alert if runtime exceeds 2x the historical average.
2. **Data quality checks**: Row counts, null percentages, value ranges -- post-processing.
3. **Error alerts**: Email/Slack on job failure. Include error message and stack trace.
4. **Spark UI / History Server**: Preserved for post-mortem analysis.
5. **Logging**: Structured logs with job name, timestamp, record counts, partition info.
6. **SLA monitoring**: Track data freshness -- alert if output isn't updated within the expected window.

---

### Q50. You need to process data from multiple vendors with different schemas. How?

**Answer**:
1. **Config-driven normalization**: Maintain a mapping file per vendor (vendor column → standard column).
2. **Read with mergeSchema**: `spark.read.option("mergeSchema", "true").parquet(path)` unions all schemas.
3. **Union by name**: `df1.unionByName(df2, allowMissingColumns=True)` handles different column sets.
4. **Common transformation layer**: After normalization, all data flows through the same business logic.
5. **Validation**: Check that required columns are present and correctly typed.

---

# PART 3 -- SYSTEM DESIGN FOR DATA ENGINEERING

---

## 3.1 System Design Framework

When you get a system design question in an interview, follow this structure:

### Step 1: Clarify Requirements (2-3 min)
- What is the data volume? (GB/day, TB/day?)
- What is the latency requirement? (Real-time, near-real-time, daily batch?)
- What are the source systems? (APIs, databases, files, IoT devices?)
- Who are the consumers? (Dashboards, ML models, APIs, reports?)
- What is the retention period? (30 days, 1 year, forever?)

### Step 2: High-Level Architecture (5 min)
- Draw the data flow: Sources → Ingestion → Storage → Processing → Serving.
- Choose tools for each layer.
- Identify the main challenges (scale, latency, fault tolerance).

### Step 3: Deep Dive (10-15 min)
- Storage format and partitioning strategy.
- Processing logic (batch, stream, incremental).
- Data modeling (schema, aggregation layers).
- Fault tolerance (retries, idempotency, dead letter queues).
- Monitoring and alerting.

### Step 4: Trade-offs and Alternatives (3-5 min)
- Why this tool over alternatives?
- What would change at 10x scale?
- Cost considerations.

---

## 3.2 Design 1: Large-Scale IoT Data Pipeline

### Requirements
- **Ingest**: 5 million IoT devices sending telemetry every 15 minutes.
- **Volume**: ~480 million records/day (~5 TB/day raw).
- **Latency**: Data must be available for analytics within 1 hour of arrival.
- **Consumers**: Dashboards, daily reports, ML models.
- **Retention**: Raw data for 2 years. Aggregated data forever.

### Architecture

```
IoT Devices (5M)
    |
    v  (MQTT / HTTP)
[Message Broker: Kafka]
    |
    +-----> [Stream Ingestion: Spark Structured Streaming / Auto Loader]
    |              |
    |              v
    |       [Bronze Layer: Raw Parquet/Delta on S3/ADLS]
    |              |
    |              v (Batch: hourly or daily)
    |       [Silver Layer: Cleaned, deduplicated, typed Delta tables]
    |              |
    |              v (Batch: daily)
    |       [Gold Layer: Aggregated KPIs, device summaries]
    |              |
    |              +-----> [Dashboard: Grafana / PowerBI]
    |              +-----> [ML Feature Store]
    |              +-----> [API: Serving layer for queries]
    |
    +-----> [Real-time Alerts: Kafka Streams / Flink]
                   |
                   v
            [Alert Service: PagerDuty / Slack]
```

### Key Design Decisions

**Ingestion**: Kafka as buffer. Devices push to Kafka topics (partitioned by device_id). Decouples ingestion from processing -- if Spark is slow, Kafka retains data.

**Storage**: Delta Lake on cloud object storage.
- Bronze: Raw events, partitioned by `ingestion_date`.
- Silver: Cleaned events, partitioned by `event_date`. Deduplicated using `device_id + timestamp`.
- Gold: Daily aggregates partitioned by `date`.

**Processing**:
- Hourly micro-batch from Kafka → Bronze (Auto Loader or Structured Streaming).
- Daily batch Bronze → Silver (data quality, dedup, schema normalization).
- Daily batch Silver → Gold (aggregation, KPI computation).

**Partitioning**: By date (primary) and device_type or region (secondary if query patterns warrant it).

**Fault Tolerance**:
- Kafka retains data for 7 days → replay if ingestion fails.
- Spark checkpointing for Structured Streaming.
- Idempotent writes to Delta (MERGE or partition overwrite).

**Scaling**:
- Add Kafka partitions and Spark executors as device count grows.
- Use Delta Lake's OPTIMIZE for file compaction.
- Use Z-ordering on frequently queried columns.

### Trade-offs

| Decision | Trade-off |
|----------|-----------|
| Kafka as buffer | Adds complexity vs direct file ingestion, but handles burst traffic and decouples systems |
| Hourly micro-batch vs true streaming | Slight latency (up to 1 hour) but much simpler processing logic |
| Delta Lake vs plain Parquet | Small overhead from transaction log, but gains ACID, time travel, MERGE |

---

## 3.3 Design 2: KPI Aggregation Pipeline

### Requirements
- Compute daily and monthly KPIs from 4 million smart meters.
- Daily run at 6 AM for the previous day.
- Monthly run on the 1st for the previous month.
- KPIs: total consumption, peak usage hour, average load, device uptime.
- Output: Cassandra (serving layer) + Parquet on HDFS (analytics).

### Architecture

```
[Source: HDFS Parquet files (15-min interval data)]
    |
    v
[Daily Job: Spark Batch]
    |
    +---> Read new day's data (incremental, using file tracking)
    +---> Validate: row count, null checks, schema match
    +---> Compute daily KPIs:
    |       - groupBy(device, date).agg(sum, max, avg, count)
    |       - Compute derived metrics (uptime = count of non-null readings / expected count)
    |       - Handle late data: re-aggregate last 3 days (rolling window)
    +---> Write to:
            - HDFS Parquet (partitioned by date) → for analytics
            - Cassandra (device as partition key, date as clustering key) → for APIs

[Monthly Job: Spark Batch]
    |
    +---> Read daily aggregates for the month (from HDFS Parquet)
    +---> Compute monthly KPIs:
    |       - groupBy(device, month).agg(sum, max, avg)
    +---> Write to HDFS + Cassandra
```

### Key Design Decisions

**Incremental ingestion**: Track the last processed file path. Only read new files each day.

**Late data handling**: Re-aggregate the last 3 days on each run. This catches records that arrived late but belong to previous dates.

**Two-pass aggregation**: Daily KPIs are computed from raw data. Monthly KPIs are computed from daily KPIs (not re-reading raw data). This is much faster.

**Idempotency**: Write with `mode("overwrite")` per partition. Re-running for the same date replaces the output.

**Performance**:
- Pre-aggregate before pivot (reduce row count before the expensive operation).
- Broadcast small lookup tables (device metadata).
- Persist intermediate DataFrames that are used for multiple outputs.

---

## 3.4 Design 3: SLA / Data Quality Monitoring

### Requirements
- Track data timeliness: Did data from each device arrive on time?
- Track data completeness: What percentage of expected records actually arrived?
- Generate daily SLA reports.
- Alert if SLA drops below threshold.

### Architecture

```
[Source: Device metadata (expected devices)]  +  [Source: Received data (HDFS)]
    |                                                    |
    v                                                    v
[Spark Job: SLA Computation]
    |
    +---> Join device list with received data (left join)
    +---> Compute per-device:
    |       - received_count vs expected_count → completeness %
    |       - latest_receive_time vs deadline → timeliness flag
    +---> Aggregate:
    |       - Per vendor: avg completeness, % on-time
    |       - Per region: avg completeness, % on-time
    |       - Overall: total devices, total received, SLA %
    +---> Write SLA report:
    |       - Parquet on HDFS (historical tracking)
    |       - Cassandra (API serving)
    |       - CSV to Azure Blob (email attachment)
    +---> Alert:
            - If SLA < 95%: Send Slack/email alert
            - If SLA < 80%: Send PagerDuty critical alert
```

### Key Design Decisions

**Completeness calculation**: Left join expected devices with received data. Null on the right side = missing device. `completeness = count(received) / count(expected) * 100`.

**Timeliness**: Define deadlines (e.g., data for hour H must arrive by H+7 hours). Flag each record as on-time or late. Calculate percentage.

**Historical tracking**: Store daily SLA snapshots. Enables trend analysis -- "is SLA degrading over time?"

**Alerting**: Threshold-based. Configurable per client/vendor. Automated via pipeline (not manual).

---

## 3.5 Design 4: Lambda / Kappa Architecture

### Lambda Architecture

```
[Source]
    |
    +-----> [Batch Layer: Spark Batch]  → processes all historical data → [Batch View]
    |                                                                          |
    +-----> [Speed Layer: Spark Streaming] → processes new data → [Real-time View]
                                                                       |
                                                          [Serving Layer: merges both views]
```

**Pros**: Accurate (batch recomputes from scratch), low-latency (speed layer serves recent data).
**Cons**: Maintaining two code paths (batch + streaming) that must produce consistent results.

### Kappa Architecture

```
[Source: Kafka (immutable event log)]
    |
    v
[Stream Processing: Spark Streaming / Flink]
    |
    v
[Serving Layer]
```

Everything is a stream. Historical reprocessing = replay Kafka from the beginning.

**Pros**: Single code path. Simpler.
**Cons**: Kafka must retain all data (expensive). Reprocessing is slow (must replay everything).

### When to Use Which

| Scenario | Architecture |
|----------|-------------|
| Need sub-second latency + accurate historical data | Lambda |
| Can tolerate minutes of latency, want simplicity | Kappa |
| Mostly batch with occasional near-real-time | Batch + micro-batch (simplest) |

**Interview answer**: "For most data engineering use cases, I'd start with a simple batch pipeline. If near-real-time is needed, add Structured Streaming micro-batches. I'd only reach for Lambda/Kappa if latency requirements are sub-second."

---
