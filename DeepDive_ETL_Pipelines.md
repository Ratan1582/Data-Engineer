# ETL Pipeline Design -- A Complete Deep Dive

**From Raw Data Ingestion to Production-Grade Pipeline Patterns**

---

## Table of Contents

1. [ETL vs ELT](#1-etl-vs-elt)
2. [Batch Processing Patterns](#2-batch-processing-patterns)
3. [Incremental Pipeline Design](#3-incremental-pipeline-design)
4. [Idempotency](#4-idempotency)
5. [Error Handling and Dead Letter Queues](#5-error-handling-and-dead-letter-queues)
6. [Schema Evolution](#6-schema-evolution)
7. [Backfill Strategies](#7-backfill-strategies)
8. [Data Quality Gates](#8-data-quality-gates)
9. [Pipeline Orchestration](#9-pipeline-orchestration)
10. [Streaming ETL](#10-streaming-etl)
11. [Medallion Architecture](#11-medallion-architecture)
12. [Slowly Changing Dimensions (SCD)](#12-slowly-changing-dimensions-scd)
13. [Pipeline Testing](#13-pipeline-testing)
14. [Monitoring and Alerting](#14-monitoring-and-alerting)
15. [Quick-Reference Cheat Sheet](#15-quick-reference-cheat-sheet)

---

## 1. ETL vs ELT

### 1.1 ETL (Extract-Transform-Load)

Data is transformed **before** loading into the target:

```
Source → Extract → Transform (staging server/Spark) → Load → Target (warehouse)
```

The transformation happens outside the target system. Common when:
- The target system has limited compute (e.g., a traditional RDBMS)
- You need to clean/reshape data before it enters the warehouse
- Sensitive data must be masked before loading
- Legacy systems that require specific format

### 1.2 ELT (Extract-Load-Transform)

Raw data is loaded first, transformed **inside** the target:

```
Source → Extract → Load (raw) → Transform (inside warehouse/lakehouse) → Curated Tables
```

The target system (data lake, lakehouse, cloud warehouse) handles transformations. Common when:
- The target has powerful compute (Spark, BigQuery, Snowflake, Databricks)
- You want to preserve raw data (data lake pattern)
- Different teams need different transformations from the same raw data
- Transformations evolve over time (raw data is always available)

### 1.3 Modern Reality: ELT Dominates

In modern data engineering, **ELT with a data lakehouse** is the dominant pattern:

```
Sources → Ingest raw data → Bronze (raw) → Silver (cleaned) → Gold (aggregated)
                             ↑ ELT: transform inside the lakehouse
```

This is the Medallion Architecture. More in Section 11.

---

## 2. Batch Processing Patterns

### 2.1 Full Load (Snapshot)

Replace the entire target with a fresh copy of the source:

```python
df = spark.read.jdbc(url, "source_table", properties=props)
df.write.mode("overwrite").parquet("/target/table/")
```

**When to use**: Small tables, dimension tables, reference data
**Pros**: Simple, always consistent
**Cons**: Wasteful for large tables that change little

### 2.2 Incremental Load

Only process data that changed since the last run:

```python
last_processed = get_watermark("orders_pipeline")  # e.g., "2024-01-15 00:00:00"

new_data = spark.read.parquet("/source/orders/") \
    .filter(col("updated_at") > last_processed)

new_data.write.mode("append").parquet("/target/orders/")
update_watermark("orders_pipeline", new_data.agg(max("updated_at")).collect()[0][0])
```

**When to use**: Large tables with a reliable timestamp or sequence column
**Pros**: Much faster than full load, processes only changes
**Cons**: Requires a monotonic column (timestamp, auto-increment ID), can miss deletes

### 2.3 Change Data Capture (CDC)

Capture every insert, update, and delete from the source:

```
Source DB → CDC Tool (Debezium, AWS DMS) → Kafka → Spark → Target

CDC Events:
{op: "INSERT", after: {id: 1, name: "Alice", age: 30}}
{op: "UPDATE", before: {id: 1, name: "Alice", age: 30}, after: {id: 1, name: "Alice", age: 31}}
{op: "DELETE", before: {id: 1, name: "Alice", age: 31}}
```

In the target (Delta Lake):
```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "/target/customers/")
source = spark.read.format("kafka").load().selectExpr("value.*")

target.alias("t").merge(
    source.alias("s"),
    "t.id = s.id"
).whenMatchedUpdate(
    condition="s.op = 'UPDATE'",
    set={"name": "s.name", "age": "s.age"}
).whenNotMatchedInsert(
    condition="s.op = 'INSERT'",
    values={"id": "s.id", "name": "s.name", "age": "s.age"}
).whenMatchedDelete(
    condition="s.op = 'DELETE'"
).execute()
```

**When to use**: Real-time or near-real-time data sync, when you need to capture deletes
**Pros**: Complete change history, handles deletes
**Cons**: Complex setup (CDC tool, message queue, merge logic)

### 2.4 Snapshot Differencing

Compare today's snapshot with yesterday's to detect changes:

```python
today = spark.read.parquet("/snapshots/2024-01-16/")
yesterday = spark.read.parquet("/snapshots/2024-01-15/")

# New rows
new_rows = today.join(yesterday, "id", "left_anti")

# Deleted rows
deleted_rows = yesterday.join(today, "id", "left_anti")

# Changed rows (exists in both, but different values)
changed_rows = today.alias("t").join(yesterday.alias("y"), "id") \
    .filter(col("t.name") != col("y.name") | col("t.age") != col("y.age"))
```

**When to use**: Source doesn't support CDC or reliable timestamps
**Cons**: Requires two full copies of the data, O(n) comparison

---

## 3. Incremental Pipeline Design

### 3.1 Watermarks

A watermark is a marker that tracks the latest processed data point:

```python
# Simple watermark: timestamp stored in a control table or file
watermarks = {
    "orders_pipeline": "2024-01-15T23:59:59",
    "events_pipeline": "2024-01-16T06:00:00"
}

# Read only data newer than the watermark
df = spark.read.parquet("/data/orders/") \
    .filter(col("event_time") > watermarks["orders_pipeline"])

# After processing, update the watermark
new_watermark = df.agg(max("event_time")).collect()[0][0]
update_watermark("orders_pipeline", new_watermark)
```

### 3.2 Partition-Based Incremental

Use file system partitions as the incremental boundary:

```python
# Data is partitioned by date: /data/events/date=2024-01-15/
# Process only new partitions
processed_dates = get_processed_dates("events_pipeline")
all_dates = list_partitions("/data/events/")
new_dates = [d for d in all_dates if d not in processed_dates]

for date in new_dates:
    df = spark.read.parquet(f"/data/events/date={date}/")
    process_and_write(df)
    mark_processed("events_pipeline", date)
```

### 3.3 Auto Loader (Databricks)

Auto Loader tracks which files have been processed using a checkpoint:

```python
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/") \
    .load("/data/landing/")

df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoints/events/") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .start("/data/bronze/events/")
```

**How it works**: Auto Loader maintains a checkpoint of processed files. On each run, it discovers only new files (via directory listing or cloud events) and processes them.

### 3.4 Exactly-Once Processing

**At-least-once**: Data may be processed multiple times (duplicates possible).
**At-most-once**: Data may be lost (no retry).
**Exactly-once**: Each record processed exactly once.

Achieving exactly-once:
1. **Idempotent writes** (Section 4): Writing the same data twice produces the same result
2. **Transactional commits**: Delta Lake MERGE with deduplication
3. **Checkpoint + replay**: If a job fails mid-way, restart from the checkpoint

---

## 4. Idempotency

### 4.1 What Is Idempotency

An operation is idempotent if running it multiple times produces the same result as running it once:

```
f(x) = f(f(x))

Idempotent:     OVERWRITE partition with data → same result each time
Not idempotent: APPEND data → duplicates on each re-run
```

### 4.2 Why It Matters

Pipelines fail. Networks drop. Tasks get retried. If your pipeline is not idempotent, re-running after a failure creates duplicates or inconsistent data.

### 4.3 Achieving Idempotency

**Pattern 1: Overwrite by partition**
```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("date").parquet("/target/")
# Re-running overwrites the same date partitions -- no duplicates
```

**Pattern 2: MERGE with deduplication**
```sql
MERGE INTO target t
USING source s
ON t.id = s.id AND t.date = s.date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Pattern 3: Write with unique run ID, then swap**
```python
run_id = generate_uuid()
df.write.parquet(f"/target/_staging/{run_id}/")
# Atomic rename (HDFS) or transaction log update (Delta Lake)
```

**Pattern 4: Deduplication in the target**
```python
df_deduped = df.dropDuplicates(["id", "date"])
df_deduped.write.mode("overwrite").partitionBy("date").parquet("/target/")
```

---

## 5. Error Handling and Dead Letter Queues

### 5.1 Types of Errors

| Error Type | Example | Handling |
|-----------|---------|---------|
| **Schema error** | Expected int, got string | Rescue data / quarantine |
| **Null error** | Required field is null | Default value or reject |
| **Business rule** | Negative quantity | Quarantine + alert |
| **Data format** | Corrupt JSON | PERMISSIVE mode + _corrupt_record |
| **System error** | Executor OOM, network timeout | Retry logic |

### 5.2 Dead Letter Queue (DLQ)

Bad records go to a separate "dead letter" location for investigation:

```python
# Read with permissive mode: corrupt records stored in _corrupt_record column
df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(expected_schema.add("_corrupt_record", StringType())) \
    .json("/data/raw/")

# Split good and bad records
good_records = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
bad_records = df.filter(col("_corrupt_record").isNotNull())

# Write good records to target
good_records.write.mode("append").parquet("/data/silver/")

# Write bad records to DLQ
bad_records.write.mode("append").parquet("/data/dlq/")
```

### 5.3 Retry Logic

```python
import time

def run_with_retry(func, max_retries=3, delay=60):
    for attempt in range(1, max_retries + 1):
        try:
            return func()
        except Exception as e:
            if attempt == max_retries:
                raise
            print(f"Attempt {attempt} failed: {e}. Retrying in {delay}s...")
            time.sleep(delay * attempt)  # exponential backoff
```

---

## 6. Schema Evolution

### 6.1 The Problem

Source systems change their schema: new columns added, columns renamed, types changed. A rigid pipeline breaks when this happens.

### 6.2 Schema Evolution Strategies

**Backward compatible**: New code can read old data (add columns with defaults).
**Forward compatible**: Old code can read new data (ignore unknown columns).
**Full compatible**: Both directions work.

### 6.3 Adding New Columns

```python
# Parquet: new columns in source, old data lacks them
# Spark handles this with mergeSchema
df = spark.read.option("mergeSchema", True).parquet("/data/")
# Old partitions: columns A, B
# New partitions: columns A, B, C
# Result: A, B, C (C is null for old data)
```

Delta Lake:
```python
df.write \
    .option("mergeSchema", True) \
    .mode("append") \
    .format("delta") \
    .save("/data/table/")
```

### 6.4 Handling Missing Columns Safely

```python
# Check if column exists before using it
if "new_column" in df.columns:
    df = df.withColumn("derived", col("new_column") * 2)
else:
    df = df.withColumn("derived", lit(None).cast("double"))
```

### 6.5 Schema Registry

For streaming pipelines (Kafka), a schema registry ensures producers and consumers agree on the schema:

```
Producer → registers schema v1 → Schema Registry
Producer → registers schema v2 (backward compatible) → Schema Registry
Consumer → fetches latest compatible schema → Schema Registry

Compatibility modes: BACKWARD, FORWARD, FULL, NONE
```

---

## 7. Backfill Strategies

### 7.1 What Is a Backfill

A backfill reprocesses historical data, usually because:
- A bug was fixed in the transformation logic
- A new column or metric was added
- Data quality issues were discovered in old data
- The pipeline was just built and needs to process historical data

### 7.2 Partition-Based Backfill

```python
# Reprocess specific date ranges
backfill_dates = generate_date_range("2023-01-01", "2023-12-31")

for date in backfill_dates:
    df = spark.read.parquet(f"/data/raw/date={date}/")
    result = transform(df)
    result.write.mode("overwrite").parquet(f"/data/silver/date={date}/")
```

### 7.3 Blue-Green Backfill

Write the backfilled data to a new location, then swap:

```python
# Step 1: Write to a new location
for date in all_dates:
    df = spark.read.parquet(f"/data/raw/date={date}/")
    result = new_transform(df)
    result.write.mode("overwrite").parquet(f"/data/silver_v2/date={date}/")

# Step 2: Validate the new data
assert_data_quality("/data/silver_v2/")

# Step 3: Swap (rename or update catalog)
rename("/data/silver/", "/data/silver_old/")
rename("/data/silver_v2/", "/data/silver/")
```

### 7.4 Delta Lake Time Travel for Rollback

```python
# If a backfill goes wrong, rollback to a previous version
spark.sql("RESTORE TABLE delta.`/data/table/` TO VERSION AS OF 42")
```

---

## 8. Data Quality Gates

### 8.1 Quality Dimensions

| Dimension | Definition | Example Check |
|-----------|-----------|--------------|
| **Completeness** | No missing values | NULL rate < 1% |
| **Accuracy** | Values are correct | Amount > 0 for sales records |
| **Consistency** | Data agrees across sources | Sum of parts = total |
| **Timeliness** | Data arrives on time | Latest record < 1 hour ago |
| **Uniqueness** | No duplicates | Count of primary key = count of distinct primary key |
| **Validity** | Values in expected range | Date between 2020-01-01 and today |

### 8.2 Implementing Quality Checks

```python
def quality_gate(df, table_name):
    checks = {}
    
    # Completeness
    total = df.count()
    null_id = df.filter(col("id").isNull()).count()
    checks["null_id_rate"] = null_id / total if total > 0 else 0
    
    # Uniqueness
    distinct_ids = df.select("id").distinct().count()
    checks["duplicate_rate"] = 1 - (distinct_ids / total) if total > 0 else 0
    
    # Validity
    invalid_dates = df.filter(col("date") > current_date()).count()
    checks["future_dates"] = invalid_dates
    
    # Volume
    checks["row_count"] = total
    yesterday_count = get_yesterday_count(table_name)
    checks["volume_change_pct"] = abs(total - yesterday_count) / yesterday_count * 100
    
    # Evaluate
    failures = []
    if checks["null_id_rate"] > 0.01:
        failures.append(f"NULL ID rate {checks['null_id_rate']:.2%} exceeds 1%")
    if checks["duplicate_rate"] > 0.001:
        failures.append(f"Duplicate rate {checks['duplicate_rate']:.4%} exceeds 0.1%")
    if checks["volume_change_pct"] > 50:
        failures.append(f"Volume changed {checks['volume_change_pct']:.1f}% vs yesterday")
    
    if failures:
        send_alert(table_name, failures)
        raise DataQualityException(failures)
    
    return df
```

### 8.3 Great Expectations Pattern

```python
# Define expectations (similar to Great Expectations library)
expectations = {
    "expect_column_values_to_not_be_null": ["id", "timestamp"],
    "expect_column_values_to_be_between": {"amount": (0, 1000000)},
    "expect_column_values_to_be_in_set": {"status": ["active", "inactive", "pending"]},
    "expect_table_row_count_to_be_between": (1000, 10000000),
}
```

---

## 9. Pipeline Orchestration

### 9.1 Apache Airflow Concepts

Airflow is the most common orchestration tool for data pipelines:

```
DAG (Directed Acyclic Graph)
├── Task: extract_orders (BashOperator: spark-submit extract.py)
├── Task: extract_customers (BashOperator: spark-submit extract.py)
├── Task: transform (BashOperator: spark-submit transform.py) [depends on both extracts]
├── Task: quality_check (PythonOperator: run_quality_checks)
└── Task: load_warehouse (BashOperator: spark-submit load.py) [depends on quality_check]
```

Key concepts:
- **DAG**: The pipeline definition (tasks + dependencies)
- **Task**: A single unit of work (run a script, execute SQL, call API)
- **Operator**: The type of task (BashOperator, PythonOperator, SparkSubmitOperator)
- **Schedule**: When the DAG runs (cron expression: `0 6 * * *` = daily at 6 AM)
- **Sensor**: Waits for an external condition (file exists, partition available)
- **XCom**: Cross-task communication (pass small data between tasks)
- **Retry**: Automatic task retry on failure (with backoff)

### 9.2 DAG Design Principles

```
1. Idempotent tasks: re-running produces the same result
2. Atomic tasks: each task either fully succeeds or fully fails
3. No side effects between tasks: tasks communicate via XCom or shared storage
4. Fail fast: quality checks early in the pipeline
5. Parameterized: pass execution date as parameter, don't hardcode
```

### 9.3 Handling Dependencies

```python
# Airflow DAG example (conceptual)
with DAG("daily_pipeline", schedule="0 6 * * *") as dag:
    
    extract_orders = SparkSubmitOperator(
        task_id="extract_orders",
        application="extract_orders.py",
        conf={"spark.executor.memory": "8g"}
    )
    
    extract_customers = SparkSubmitOperator(
        task_id="extract_customers",
        application="extract_customers.py"
    )
    
    transform = SparkSubmitOperator(
        task_id="transform",
        application="transform.py"
    )
    
    quality_check = PythonOperator(
        task_id="quality_check",
        python_callable=run_quality_checks
    )
    
    [extract_orders, extract_customers] >> transform >> quality_check
```

### 9.4 Databricks Workflows

```python
# Databricks job with task dependencies
{
    "name": "daily_pipeline",
    "tasks": [
        {"task_key": "bronze", "notebook_task": {"notebook_path": "/pipelines/bronze"}},
        {"task_key": "silver", "notebook_task": {"notebook_path": "/pipelines/silver"},
         "depends_on": [{"task_key": "bronze"}]},
        {"task_key": "gold", "notebook_task": {"notebook_path": "/pipelines/gold"},
         "depends_on": [{"task_key": "silver"}]}
    ],
    "schedule": {"quartz_cron_expression": "0 0 6 * * ?"}
}
```

---

## 10. Streaming ETL

### 10.1 Micro-Batch vs Continuous

**Micro-batch** (Spark Structured Streaming default):
```
Every N seconds (or triggered manually):
1. Discover new data since last batch
2. Process entire micro-batch as a regular Spark job
3. Write results + commit offset

Latency: seconds to minutes
```

**Continuous processing** (Spark experimental):
```
Row-by-row processing with ~1ms latency
Limited operations supported (no aggregations)
```

### 10.2 Structured Streaming Basics

```python
# Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "events") \
    .load()

# Parse JSON
from pyspark.sql.types import StructType, StringType, DoubleType
schema = StructType() \
    .add("device_id", StringType()) \
    .add("timestamp", TimestampType()) \
    .add("value", DoubleType())

events = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Transform
enriched = events.filter(col("value").isNotNull()).withColumn("hour", hour(col("timestamp")))

# Write stream to Delta
enriched.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/checkpoints/events/") \
    .trigger(processingTime="1 minute") \
    .start("/data/bronze/events/")
```

### 10.3 Trigger Modes

| Trigger | Behavior | Use Case |
|---------|----------|----------|
| `processingTime="1 minute"` | Process micro-batch every minute | Near-real-time |
| `availableNow=True` | Process all available data, then stop | Scheduled batch-like streaming |
| `once=True` | Process one micro-batch, then stop (deprecated) | Legacy |
| Continuous (experimental) | Continuous row-by-row | Ultra-low latency |

### 10.4 Watermarks for Late Data

```python
events.withWatermark("timestamp", "1 hour") \
    .groupBy(window("timestamp", "10 minutes"), "device_id") \
    .agg(avg("value").alias("avg_value"))
```

The watermark tells Spark: "data arriving more than 1 hour late can be dropped." This allows Spark to clean up old state.

---

## 11. Medallion Architecture

### 11.1 Overview

The Medallion Architecture organizes data into three layers of increasing quality:

```
Raw Sources → Bronze (Raw) → Silver (Cleaned) → Gold (Aggregated)
              ↑              ↑                  ↑
              "land as-is"   "validate+clean"   "business-ready"
```

### 11.2 Bronze Layer (Raw)

```python
# Ingest raw data exactly as received, add metadata
raw = spark.read.json("/landing/events/")
bronze = raw.withColumn("_ingestion_time", current_timestamp()) \
            .withColumn("_source_file", input_file_name()) \
            .withColumn("_raw_data", to_json(struct("*")))

bronze.write.format("delta").mode("append").save("/data/bronze/events/")
```

Bronze characteristics:
- Raw, untransformed data
- Append-only (never delete or update)
- Schema may vary (use rescue data column for malformed records)
- Metadata columns: ingestion time, source file, batch ID

### 11.3 Silver Layer (Cleaned)

```python
bronze = spark.read.format("delta").load("/data/bronze/events/")

silver = bronze \
    .filter(col("device_id").isNotNull()) \
    .dropDuplicates(["device_id", "timestamp"]) \
    .withColumn("value", col("value").cast("double")) \
    .withColumn("date", to_date(col("timestamp"))) \
    .select("device_id", "timestamp", "date", "value")

silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", True) \
    .partitionBy("date") \
    .save("/data/silver/events/")
```

Silver characteristics:
- Cleaned, validated, deduplicated
- Consistent schema (enforced types, renamed columns)
- Joins with reference data (enrich with device metadata)
- Partition by commonly-filtered column (date)

### 11.4 Gold Layer (Aggregated)

```python
silver = spark.read.format("delta").load("/data/silver/events/")

gold_daily = silver \
    .groupBy("device_id", "date") \
    .agg(
        avg("value").alias("avg_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        count("*").alias("reading_count")
    )

gold_daily.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("date") \
    .save("/data/gold/daily_device_stats/")
```

Gold characteristics:
- Business-ready aggregations
- Optimized for specific use cases (dashboards, reports, APIs)
- Often denormalized (pre-joined, pre-aggregated)
- Typically smaller than Silver

---

## 12. Slowly Changing Dimensions (SCD)

### 12.1 What Are SCDs

Dimension tables change over time (a customer moves to a new city, a product's price changes). SCDs define how to handle these changes.

### 12.2 SCD Type 0: Retain Original

Never update. Keep the original value forever.
```sql
-- Customer's birth date: never changes
INSERT INTO customers (id, name, birth_date) VALUES (1, 'Alice', '1990-01-15');
-- No updates allowed on birth_date
```

### 12.3 SCD Type 1: Overwrite

Replace the old value with the new value. No history.

```sql
-- Customer moved from NYC to LA
UPDATE customers SET city = 'LA' WHERE id = 1;
-- Old value (NYC) is lost
```

```python
# Delta Lake SCD Type 1
target = DeltaTable.forPath(spark, "/data/dim_customers/")
target.alias("t").merge(
    source.alias("s"), "t.id = s.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

### 12.4 SCD Type 2: Add New Row (Historical Tracking)

Add a new row for each change, preserving history:

```
id | name  | city | effective_from | effective_to   | is_current
1  | Alice | NYC  | 2020-01-01    | 2024-06-30      | false
1  | Alice | LA   | 2024-07-01    | 9999-12-31      | true
```

```python
from pyspark.sql.functions import current_date, lit

# Step 1: Identify changed records
changes = source.alias("s").join(target.alias("t"), "id") \
    .filter(col("s.city") != col("t.city")) \
    .filter(col("t.is_current") == True)

# Step 2: Expire old records
target.alias("t").merge(
    changes.alias("c"), "t.id = c.id AND t.is_current = true"
).whenMatchedUpdate(set={
    "is_current": lit(False),
    "effective_to": current_date()
}).execute()

# Step 3: Insert new current records
new_records = changes.select(
    col("s.id"), col("s.name"), col("s.city"),
    current_date().alias("effective_from"),
    lit("9999-12-31").cast("date").alias("effective_to"),
    lit(True).alias("is_current")
)
new_records.write.format("delta").mode("append").save("/data/dim_customers/")

# Step 4: Insert brand new records (not in target at all)
new_customers = source.join(target, "id", "left_anti")
new_customers.withColumn("effective_from", current_date()) \
    .withColumn("effective_to", lit("9999-12-31").cast("date")) \
    .withColumn("is_current", lit(True)) \
    .write.format("delta").mode("append").save("/data/dim_customers/")
```

### 12.5 SCD Type 3: Previous Value Column

Keep only the current and previous value:

```
id | name  | current_city | previous_city | city_changed_date
1  | Alice | LA           | NYC           | 2024-07-01
```

Limited to one level of history.

### 12.6 SCD Type 6: Hybrid (1 + 2 + 3)

Combine all approaches:
```
id | name  | current_city | historical_city | effective_from | effective_to | is_current
1  | Alice | LA           | NYC             | 2020-01-01     | 2024-06-30   | false
1  | Alice | LA           | LA              | 2024-07-01     | 9999-12-31   | true
```

Both rows have `current_city = LA` (Type 1 updated on all rows), but `historical_city` preserves the value at that point in time (Type 2).

---

## 13. Pipeline Testing

### 13.1 Unit Tests

Test individual transformation functions with small datasets:

```python
def test_clean_nulls():
    df = spark.createDataFrame([
        (1, "Alice", 30),
        (2, None, 25),
        (None, "Bob", 35)
    ], ["id", "name", "age"])
    
    result = clean_nulls(df)
    
    assert result.count() == 1  # only row 1 has no nulls
    assert result.collect()[0]["name"] == "Alice"
```

### 13.2 Integration Tests

Test the full pipeline with realistic sample data:

```python
def test_full_pipeline():
    # Setup: write test data
    test_data.write.parquet("/test/input/")
    
    # Run: execute pipeline
    run_pipeline(input="/test/input/", output="/test/output/")
    
    # Verify: check output
    result = spark.read.parquet("/test/output/")
    assert result.count() > 0
    assert "expected_column" in result.columns
    assert result.filter(col("id").isNull()).count() == 0
    
    # Cleanup
    delete("/test/")
```

### 13.3 Data Contract Tests

Verify that the output schema and data quality meet expectations:

```python
def test_output_contract():
    result = spark.read.parquet("/output/table/")
    
    # Schema contract
    assert result.schema == expected_schema
    
    # Not-null contract
    for col_name in ["id", "timestamp", "value"]:
        assert result.filter(col(col_name).isNull()).count() == 0
    
    # Range contract
    assert result.filter(col("value") < 0).count() == 0
    
    # Uniqueness contract
    assert result.count() == result.select("id").distinct().count()
```

---

## 14. Monitoring and Alerting

### 14.1 What to Monitor

| Metric | Why | Alert Threshold |
|--------|-----|----------------|
| **Pipeline duration** | Detect slowdowns | > 2x historical average |
| **Row count** | Detect data loss or explosion | > 50% change from yesterday |
| **Error rate** | Detect data quality issues | > 1% of records |
| **Freshness** | Detect stale data | > 2x expected schedule |
| **Schema changes** | Detect upstream changes | Any change |
| **Null rate** | Detect data quality degradation | > threshold per column |
| **Duplicate rate** | Detect ingestion issues | > 0.1% |

### 14.2 Logging Best Practices

```python
import logging

logger = logging.getLogger("pipeline.daily_aggregation")

def run_pipeline(date):
    logger.info(f"Starting pipeline for date={date}")
    
    df = spark.read.parquet(f"/data/silver/date={date}/")
    input_count = df.count()
    logger.info(f"Read {input_count} rows from silver")
    
    result = transform(df)
    output_count = result.count()
    logger.info(f"Produced {output_count} rows after transformation")
    
    if output_count == 0:
        logger.error(f"Zero output rows for date={date}!")
        raise ValueError("Pipeline produced no output")
    
    result.write.mode("overwrite").parquet(f"/data/gold/date={date}/")
    logger.info(f"Pipeline complete for date={date}: {input_count} in, {output_count} out")
```

---

## 15. Quick-Reference Cheat Sheet

### Pipeline Pattern Selection

```
Small, infrequently changing data? → Full Load (snapshot overwrite)
Large table with timestamp column? → Incremental Load (watermark-based)
Need to capture deletes? → CDC (Debezium + Kafka + MERGE)
No reliable change indicator? → Snapshot Differencing
Real-time requirements? → Structured Streaming
Near-real-time (minutes)? → Streaming with trigger(availableNow=True)
```

### Medallion Architecture

| Layer | Data State | Update Pattern | Example |
|-------|-----------|---------------|---------|
| Bronze | Raw, as-is | Append only | Raw JSON/Parquet from source |
| Silver | Cleaned, validated | Overwrite partitions / MERGE | Deduplicated, typed, enriched |
| Gold | Aggregated, business-ready | Overwrite / Incremental | Daily KPIs, dashboards |

### SCD Type Selection

| Type | History | Complexity | When to Use |
|------|---------|-----------|------------|
| 0 | None (immutable) | Trivial | Birth dates, creation timestamps |
| 1 | None (overwrite) | Low | Don't need history, latest value only |
| 2 | Full history | High | Need to query any point in time |
| 3 | One previous value | Medium | Need current + previous only |

### Idempotency Patterns

```
Append-only table? → Deduplicate on write (MERGE or dropDuplicates)
Partitioned table? → Dynamic partition overwrite
Delta Lake? → MERGE with match condition
Key-value store? → Upsert (INSERT ON CONFLICT UPDATE)
```
