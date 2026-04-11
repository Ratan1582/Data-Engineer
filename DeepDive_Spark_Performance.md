# Spark Performance Tuning -- A Complete Deep Dive

**From Understanding Bottlenecks to Production-Grade Optimization**

---

## Table of Contents

1. [Performance Fundamentals](#1-performance-fundamentals)
2. [Partitioning Deep Dive](#2-partitioning-deep-dive)
3. [Shuffle Internals](#3-shuffle-internals)
4. [Data Skew](#4-data-skew)
5. [Join Optimization](#5-join-optimization)
6. [Caching and Persistence](#6-caching-and-persistence)
7. [Memory Tuning](#7-memory-tuning)
8. [Executor and Core Sizing](#8-executor-and-core-sizing)
9. [Serialization Tuning](#9-serialization-tuning)
10. [I/O Optimization](#10-io-optimization)
11. [Spark Configuration Reference](#11-spark-configuration-reference)
12. [Profiling with Spark UI](#12-profiling-with-spark-ui)
13. [Common Anti-Patterns](#13-common-anti-patterns)
14. [Optimization Case Studies](#14-optimization-case-studies)
15. [Quick-Reference Cheat Sheet](#15-quick-reference-cheat-sheet)

---

## 1. Performance Fundamentals

### 1.1 Where Time Goes

A Spark job's execution time breaks down into:

```
Total Time = Read Time + Compute Time + Shuffle Time + Write Time + Overhead

Read Time:    Reading data from storage (HDFS, S3, ADLS)
Compute Time: Transformations (filter, map, aggregate, join)
Shuffle Time: Moving data between executors (network + disk I/O + serialization)
Write Time:   Writing results to storage
Overhead:     Task scheduling, serialization, GC, speculative execution
```

In most real-world jobs:
- **Shuffle** is the #1 bottleneck (network I/O, disk spill, serialization)
- **I/O** is #2 (reading/writing large datasets)
- **Compute** is rarely the bottleneck (Spark's codegen is fast)

### 1.2 The Spark Execution Model

```
Job → Stages → Tasks

Job:   Triggered by an action (show(), write(), count())
Stage: A sequence of transformations between shuffles
Task:  One partition of one stage, running on one core

Example:
df.read.parquet(...)          ─┐
  .filter(...)                 │  Stage 1: read + filter + partial agg
  .groupBy(...).count()       ─┘  (200 tasks, one per input partition)
                               │  ← SHUFFLE (exchange)
  .show()                     ─┘  Stage 2: final agg + collect
                                   (200 tasks, one per shuffle partition)
```

### 1.3 The Tuning Framework

```
Step 1: MEASURE → Identify the bottleneck (Spark UI)
Step 2: DIAGNOSE → Understand why (shuffle? skew? spill? GC? I/O?)
Step 3: FIX → Apply targeted optimization
Step 4: VERIFY → Check Spark UI again, compare metrics
```

Never optimize blind. Always measure first.

---

## 2. Partitioning Deep Dive

### 2.1 What Is a Partition

A partition is the fundamental unit of parallelism in Spark. One partition = one task = one CPU core processing that chunk of data.

```
DataFrame with 1000 partitions:
[Part 0] [Part 1] [Part 2] ... [Part 999]
   ↓        ↓        ↓              ↓
 Task 0   Task 1   Task 2  ...  Task 999
 Core 0   Core 1   Core 2  ...  (queued)
```

If you have 100 cores, the 1000 tasks run in ~10 waves.

### 2.2 Input Partitions

When reading data, the number of partitions depends on the source:

| Source | Partitions |
|--------|-----------|
| Parquet/ORC | One per file (or per row group for large files) |
| CSV/JSON | One per block (128 MB default on HDFS) |
| JDBC | Controlled by `numPartitions`, `partitionColumn`, `lowerBound`, `upperBound` |
| Kafka | One per Kafka partition |
| In-memory (parallelize) | `spark.default.parallelism` |

### 2.3 Shuffle Partitions

After a shuffle (groupBy, join, repartition), the number of partitions is controlled by:
```python
spark.conf.set("spark.sql.shuffle.partitions", 200)  # default: 200
```

**200 is almost always wrong**:
- For small data (< 1 GB): 200 partitions = 200 tiny tasks = scheduling overhead
- For large data (1 TB): 200 partitions = 5 GB per partition = OOM or slow

**Rule of thumb**: Target **128 MB - 256 MB per partition** after shuffle.

```python
# For 100 GB of data after shuffle:
# 100 GB / 200 MB = 500 partitions
spark.conf.set("spark.sql.shuffle.partitions", 500)

# Or let AQE handle it (recommended):
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
```

### 2.4 repartition() vs coalesce()

**repartition(n)**: Creates exactly n partitions via a **full shuffle**.
```python
df.repartition(100)            # random distribution into 100 partitions
df.repartition(100, "key")     # hash partition by 'key' into 100 partitions
df.repartition("key")          # hash partition by 'key', default partition count
```

**coalesce(n)**: Reduces partitions **without a full shuffle** by merging adjacent partitions.
```python
df.coalesce(10)  # merge partitions locally, no network transfer
```

| Operation | Can Increase? | Can Decrease? | Shuffle? | Data Balance |
|-----------|--------------|--------------|----------|-------------|
| repartition(n) | Yes | Yes | Yes (full) | Even |
| coalesce(n) | No | Yes | No (local merge) | Uneven (some partitions larger) |

**When to use which**:
```
Need to INCREASE partitions → repartition()
Need to DECREASE partitions → coalesce() (if data balance is acceptable)
Need even distribution → repartition()
Writing fewer output files → coalesce()
Before a join on the same key → repartition() by join key
```

### 2.5 Partition Sizing

```
Too many partitions (thousands of tiny partitions):
→ Task scheduling overhead (each task has ~1-10ms overhead)
→ Small files problem on write
→ Driver bottleneck managing tasks

Too few partitions (a handful of huge partitions):
→ Long task execution time
→ Memory pressure (OOM)
→ Poor utilization (most cores idle, waiting for big tasks)
→ Spill to disk

Sweet spot: 128 MB - 256 MB per partition
Guideline: 2-4 partitions per available core
```

### 2.6 Repartition for Write Optimization

```python
# Bad: writing 10,000 tiny files
df.write.parquet("/output/")

# Good: write 100 reasonably-sized files
df.coalesce(100).write.parquet("/output/")

# Best: partition by key AND control file count
df.repartition(100, "date").write.partitionBy("date").parquet("/output/")
```

---

## 3. Shuffle Internals

### 3.1 What Is a Shuffle

A shuffle redistributes data across partitions based on a key. It is triggered by:
- `groupBy()` + aggregation
- `join()` (except broadcast)
- `repartition()`
- `distinct()`
- `orderBy()` (global sort)

### 3.2 Shuffle Process

```
Map Side (Stage N)                              Reduce Side (Stage N+1)
+----------+                                    +----------+
|Partition 0|→ [Sort by key] → Write to         |Partition 0|← Fetch from
|           |    local disk     shuffle files    |           |   all map tasks
+----------+                                    +----------+
|Partition 1|→ [Sort by key] → Write to         |Partition 1|← Fetch from
|           |    local disk     shuffle files    |           |   all map tasks
+----------+                                    +----------+
```

Detailed steps:
1. **Map side**: Each task processes its partition, computes the target partition for each record (hash(key) % numPartitions), sorts records by target partition, writes to local disk
2. **Shuffle service**: External shuffle service (or the executor itself) serves shuffle files to requesting tasks
3. **Reduce side**: Each task fetches its partition's data from ALL map tasks across the network, merges, and processes

### 3.3 Shuffle File Format

Each map task writes one shuffle file with an index:
```
shuffle_0_0_0.data    ← actual data (sorted by partition ID)
shuffle_0_0_0.index   ← offset of each partition within the data file
```

The reduce task reads only its partition's offset range from each map task's file.

### 3.4 Shuffle Spill

If the in-memory shuffle buffer exceeds available memory, Spark **spills** to disk:
```
Memory buffer (e.g., 64 MB) fills up
→ Sort buffer contents
→ Write sorted chunk to disk (spill file)
→ Reset memory buffer
→ Repeat until all data is processed
→ Merge all spill files + remaining memory data
```

Spill is expensive: writes + reads from local disk + merge step. Reduce spill by:
- Increasing executor memory
- Increasing `spark.shuffle.spill.compress` (enabled by default)
- Reducing partition skew (even out data distribution)

### 3.5 Shuffle Metrics in Spark UI

| Metric | Meaning | Concern Threshold |
|--------|---------|-------------------|
| Shuffle Write | Data written by map tasks | > 10 GB per stage |
| Shuffle Read | Data read by reduce tasks | > 10 GB per stage |
| Spill (Memory) | Data spilled from memory | Any spill is a warning |
| Spill (Disk) | Data written to disk during spill | Definitely a problem |
| Fetch Wait Time | Time waiting for shuffle data | > 10% of task time |

### 3.6 Minimizing Shuffle

```python
# 1. Use broadcast joins to eliminate shuffle of large table
large_df.join(broadcast(small_df), "key")

# 2. Filter before groupBy/join to reduce data shuffled
df.filter(col("active") == True).groupBy("dept").count()
# vs
df.groupBy("dept").count().filter(...)  # shuffles inactive records too

# 3. Use coalesce instead of repartition when reducing partitions
df.coalesce(10)  # no shuffle
# vs
df.repartition(10)  # full shuffle

# 4. Pre-partition data on disk by join key
df.write.bucketBy(256, "user_id").sortBy("user_id").saveAsTable("events_bucketed")
# Future joins on user_id avoid shuffle
```

---

## 4. Data Skew

### 4.1 What Is Data Skew

Data skew means data is unevenly distributed across partitions:

```
Balanced:  [100MB] [100MB] [100MB] [100MB] [100MB]  ← all tasks finish ~same time
Skewed:    [100MB] [100MB] [100MB] [100MB] [5 GB]   ← last task takes 50x longer
```

Skew happens when a join or groupBy key has highly uneven cardinality:
- A "null" key with millions of records
- A popular customer with 10x more transactions than average
- A default/unknown category

### 4.2 Detecting Skew

**Spark UI**: In the Stages tab, look at the task metrics summary:
```
Min task duration: 5 seconds
25th percentile:   8 seconds
Median:            10 seconds
75th percentile:   12 seconds
Max task duration:  5 MINUTES  ← skew! Max >> Median
```

Also check:
- **Shuffle Read Size**: If max >> median, one partition has much more data
- **Spill**: Skewed partitions often spill to disk

**Programmatically**:
```python
# Check partition sizes
df.groupBy(spark_partition_id()).count().orderBy(col("count").desc()).show()

# Check key distribution
df.groupBy("join_key").count().orderBy(col("count").desc()).show(20)
```

### 4.3 Solution 1: Salting

Add a random salt to the skewed key to spread data across partitions:

```python
from pyspark.sql.functions import rand, floor, concat, lit

num_salts = 10

# Salt the skewed table
df_salted = df.withColumn("salt", floor(rand() * num_salts).cast("string"))
df_salted = df_salted.withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))

# Explode the lookup table to match all salt values
from pyspark.sql.functions import explode, array
salt_values = [lit(str(i)) for i in range(num_salts)]
lookup_exploded = lookup_df.withColumn("salt", explode(array(*salt_values)))
lookup_exploded = lookup_exploded.withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))

# Join on salted key -- data is now spread across 10x more partitions
result = df_salted.join(lookup_exploded, "salted_key")
result = result.drop("salt", "salted_key")
```

### 4.4 Solution 2: AQE Skew Join

Let Spark handle it automatically (Spark 3.0+):

```python
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 5)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

AQE detects skewed partitions at runtime and splits them into smaller sub-partitions, duplicating the matching data from the other table.

### 4.5 Solution 3: Broadcast the Smaller Side

If one table is small enough to broadcast, skew doesn't matter:

```python
result = skewed_df.join(broadcast(small_df), "key")
```

### 4.6 Solution 4: Isolate and Handle Separately

```python
# Separate skewed keys (e.g., NULL or known hot keys)
skewed = df.filter(col("key").isNull() | col("key").isin(["HOT_KEY_1", "HOT_KEY_2"]))
normal = df.filter(col("key").isNotNull() & ~col("key").isin(["HOT_KEY_1", "HOT_KEY_2"]))

# Process normal data with standard join
result_normal = normal.join(lookup, "key")

# Process skewed data with broadcast join
result_skewed = skewed.join(broadcast(lookup), "key")

# Combine
result = result_normal.union(result_skewed)
```

---

## 5. Join Optimization

### 5.1 Broadcast Join Tuning

```python
# Increase broadcast threshold (default 10 MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)  # 100 MB

# Force broadcast with hint
result = large.join(broadcast(medium), "key")

# Disable broadcast entirely (for debugging)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
```

**Broadcast memory calculation**: The broadcast table must fit in **driver memory** (for collection) and **executor memory** (for the hash table). A 100 MB table in storage may expand to 300+ MB in memory (Java object overhead).

### 5.2 Sort-Merge Join Tuning

For large-large joins, Sort-Merge is the default. Optimize by:

```python
# 1. Pre-sort data on join key (avoid sort at join time)
df.write.bucketBy(256, "key").sortBy("key").saveAsTable("bucketed_table")

# 2. Increase shuffle partitions for large joins
spark.conf.set("spark.sql.shuffle.partitions", 1000)

# 3. Enable/disable sort-merge preference
spark.conf.set("spark.sql.join.preferSortMergeJoin", True)  # default
```

### 5.3 Bucketed Joins

Pre-partition data on disk to avoid shuffle at join time:

```python
# Write both tables bucketed by the same key, same bucket count
df_orders.write.bucketBy(256, "customer_id").sortBy("customer_id").saveAsTable("orders_bucketed")
df_customers.write.bucketBy(256, "customer_id").sortBy("customer_id").saveAsTable("customers_bucketed")

# Join: NO SHUFFLE needed (both tables already partitioned the same way)
orders = spark.table("orders_bucketed")
customers = spark.table("customers_bucketed")
result = orders.join(customers, "customer_id")
```

### 5.4 Join Filter Pushdown

```python
# Spark automatically pushes filters before joins
df1.join(df2, "key").filter(col("date") > "2024-01-01")
# Optimizer rewrites to:
df1.filter(col("date") > "2024-01-01").join(df2, "key")
# Less data shuffled for the join
```

---

## 6. Caching and Persistence

### 6.1 When to Cache

Cache when a DataFrame is **reused multiple times** in the same job:

```python
# GOOD: df is used twice
df = spark.read.parquet("/data/").filter(col("active") == True)
df.cache()

count = df.count()           # first action: reads from storage, caches in memory
df.groupBy("dept").count()   # second action: reads from cache (fast)
```

### 6.2 Storage Levels

```python
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)          # default: in-memory as Java objects
df.persist(StorageLevel.MEMORY_AND_DISK)       # spill to disk if doesn't fit in memory
df.persist(StorageLevel.MEMORY_ONLY_SER)       # serialized in memory (smaller, slower access)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)   # serialized, spill to disk
df.persist(StorageLevel.DISK_ONLY)             # only on disk (rare)
df.persist(StorageLevel.OFF_HEAP)              # off-heap memory (Tungsten)
```

| Level | Memory | Disk | Serialized | CPU Cost | Space |
|-------|--------|------|-----------|----------|-------|
| MEMORY_ONLY | Yes | No | No | Low | High |
| MEMORY_AND_DISK | Yes | Spillover | No | Low | High |
| MEMORY_ONLY_SER | Yes | No | Yes | Medium | Medium |
| MEMORY_AND_DISK_SER | Yes | Spillover | Yes | Medium | Medium |
| DISK_ONLY | No | Yes | Yes | High | Low |

### 6.3 When NOT to Cache

```python
# BAD: caching data used only once
df.cache()
df.write.parquet("/output/")  # only one action -- cache was useless

# BAD: caching very large data that doesn't fit in memory
# This evicts other cached data and causes spill
huge_df.cache()

# BAD: caching when the source is fast (e.g., already in Delta cache)
# Delta Lake in Databricks has its own caching layer

# BAD: caching before a shuffle (the shuffle writes to disk anyway)
df.cache()
df.groupBy("key").count()  # shuffle reads from disk, not from cache
```

### 6.4 Unpersist

Always unpersist when done:
```python
df.cache()
# ... use df multiple times ...
df.unpersist()  # free the memory
```

Spark may also auto-evict cached data using LRU (Least Recently Used) when memory is needed.

### 6.5 Checkpoint vs Cache

```python
# Cache: stores in memory/disk, recomputed if lost (lineage preserved)
df.cache()

# Checkpoint: writes to reliable storage (HDFS), truncates lineage
spark.sparkContext.setCheckpointDir("/tmp/checkpoints/")
df.checkpoint()
```

Use checkpoint when:
- The lineage (computation graph) is very deep (100+ stages) and you want to prevent stack overflow on recomputation
- You need fault tolerance beyond memory loss

---

## 7. Memory Tuning

### 7.1 Executor Memory Layout

```
Total Executor Memory (e.g., 8 GB)
├── JVM Heap (spark.executor.memory = 6 GB)
│   ├── Spark Memory (spark.memory.fraction = 0.6 → 3.6 GB)
│   │   ├── Execution Memory (shared, dynamic)
│   │   │   Used for: shuffle buffers, join hash tables, sort buffers, aggregation
│   │   └── Storage Memory (shared, dynamic)
│   │       Used for: cached DataFrames, broadcast variables
│   └── User Memory (1 - 0.6 = 0.4 → 2.4 GB)
│       Used for: user data structures, UDF objects, metadata
├── Memory Overhead (spark.executor.memoryOverhead = ~10% = 800 MB)
│   Used for: JVM internals, thread stacks, native memory, Python workers
└── Off-Heap (spark.memory.offHeap.size, if enabled)
    Used for: Tungsten off-heap storage
```

### 7.2 Unified Memory Model

Since Spark 1.6, execution and storage share a pool:

```
Spark Memory Pool (3.6 GB in example above)
+--------------------------------------+
|  Execution Memory  | Storage Memory   |
|  (grows as needed) | (grows as needed)|
+--------------------------------------+
         ↕ boundary moves dynamically ↕
```

Rules:
- Execution can **borrow** from Storage if Storage has spare space
- Storage can **borrow** from Execution if Execution has spare space
- Execution can **evict** Storage (cached data) if it needs more space
- Storage **cannot** evict Execution (to avoid killing running tasks)

### 7.3 Common Memory Issues

**OOM (Out of Memory)**:
```
java.lang.OutOfMemoryError: Java heap space
# → Executor ran out of heap memory

java.lang.OutOfMemoryError: GC overhead limit exceeded
# → Executor spending >98% of time in garbage collection

ERROR: Container killed by YARN for exceeding memory limits
# → Total memory (heap + overhead) exceeded YARN container limit
```

**Solutions**:
```python
# Increase executor memory
spark.conf.set("spark.executor.memory", "8g")

# Increase memory overhead (for Python UDFs, off-heap allocations)
spark.conf.set("spark.executor.memoryOverhead", "2g")

# Increase the fraction used by Spark (less user memory)
spark.conf.set("spark.memory.fraction", 0.8)

# Increase partitions to reduce per-task data
spark.conf.set("spark.sql.shuffle.partitions", 500)

# Use off-heap memory
spark.conf.set("spark.memory.offHeap.enabled", True)
spark.conf.set("spark.memory.offHeap.size", "4g")
```

### 7.4 GC Tuning

```python
# Use G1GC (recommended for Spark)
spark.conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:G1HeapRegionSize=16m")

# Monitor GC in Spark UI: Executors tab → GC Time column
# If GC Time > 10% of task time, you have a GC problem
```

---

## 8. Executor and Core Sizing

### 8.1 The Fat vs Thin Executor Debate

**Thin executors** (many executors, few cores each):
```
20 executors × 2 cores × 4 GB = 40 cores, 80 GB
```
Pros: Better fault tolerance (losing one executor loses less work), more parallelism
Cons: Higher overhead (20 JVMs), less sharing of broadcast variables

**Fat executors** (few executors, many cores each):
```
4 executors × 10 cores × 20 GB = 40 cores, 80 GB
```
Pros: Better memory sharing, fewer JVMs, broadcast shared across more tasks
Cons: GC pressure (large heaps), HDFS write contention (each executor has N output writers)

### 8.2 Recommended Configuration

**General guideline**: 5 cores per executor is a good balance.

```
YARN node: 32 cores, 128 GB RAM
Reserved for OS: 1 core, 8 GB
Available: 31 cores, 120 GB

Executors per node: 31 / 5 = 6 executors (5 cores each, 1 core left for overhead)
Memory per executor: 120 / 6 = 20 GB
  → spark.executor.memory = 17 GB (85% for heap)
  → spark.executor.memoryOverhead = 3 GB (15% for overhead)
```

**Why 5 cores?**
- More than 5 cores: HDFS throughput drops (concurrent writers contend)
- Fewer than 3 cores: too many small executors, high overhead

### 8.3 Dynamic Allocation

Let Spark auto-scale executors based on workload:

```python
spark.conf.set("spark.dynamicAllocation.enabled", True)
spark.conf.set("spark.dynamicAllocation.minExecutors", 2)
spark.conf.set("spark.dynamicAllocation.maxExecutors", 100)
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")
```

How it works:
- **Scale up**: If there are pending tasks, add executors
- **Scale down**: If executors are idle for 60s, remove them
- Requires an external shuffle service (so shuffle data survives executor removal)

### 8.4 Driver Sizing

The driver needs memory for:
- `collect()` results
- Broadcast variables (collected from executors)
- Job metadata, lineage graph
- Scheduling overhead

```python
spark.conf.set("spark.driver.memory", "4g")    # increase for large collects or broadcasts
spark.conf.set("spark.driver.maxResultSize", "2g")  # max size of result sent to driver
```

---

## 9. Serialization Tuning

### 9.1 Java Serialization vs Kryo

| Aspect | Java Serialization | Kryo |
|--------|-------------------|------|
| Speed | Slow | 2-10x faster |
| Size | Large | 2-5x smaller |
| Compatibility | Works with all Java objects | Requires registration for best performance |
| Default | Yes (for RDDs) | No |

```python
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.kryo.registrationRequired", False)  # True = fail on unregistered classes
```

Note: For DataFrame/SQL operations, Spark uses **Tungsten's UnsafeRow** serialization (binary format), not Java or Kryo. Java/Kryo serialization applies to:
- RDD operations
- Broadcast variables
- Closure serialization (sending functions to executors)

### 9.2 Apache Arrow (for Python)

For Pandas UDFs, Arrow serialization is critical:

```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", True)
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", True)  # fallback to pickle if Arrow fails
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 10000)  # batch size for Arrow transfer
```

---

## 10. I/O Optimization

### 10.1 File Format Selection

| Format | Read Speed | Write Speed | Compression | Column Pruning | Predicate Pushdown |
|--------|-----------|-------------|------------|---------------|-------------------|
| Parquet | Fastest | Medium | Excellent | Yes | Yes (row group stats) |
| ORC | Fast | Medium | Excellent | Yes | Yes (stripe stats + bloom) |
| Avro | Medium | Fast | Good | No | No |
| CSV | Slow | Fast | Poor | No | No |
| JSON | Slowest | Fast | Poor | No | No |

### 10.2 Parquet Tuning

```python
# Row group size (larger = better compression, but more memory during write)
spark.conf.set("spark.sql.parquet.int96RebaseMode", "CORRECTED")
spark.conf.set("parquet.block.size", str(128 * 1024 * 1024))  # 128 MB

# Enable vectorized reader (columnar batch processing)
spark.conf.set("spark.sql.parquet.enableVectorizedReader", True)

# Compression
df.write.option("compression", "snappy").parquet("/output/")  # fast, default
df.write.option("compression", "zstd").parquet("/output/")    # best ratio
```

### 10.3 Partition Pruning

```python
# Write with partition columns
df.write.partitionBy("year", "month").parquet("/output/")

# Read with filter -- only reads matching partitions
spark.read.parquet("/output/").filter(col("year") == 2024)
```

**Don't over-partition**: If partition values have high cardinality (e.g., `device_id` with 1M values), you create 1M directories with tiny files. Partition by **low-cardinality, frequently-filtered** columns (date, region, category).

### 10.4 Small File Merging

```python
# After many small appends, compact:
df = spark.read.parquet("/data/table/")
df.coalesce(100).write.mode("overwrite").parquet("/data/table_compacted/")

# With Delta Lake:
# OPTIMIZE handles compaction automatically
spark.sql("OPTIMIZE delta.`/data/table/`")
```

### 10.5 Write Performance

```python
# Control output files
df.repartition(100).write.parquet("/output/")  # exactly 100 files (shuffle cost)
df.coalesce(100).write.parquet("/output/")     # ~100 files (no shuffle, uneven)

# Dynamic partition overwrite (replace only affected partitions)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("date").parquet("/output/")
```

---

## 11. Spark Configuration Reference

### 11.1 Most Important Configs

| Config | Default | What It Does | When to Change |
|--------|---------|-------------|---------------|
| `spark.executor.memory` | 1g | JVM heap per executor | Always increase for real workloads |
| `spark.executor.cores` | 1 | CPU cores per executor | Set to 3-5 |
| `spark.executor.memoryOverhead` | 10% of executor memory | Non-heap memory (native, Python) | Increase for Python UDFs |
| `spark.sql.shuffle.partitions` | 200 | Partitions after shuffle | Tune based on data size |
| `spark.sql.autoBroadcastJoinThreshold` | 10 MB | Max broadcast table size | Increase for medium tables |
| `spark.sql.adaptive.enabled` | true (3.2+) | Enable AQE | Keep enabled |
| `spark.default.parallelism` | total cores | Default RDD partitions | Set to 2-3x total cores |
| `spark.driver.memory` | 1g | Driver JVM heap | Increase for large collects |
| `spark.serializer` | Java | Serializer class | Set to Kryo |
| `spark.dynamicAllocation.enabled` | false | Auto-scale executors | Enable for varying workloads |
| `spark.memory.fraction` | 0.6 | Fraction of heap for Spark | Increase if tasks spill |
| `spark.sql.files.maxPartitionBytes` | 128 MB | Max size of file partition | Tune for file reading |
| `spark.speculation` | false | Speculative execution | Enable for stragglers |

---

## 12. Profiling with Spark UI

### 12.1 Jobs Tab

Shows all jobs triggered by actions. Each job corresponds to one action call.
- **Job duration**: Total wall-clock time
- **Stages**: Number of stages (each shuffle boundary creates a new stage)
- **Tasks**: Total tasks across all stages

### 12.2 Stages Tab

The most important tab for performance tuning.

**Stage details** show:
- **Duration**: Wall-clock time for the stage
- **Input size**: Data read from storage
- **Output size**: Data written to storage
- **Shuffle Read/Write**: Data moved during shuffle
- **Spill (Memory/Disk)**: Data spilled from memory to disk

**Task metrics summary** (key diagnostic tool):
```
Metric         Min    25%    Median   75%    Max
Duration       0.1s   0.5s   0.8s    1.2s   45s   ← Max >> Median = SKEW
GC Time        0s     0.01s  0.02s   0.05s  5s    ← High GC = memory pressure
Shuffle Read   1MB    10MB   15MB    20MB   2GB   ← Max >> Median = SKEW
Spill Memory   0      0      0       0      500MB ← Any spill = needs more memory
```

### 12.3 Storage Tab

Shows cached/persisted DataFrames:
- **RDD Name**: What was cached
- **Storage Level**: MEMORY_ONLY, MEMORY_AND_DISK, etc.
- **Size in Memory/Disk**: How much space it occupies
- **Fraction Cached**: What percentage actually fits in memory

### 12.4 SQL Tab

Shows the DAG for SQL/DataFrame operations:
- Visual representation of the physical plan
- Per-operator metrics (rows processed, time, data size)
- Identify which operator is the bottleneck

### 12.5 Environment Tab

Shows all Spark configurations. Verify your settings are actually applied.

### 12.6 Reading the DAG Visualization

```
WholeStageCodegen (1)
├── FileScan parquet [name, age, dept] → 10M rows, 500 MB
├── Filter (age > 30) → 3M rows
└── HashAggregate (partial) → 50K rows

Exchange hashpartitioning(dept, 200) → 50K rows, 5 MB  ← SHUFFLE

WholeStageCodegen (2)
└── HashAggregate (final) → 20 rows
```

Key metrics to look for:
- **Rows in/out per operator**: Identify where data is filtered/expanded
- **Shuffle bytes**: How much data crosses the network
- **Time per operator**: Where is the time spent

---

## 13. Common Anti-Patterns

### 13.1 collect() on Large Data

```python
# BAD: brings entire dataset to driver memory
all_data = df.collect()  # OOM if df is large

# GOOD: process in Spark, only collect aggregates
count = df.count()
top_10 = df.orderBy(col("value").desc()).limit(10).collect()
df.write.parquet("/output/")  # write to storage, not to driver
```

### 13.2 Python UDF Instead of Built-in Functions

```python
# BAD: Python UDF (serialization overhead, no pushdown)
@udf(StringType())
def upper_case(s):
    return s.upper() if s else None

df.withColumn("name_upper", upper_case(col("name")))

# GOOD: Built-in function (runs in JVM, pushdown enabled)
from pyspark.sql.functions import upper
df.withColumn("name_upper", upper(col("name")))
```

### 13.3 Repeated Reading

```python
# BAD: reads the same data 3 times
df1 = spark.read.parquet("/data/")
count = df1.count()
df2 = spark.read.parquet("/data/")  # reads again!
avg_val = df2.agg(avg("value")).collect()
df3 = spark.read.parquet("/data/")  # reads a third time!
df3.write.parquet("/output/")

# GOOD: read once, cache if reused
df = spark.read.parquet("/data/").cache()
count = df.count()
avg_val = df.agg(avg("value")).collect()
df.write.parquet("/output/")
df.unpersist()
```

### 13.4 Cartesian Join

```python
# BAD: missing join condition -- O(N × M) rows
df1.crossJoin(df2)  # 1M × 1M = 1 TRILLION rows

# Only acceptable for small tables:
dates.crossJoin(categories)  # 365 × 10 = 3,650 rows (fine)
```

### 13.5 Using count() for Existence Check

```python
# BAD: counts ALL matching rows
if df.filter(col("status") == "error").count() > 0:
    handle_errors()

# GOOD: stops at first match
if len(df.filter(col("status") == "error").head(1)) > 0:
    handle_errors()

# Or (Spark 3.3+):
if not df.filter(col("status") == "error").isEmpty():
    handle_errors()
```

### 13.6 select(*) on Wide Tables

```python
# BAD: reads all 200 columns from Parquet
result = spark.read.parquet("/wide_table/").filter(col("id") == 123)

# GOOD: only reads needed columns
result = spark.read.parquet("/wide_table/").select("id", "name", "email").filter(col("id") == 123)
```

### 13.7 orderBy() Without Limit

```python
# BAD: global sort of entire dataset (huge shuffle)
df.orderBy("timestamp").write.parquet("/output/")

# GOOD: if you only need top-N
df.orderBy(col("timestamp").desc()).limit(1000)

# GOOD: if you need sorted output, sort within partitions
df.sortWithinPartitions("timestamp").write.parquet("/output/")
```

---

## 14. Optimization Case Studies

### 14.1 Case: Slow Join on Skewed Key

**Problem**: A join between `orders` (500M rows) and `customers` (1M rows) takes 2 hours. Spark UI shows one task taking 90 minutes while others finish in 2 minutes.

**Diagnosis**: The `customer_id` column has a "guest" customer with 50M orders (10% of all orders).

**Solution**:
```python
# Broadcast the small table (customers is 1M rows, ~200 MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 300 * 1024 * 1024)
result = orders.join(broadcast(customers), "customer_id")
# Result: 3 minutes (no shuffle, no skew issue)
```

### 14.2 Case: OOM During Aggregation

**Problem**: `df.groupBy("device_id").agg(collect_list("readings"))` crashes with OOM.

**Diagnosis**: Some devices have millions of readings. `collect_list` tries to build an array of millions of values in one executor's memory.

**Solution**:
```python
# Option 1: Limit the list size
from pyspark.sql.functions import slice
df.groupBy("device_id").agg(slice(collect_list("readings"), 1, 1000).alias("readings"))

# Option 2: Write as separate rows instead of collecting
df.write.partitionBy("device_id").parquet("/output/")

# Option 3: Increase executor memory and partitions
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.sql.shuffle.partitions", 2000)
```

### 14.3 Case: Too Many Small Files

**Problem**: A daily pipeline appends to a Parquet table. After 6 months, the table has 500K files (~1 MB each). Queries take 30 minutes.

**Diagnosis**: 500K files means 500K tasks for any read. Most time is spent in task scheduling and file listing, not processing.

**Solution**:
```python
# One-time compaction
df = spark.read.parquet("/data/table/")
df.repartition(500).write.mode("overwrite").parquet("/data/table_compacted/")

# Prevent future small files: coalesce before write in daily pipeline
daily_df.coalesce(5).write.mode("append").parquet("/data/table/")

# Better: use Delta Lake with OPTIMIZE
spark.sql("OPTIMIZE delta.`/data/table/`")
```

---

## 15. Quick-Reference Cheat Sheet

### Partition Sizing
```
Target: 128 MB - 256 MB per partition
Formula: num_partitions = total_data_size / target_partition_size
Example: 100 GB / 200 MB = 500 partitions
```

### Memory Sizing
```
Executor memory = data_per_partition × 3 to 5 (for overhead, hash tables, buffers)
Driver memory = max(collect_size, broadcast_size) + 2 GB overhead
```

### Performance Checklist

```
□ Are shuffle partitions tuned (not default 200)?
□ Is AQE enabled?
□ Are small tables broadcast joined?
□ Is data skew handled (check Spark UI max vs median task)?
□ Is there spill to disk (check stage metrics)?
□ Are partition columns used in filters (partition pruning)?
□ Are only needed columns selected (column pruning)?
□ Is caching used for reused DataFrames (and unpersisted after)?
□ Are Python UDFs replaced with built-in functions?
□ Are output files a reasonable size (not thousands of tiny files)?
```

### Quick Fixes by Symptom

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| One task much slower than others | Data skew | Salting, AQE skew join, broadcast |
| OOM on executor | Large partitions or collect_list | More partitions, more memory, limit results |
| OOM on driver | collect() or large broadcast | Avoid collect(), reduce broadcast size |
| Excessive GC time | Large Java objects in memory | More memory, G1GC, Kryo serialization |
| Slow reads | Too many small files | Compact files, use Delta OPTIMIZE |
| Slow joins | Wrong join strategy | Broadcast small table, check join type in plan |
| Slow writes | Too many output files | coalesce() before write |
| Shuffle spill | Partitions too large | More shuffle partitions, more executor memory |
