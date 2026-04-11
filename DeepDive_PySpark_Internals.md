# PySpark Internals -- A Complete Deep Dive

**From Py4J Bridge to Catalyst Optimizer to Tungsten Execution**

---

## Table of Contents

1. [PySpark Architecture Overview](#1-pyspark-architecture-overview)
2. [The Py4J Bridge](#2-the-py4j-bridge)
3. [How DataFrame API Translates to JVM](#3-how-dataframe-api-translates-to-jvm)
4. [Catalyst Optimizer](#4-catalyst-optimizer)
5. [Tungsten Execution Engine](#5-tungsten-execution-engine)
6. [Execution Plans -- Reading and Understanding](#6-execution-plans----reading-and-understanding)
7. [Predicate Pushdown and Column Pruning](#7-predicate-pushdown-and-column-pruning)
8. [Join Strategy Selection](#8-join-strategy-selection)
9. [Adaptive Query Execution (AQE)](#9-adaptive-query-execution-aqe)
10. [PySpark vs Scala Spark Performance](#10-pyspark-vs-scala-spark-performance)
11. [Quick-Reference Cheat Sheet](#11-quick-reference-cheat-sheet)

---

## 1. PySpark Architecture Overview

### 1.1 The Two-Language Problem

PySpark runs Python code but Spark's core engine is written in **Scala/Java** and runs on the **JVM**. The architecture must bridge two runtimes:

```
Python World (CPython)              JVM World (Scala/Java)
+-------------------+              +---------------------+
| Python Driver     |  <--Py4J-->  | JVM Driver          |
| (your script)     |              | (SparkContext,       |
|                   |              |  Catalyst, Tungsten) |
+-------------------+              +---------------------+
                                            |
                                   (cluster communication)
                                            |
                               +------------+------------+
                               |            |            |
                          +----v---+   +----v---+   +----v---+
                          |Executor|   |Executor|   |Executor|
                          | (JVM)  |   | (JVM)  |   | (JVM)  |
                          |        |   |        |   |        |
                          |Python  |   |Python  |   |Python  |
                          |Worker  |   |Worker  |   |Worker  |
                          |(only if|   |(only if|   |(only if|
                          | UDF)   |   | UDF)   |   | UDF)   |
                          +--------+   +--------+   +--------+
```

**Key insight**: When you use the DataFrame API or Spark SQL (no Python UDFs), **no Python code runs on executors**. Your Python calls are translated to JVM calls via Py4J, and the entire execution happens in the JVM. Python workers on executors are only needed when you use Python UDFs or RDD operations.

### 1.2 What Happens When You Create a SparkSession

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MyApp").getOrCreate()
```

Behind the scenes:
1. Python creates a `SparkSession` Python object
2. This object connects via **Py4J** to a Java `SparkSession` in the JVM
3. The JVM `SparkSession` creates:
   - `SparkContext`: the connection to the cluster manager
   - `SQLContext`: the SQL query engine
   - Catalyst optimizer
   - Tungsten execution engine
4. The cluster manager (YARN, Kubernetes, Standalone) allocates executors
5. Each executor starts a JVM process

The Python driver is essentially a **thin client** that sends instructions to the JVM.

### 1.3 DataFrame Operations Flow

When you write:
```python
df = spark.read.parquet("/data/")
result = df.filter(col("age") > 30).select("name", "age").groupBy("name").count()
result.show()
```

The flow is:
1. `spark.read.parquet(...)` → Py4J call → JVM creates a logical plan for reading Parquet
2. `.filter(col("age") > 30)` → Py4J call → JVM adds a Filter node to the logical plan
3. `.select("name", "age")` → Py4J call → JVM adds a Project node
4. `.groupBy("name").count()` → Py4J call → JVM adds Aggregate node
5. `.show()` → This is an **action**. Now Catalyst optimizes the plan, Tungsten generates code, and executors run it
6. Results return from JVM → Py4J → Python → printed to console

Steps 1-4 are **lazy** -- they just build a plan. No data is read or processed. Step 5 triggers actual execution.

---

## 2. The Py4J Bridge

### 2.1 What Is Py4J

Py4J is a library that enables Python to call Java methods as if they were local Python methods. It works over a **TCP socket connection** between the Python and JVM processes:

```
Python Process          TCP Socket          JVM Process
+-------------+     +-----------+     +--------------+
| Python code | --> | serialize  | --> | deserialize   |
|             |     | method    |     | + call Java   |
|             |     | call      |     | method        |
|             | <-- | serialize  | <-- | serialize     |
|             |     | return    |     | return value  |
+-------------+     +-----------+     +--------------+
```

### 2.2 How Py4J Works in PySpark

When you create a PySpark `DataFrame`, the Python object holds a **reference** to a Java object:

```python
# Python side
df = spark.read.parquet("/data/")
# df._jdf is the Py4J reference to the Java DataFrame object

# When you call:
df.filter(col("age") > 30)
# Py4J translates this to:
# jvm_df.filter(jvm_column_expression)
```

Each Python DataFrame method:
1. Constructs the equivalent Java/Scala expression
2. Sends it over the Py4J socket to the JVM
3. Receives a reference to the result Java object
4. Wraps it in a new Python DataFrame object

### 2.3 Py4J Overhead

Each Py4J call has overhead:
- TCP socket communication (even on localhost): ~0.1-1ms per call
- Serialization/deserialization of arguments and return values
- Python GIL acquisition/release

For DataFrame operations, this overhead is negligible because:
- Each Python call represents a large logical operation (filter, join, aggregate)
- The actual computation happens entirely in the JVM
- You make tens of Py4J calls, not millions

For RDD operations or Python UDFs, the overhead is significant because:
- Data must be serialized from JVM to Python and back for every partition
- Python processes must be spawned on each executor

### 2.4 The `_jvm` Gateway

PySpark provides direct access to the JVM through `spark._jvm`:

```python
# Access Java classes directly
jvm = spark._jvm

# Call a Java static method
timestamp = jvm.java.lang.System.currentTimeMillis()

# Access Spark internal classes
hadoop_conf = spark._jsc.hadoopConfiguration()
```

This is rarely needed in application code but useful for understanding how PySpark works internally.

---

## 3. How DataFrame API Translates to JVM

### 3.1 Column Expressions

When you write `col("age") > 30`, PySpark builds a **Column expression tree**:

```python
from pyspark.sql.functions import col

expr = col("age") > 30

# Internally, this creates:
# GreaterThan(UnresolvedAttribute("age"), Literal(30))
```

Each PySpark function creates a corresponding Catalyst expression node:
| PySpark | Catalyst Expression |
|---------|-------------------|
| `col("age")` | `UnresolvedAttribute("age")` |
| `col("age") > 30` | `GreaterThan(Attribute("age"), Literal(30))` |
| `col("a") + col("b")` | `Add(Attribute("a"), Attribute("b"))` |
| `F.when(cond, val)` | `CaseWhen([(cond, val)])` |
| `F.sum("amount")` | `AggregateExpression(Sum(Attribute("amount")))` |

### 3.2 Logical Plan Construction

Each DataFrame transformation adds a node to the logical plan tree:

```python
df = spark.read.parquet("/data/")
    .filter(col("age") > 30)
    .select("name", "age")
    .groupBy("name").count()
```

Logical plan (read bottom to top):
```
Aggregate [name], [name, count(*)]
└── Project [name, age]
    └── Filter (age > 30)
        └── Relation [parquet: /data/] [id, name, age, city, ...]
```

### 3.3 spark.sql() Path

When you use `spark.sql()`:
```python
result = spark.sql("SELECT name, COUNT(*) FROM people WHERE age > 30 GROUP BY name")
```

1. The SQL string is sent to the JVM via Py4J
2. Spark's SQL parser (ANTLR-based) parses it into an AST
3. The AST is converted to an **unresolved logical plan** (same as DataFrame API)
4. From this point, both paths (SQL and DataFrame API) follow the exact same optimization pipeline

This is why there is **no performance difference** between SQL and DataFrame API -- they produce the same logical plan.

---

## 4. Catalyst Optimizer

### 4.1 What Is Catalyst

Catalyst is Spark's **query optimizer**. It takes a logical plan and transforms it into an optimized physical plan through a series of phases:

```
Unresolved        Analyzed         Optimized        Physical
Logical Plan  →  Logical Plan  →  Logical Plan  →  Plan      →  Execution
(raw plan)       (resolved)       (optimized)      (executable)   (Tungsten)
```

### 4.2 Phase 1: Analysis

The analyzer resolves **names and types**. The unresolved plan has symbolic references ("age" as a string); the analyzer resolves them to actual column references with data types.

```
Before Analysis (Unresolved):
Filter (UnresolvedAttribute("age") > 30)
└── UnresolvedRelation("people")

After Analysis (Resolved):
Filter (Attribute("age", IntegerType) > Literal(30, IntegerType))
└── Relation("people", schema=[id: Long, name: String, age: Int, ...])
```

What the analyzer does:
1. **Resolve relations**: Look up table/view "people" in the catalog
2. **Resolve columns**: Match column names to the table schema
3. **Type checking**: Verify that `age > 30` is valid (both sides numeric)
4. **Implicit casts**: If you compare `int` to `double`, insert a cast
5. **Resolve functions**: Match `count(*)` to the built-in count aggregate
6. **Star expansion**: Replace `SELECT *` with actual column names

Errors at this stage:
```python
# AnalysisException: cannot resolve 'agee' given input columns: [id, name, age, city]
df.filter(col("agee") > 30)  # typo in column name
```

### 4.3 Phase 2: Logical Optimization

The optimizer applies **rule-based transformations** to reduce the cost of the plan. These rules are applied repeatedly until the plan stops changing (fixed-point).

**Key optimization rules**:

**1. Predicate Pushdown**: Move filters as close to the data source as possible.
```
Before:
Join
├── Filter (a.age > 30)
│   └── Scan(table_a)
└── Scan(table_b)

After:
Join
├── Scan(table_a, filter: age > 30)  ← pushed into scan
└── Scan(table_b)
```

**2. Column Pruning**: Remove columns that are never used downstream.
```
Before:
Project [name]
└── Scan(people) [id, name, age, city, email, phone, ...]

After:
Project [name]
└── Scan(people) [name]  ← only reads 'name' column from Parquet
```

**3. Constant Folding**: Evaluate constant expressions at compile time.
```
Before: Filter (1 + 1 > age)
After:  Filter (2 > age)

Before: Filter (true AND age > 30)
After:  Filter (age > 30)
```

**4. Boolean Simplification**:
```
Before: Filter (NOT (NOT (age > 30)))
After:  Filter (age > 30)

Before: Filter (age > 30 OR true)
After:  (no filter needed -- always true)
```

**5. Combine Adjacent Filters**:
```
Before:
Filter (age > 30)
└── Filter (city = "NYC")
    └── Scan(people)

After:
Filter (age > 30 AND city = "NYC")
└── Scan(people)
```

**6. Push Limit Through Union**:
```
Before:
Limit 10
└── Union
    ├── Scan(table_a)
    └── Scan(table_b)

After:
Limit 10
└── Union
    ├── Limit 10 ← pushed down
    │   └── Scan(table_a)
    └── Limit 10 ← pushed down
        └── Scan(table_b)
```

**7. Join Reordering**: Reorder joins to minimize intermediate result sizes (cost-based, requires statistics).

### 4.4 Phase 3: Physical Planning

The optimizer converts the optimized logical plan into one or more **physical plans** and selects the best one using a cost model.

Decisions made at this stage:
- **Join algorithm**: Broadcast Hash Join vs Sort-Merge Join vs Shuffle Hash Join
- **Aggregation strategy**: Hash Aggregate vs Sort Aggregate
- **Scan strategy**: In-memory scan vs file scan with pushdown
- **Shuffle strategy**: Hash partitioning vs range partitioning

```
Logical: Join(a, b, a.id = b.id)

Physical options:
1. BroadcastHashJoin (if b is small enough)
   Cost: no shuffle of a, broadcast b to all partitions
   
2. SortMergeJoin (default for large-large)
   Cost: shuffle both a and b by join key, then sort and merge
   
3. ShuffledHashJoin (one side fits in memory after shuffle)
   Cost: shuffle both, build hash table on smaller side

Selection: if b.sizeInBytes < spark.sql.autoBroadcastJoinThreshold → BroadcastHashJoin
           else → SortMergeJoin (default)
```

### 4.5 Phase 4: Code Generation (Whole-Stage CodeGen)

After selecting the physical plan, Catalyst generates **Java bytecode** at runtime for the entire pipeline stage. This is called **Whole-Stage Code Generation** (WholeStageCodegen).

Without codegen, each operator processes one row at a time through virtual method calls:
```
Row → Filter.process(row) → Project.process(row) → Aggregate.process(row)
     (virtual dispatch)     (virtual dispatch)     (virtual dispatch)
```

With codegen, the entire stage is fused into a single tight loop:
```java
// Generated code (simplified)
while (input.hasNext()) {
    Row row = input.next();
    if (row.getInt(2) > 30) {         // filter: age > 30
        String name = row.getString(1); // project: name
        hashMap.update(name, 1);        // aggregate: count
    }
}
```

Benefits:
- **No virtual dispatch** between operators (everything is inlined)
- **CPU register optimization**: values stay in CPU registers
- **Loop unrolling**: JVM JIT can further optimize the generated code
- **Cache-friendly**: sequential memory access

In the physical plan, you see this as `*(n)` wrapping operators:
```
*(2) HashAggregate(keys=[name], functions=[count(1)])
+- Exchange hashpartitioning(name, 200)
   +- *(1) HashAggregate(keys=[name], functions=[partial_count(1)])
      +- *(1) Project [name]
         +- *(1) Filter (age > 30)
            +- *(1) FileScan parquet [name, age]
```

`*(1)` means operators Filter, Project, and partial HashAggregate are fused into one generated function.

---

## 5. Tungsten Execution Engine

### 5.1 What Is Tungsten

Tungsten is Spark's **execution engine** focused on maximizing CPU and memory efficiency. While Catalyst optimizes the plan (what to compute), Tungsten optimizes the execution (how to compute it efficiently).

### 5.2 Problem: JVM Memory Overhead

Java objects have significant overhead:
- A Java `String` "hello" takes ~56 bytes (object header + char array + length + hash + padding)
- A Spark `Row` with 10 fields could take 400+ bytes in Java objects

For billions of rows, this means:
- Excessive garbage collection (GC pauses)
- Poor CPU cache utilization (objects scattered across heap)
- High memory consumption

### 5.3 UnsafeRow -- Tungsten's Binary Format

Tungsten uses its own binary format called **UnsafeRow** that bypasses Java object creation:

```
UnsafeRow binary layout:
+--------+--------+--------+--------+--------+--------+
| null   | field  | field  | field  | field  | var    |
| bitmap | offset | offset | offset | offset | length |
| (8B)   | (8B)   | (8B)   | (8B)   | (8B)   | data   |
+--------+--------+--------+--------+--------+--------+

A row with (id=42, name="Alice", age=30):
[00000000] [0000002A] [offset→Alice] [0000001E] ["Alice"]
 null bits   id=42      name ptr       age=30     name data
```

Benefits:
- **No Java objects per field**: Fields are stored as raw bytes, not boxed types
- **Contiguous memory**: Entire row in a single byte array (cache-friendly)
- **No GC pressure**: Managed outside Java's garbage collector (off-heap or pooled)
- **Compact**: ~2-4x smaller than Java object representation

### 5.4 Off-Heap Memory Management

Tungsten can allocate memory **outside the JVM heap** using `sun.misc.Unsafe` (or `java.nio.ByteBuffer`):

```
JVM Heap                           Off-Heap (OS memory)
+------------------+               +------------------+
| Java objects     |               | UnsafeRow data   |
| (GC managed)     |               | (manually managed)|
| - Driver objects |               | - Shuffle buffers|
| - Metadata       |               | - Sort buffers   |
| - Small caches   |               | - Join hash maps |
+------------------+               +------------------+
```

Off-heap advantages:
- Not subject to GC (no pause times)
- Directly accessed via memory addresses (fast)
- Can be larger than JVM heap

Controlled by:
```python
spark.conf.set("spark.memory.offHeap.enabled", True)
spark.conf.set("spark.memory.offHeap.size", "4g")
```

### 5.5 Cache-Aware Computation

Tungsten organizes data to maximize **CPU cache hits**:

```
L1 Cache:  ~32 KB,  ~1 ns access    (register-like speed)
L2 Cache:  ~256 KB, ~4 ns access
L3 Cache:  ~8 MB,   ~12 ns access
Main RAM:  ~GB,     ~100 ns access   (~100x slower than L1)
```

By storing data in contiguous byte arrays (UnsafeRow) instead of scattered Java objects, Tungsten ensures that when the CPU loads one row from memory, the next row is likely already in cache (spatial locality).

### 5.6 Whole-Stage Code Generation (Codegen)

Tungsten's codegen generates specialized Java code at runtime:

Traditional execution (Volcano model):
```
Each operator calls next() on its child:
Aggregate.next()
  → Project.next()
    → Filter.next()
      → Scan.next()
        → return row
      → check filter
    → project columns
  → update aggregate
  
Each call: virtual dispatch + function call overhead
```

Tungsten codegen (WholeStageCodegen):
```java
while (scan.hasNext()) {
    UnsafeRow row = scan.next();
    int age = row.getInt(2);
    if (age > 30) {
        UTF8String name = row.getUTF8String(1);
        aggregateMap.update(name, 1);
    }
}
// No virtual dispatch, no function calls between operators
```

You can see the generated code with:
```python
df.filter(col("age") > 30).select("name").queryExecution.debug.codegen()
```

---

## 6. Execution Plans -- Reading and Understanding

### 6.1 Types of explain()

```python
df = spark.read.parquet("/data/people/")
result = df.filter(col("age") > 30).select("name", "age").groupBy("name").count()

# Simple physical plan
result.explain()

# All plans (parsed, analyzed, optimized, physical)
result.explain(True)

# Formatted plan (Spark 3.0+)
result.explain("formatted")

# Cost-based information
result.explain("cost")

# With codegen details
result.explain("codegen")
```

### 6.2 Reading a Physical Plan

```
== Physical Plan ==
*(2) HashAggregate(keys=[name#10], functions=[count(1)])
+- Exchange hashpartitioning(name#10, 200), ENSURE_REQUIREMENTS, [plan_id=15]
   +- *(1) HashAggregate(keys=[name#10], functions=[partial_count(1)])
      +- *(1) Project [name#10]
         +- *(1) Filter (isnotnull(age#12) AND (age#12 > 30))
            +- *(1) ColumnarToRow
               +- FileScan parquet [name#10,age#12]
                  Batched: true, DataFilters: [isnotnull(age#12), (age#12 > 30)],
                  Format: Parquet, Location: InMemoryFileIndex[/data/people],
                  PartitionFilters: [], PushedFilters: [IsNotNull(age), GreaterThan(age,30)],
                  ReadSchema: struct<name:string,age:int>
```

Reading this bottom-to-top:

1. **FileScan parquet**: Reads Parquet files. 
   - `ReadSchema: struct<name:string,age:int>` -- column pruning (only reads 2 of N columns)
   - `PushedFilters: [IsNotNull(age), GreaterThan(age,30)]` -- predicate pushdown to Parquet
   
2. **ColumnarToRow**: Converts columnar Parquet batches to row format

3. **Filter**: Applies the remaining filter (though most is already pushed down)

4. **Project [name#10]**: Keeps only the `name` column (pruned `age` after filter)

5. **HashAggregate (partial)**: Local aggregation on each partition (partial count)

6. **Exchange hashpartitioning(name#10, 200)**: **SHUFFLE** -- redistributes data by `name` into 200 partitions

7. **HashAggregate (final)**: Merges partial counts into final counts

The `*(1)` and `*(2)` indicate **WholeStageCodegen** boundaries. Operators within the same `*()` are fused into a single generated function.

### 6.3 Key Plan Elements

| Element | Meaning | Performance Impact |
|---------|---------|-------------------|
| `*(n)` | WholeStageCodegen stage | Good -- operators are fused |
| `Exchange` | Shuffle (data movement) | Expensive -- network I/O |
| `BroadcastExchange` | Broadcast small table | Cheap for small data |
| `Sort` | Sorting data | Expensive for large data |
| `HashAggregate` | Hash-based aggregation | Fast, uses memory |
| `SortAggregate` | Sort-based aggregation | Slower, used as fallback |
| `BroadcastHashJoin` | Join with broadcast | Fast, no shuffle of large table |
| `SortMergeJoin` | Sort + merge join | Requires shuffle + sort |
| `FileScan` | Reading from files | Check PushedFilters and ReadSchema |
| `InMemoryTableScan` | Reading from cache | Fastest read |

### 6.4 Formatted Explain

```python
result.explain("formatted")
```

Output:
```
== Physical Plan ==
* HashAggregate (6)
+- Exchange (5)
   +- * HashAggregate (4)
      +- * Project (3)
         +- * Filter (2)
            +- Scan parquet  (1)

(1) Scan parquet 
Output [2]: [name#10, age#12]
Batched: true
Location: InMemoryFileIndex [/data/people]
PushedFilters: [IsNotNull(age), GreaterThan(age,30)]
ReadSchema: struct<name:string,age:int>

(2) Filter
Input [2]: [name#10, age#12]
Condition : (isnotnull(age#12) AND (age#12 > 30))

...
```

This format provides **per-operator details** and is easier to read for complex plans.

---

## 7. Predicate Pushdown and Column Pruning

### 7.1 Predicate Pushdown

Predicate pushdown moves filter conditions as close to the data source as possible:

**Level 1: Partition Pruning**
```python
# Table partitioned by year and month
df = spark.read.parquet("/data/events/")
df.filter(col("year") == 2024).filter(col("month") == 1)

# Spark only reads files in /data/events/year=2024/month=1/
# All other partition directories are skipped entirely
```

**Level 2: File-Level (Parquet Statistics)**
```python
# Parquet footer has min/max statistics per row group
df.filter(col("age") > 80)

# If a row group's max(age) = 65, the entire row group is skipped
# No decompression or reading of that row group's data
```

**Level 3: Data Source Pushdown**
```python
# JDBC pushdown: filter runs in the database, not Spark
df = spark.read.jdbc(url, "orders", properties=props)
df.filter(col("amount") > 1000)
# Spark generates: SELECT * FROM orders WHERE amount > 1000
# The database filters before sending data to Spark
```

### 7.2 What Breaks Predicate Pushdown

```python
# UDFs prevent pushdown (Spark can't push Python functions to Parquet)
@udf(IntegerType())
def double_age(age):
    return age * 2

df.filter(double_age(col("age")) > 60)  # NOT pushed down

# Expressions on partition columns prevent partition pruning
df.filter(col("year") + col("month") > 2025)  # NOT pruned

# Casts can sometimes prevent pushdown
df.filter(col("date").cast("string") == "2024-01-01")  # May not push down
```

### 7.3 Column Pruning

Catalyst automatically removes unused columns from the scan:

```python
# Only 'name' is used downstream
df = spark.read.parquet("/data/people/")  # 50 columns in schema
result = df.filter(col("age") > 30).select("name")

# Physical plan:
# FileScan parquet [name, age]    ← only reads 2 columns, not 50
# Filter (age > 30)
# Project [name]                   ← drops 'age' after filter
```

For Parquet with 50 columns, column pruning can reduce I/O by **96%** (reading 2 of 50 columns).

### 7.4 Constant Folding

```python
df.filter(col("age") > 10 + 20)
# Optimized to: df.filter(col("age") > 30)
# The expression 10+20 is evaluated at compile time

df.filter(lit(True))
# Optimized away: the filter is removed entirely
```

---

## 8. Join Strategy Selection

### 8.1 How Catalyst Chooses a Join Strategy

Catalyst selects the join algorithm based on:
1. **Table sizes** (from statistics or file sizes)
2. **Join type** (inner, outer, semi, anti)
3. **Join condition** (equi-join vs non-equi)
4. **User hints** (BROADCAST, MERGE, SHUFFLE_HASH)

Decision flow:
```
Is there a BROADCAST hint?
├── YES → BroadcastHashJoin
└── NO
    Is one side < autoBroadcastJoinThreshold (default 10 MB)?
    ├── YES → BroadcastHashJoin
    └── NO
        Is there a SHUFFLE_HASH hint?
        ├── YES → ShuffledHashJoin
        └── NO
            Is one side < 3x the other AND much smaller than spark.sql.shuffle.partitions * 
            spark.sql.adaptive.advisoryPartitionSizeInBytes?
            ├── YES → ShuffledHashJoin (if enabled)
            └── NO → SortMergeJoin (default for large-large)
```

### 8.2 Broadcast Hash Join

```
Driver collects small table → broadcasts to all executors
Each executor joins its partition of large table with the broadcast table locally

No shuffle of the large table!
```

```python
from pyspark.sql.functions import broadcast

# Explicit broadcast
result = large_df.join(broadcast(small_df), "key")

# Or let Spark auto-decide (if small_df < 10 MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10 * 1024 * 1024)  # 10 MB
```

Physical plan shows:
```
BroadcastHashJoin [key], [key], Inner, BuildRight
:- *(1) FileScan parquet [large_table]
+- BroadcastExchange HashedRelationBroadcastMode
   +- *(2) FileScan parquet [small_table]
```

When to use: One table fits in memory (driver + executor memory). The threshold is per-table, so a 10 MB table broadcast to 100 executors uses 1 GB total cluster memory.

### 8.3 Sort-Merge Join

```
1. Shuffle both tables by join key (Exchange hashpartitioning)
2. Sort both sides by join key within each partition
3. Merge: walk through both sorted partitions simultaneously

Two shuffle operations + two sorts
```

Physical plan:
```
SortMergeJoin [key], [key], Inner
:- *(2) Sort [key ASC]
:  +- Exchange hashpartitioning(key, 200)
:     +- *(1) FileScan parquet [table_a]
+- *(4) Sort [key ASC]
   +- Exchange hashpartitioning(key, 200)
      +- *(3) FileScan parquet [table_b]
```

When used: Both tables are large. This is the most general join strategy.

### 8.4 Shuffle Hash Join

```
1. Shuffle both tables by join key
2. Build a hash table on the smaller side within each partition
3. Probe with the larger side

One shuffle for each table + hash table build
```

When used: One table is significantly smaller than the other but too large to broadcast. Must be enabled:
```python
spark.conf.set("spark.sql.join.preferSortMergeJoin", False)
```

### 8.5 Broadcast Nested Loop Join

Used for:
- Non-equi joins (e.g., `a.value BETWEEN b.low AND b.high`)
- Cross joins with a small table

```python
# Non-equi join: range lookup
result = events.join(broadcast(ranges), 
    (events.timestamp >= ranges.start) & (events.timestamp < ranges.end))
```

### 8.6 Join Strategy Comparison

| Strategy | Shuffle | Sort | Memory | Best For |
|----------|---------|------|--------|----------|
| Broadcast Hash | None (large side) | None | Must fit small table | Small-large |
| Sort-Merge | Both sides | Both sides | Low | Large-large |
| Shuffle Hash | Both sides | None | Must fit one side per partition | Medium-large |
| Broadcast Nested Loop | None (large side) | None | Must fit small table | Non-equi + small |

---

## 9. Adaptive Query Execution (AQE)

### 9.1 What Is AQE

AQE is a runtime optimization framework (Spark 3.0+) that re-optimizes the query plan **during execution** based on actual runtime statistics, not compile-time estimates.

```python
spark.conf.set("spark.sql.adaptive.enabled", True)  # enabled by default in Spark 3.2+
```

### 9.2 How AQE Works

Traditional Spark: compile plan → execute entire plan
AQE: compile plan → execute stage → collect runtime stats → re-optimize remaining plan → execute next stage → ...

```
Stage 1: Scan + Filter + Partial Aggregate
    ↓ (shuffle write)
    [AQE collects: partition sizes, row counts, data distribution]
    ↓ (re-optimize remaining plan)
Stage 2: Final Aggregate (with optimized partition count)
```

### 9.3 AQE Feature 1: Coalescing Shuffle Partitions

The default `spark.sql.shuffle.partitions = 200` is often wrong:
- Too many partitions for small data → overhead of scheduling 200 tiny tasks
- Too few partitions for large data → OOM or slow tasks

AQE dynamically adjusts:
```
Before AQE:
200 partitions, many are tiny: [1MB, 50KB, 2MB, 10KB, 100KB, ...]

After AQE coalescing:
20 partitions, well-sized: [50MB, 48MB, 52MB, ...]
```

```python
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", True)
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")  # target partition size
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1MB")
```

### 9.4 AQE Feature 2: Switching Join Strategies

At compile time, Spark might estimate a table as 500 MB (too large for broadcast). But after runtime filtering, the actual data is only 5 MB. AQE switches to a broadcast join:

```
Compile-time plan: SortMergeJoin (estimated 500 MB table)
Runtime: after filters, actual size is 5 MB
AQE re-plan: BroadcastHashJoin (5 MB fits broadcast threshold)
```

### 9.5 AQE Feature 3: Skew Join Optimization

When data is skewed (one partition has much more data than others), one task takes much longer:

```
Normal partitions: [100MB, 100MB, 100MB, 100MB, 5GB, 100MB]
                                                  ↑ skewed partition
```

AQE detects the skewed partition and splits it into smaller sub-partitions:
```
After AQE: [100MB, 100MB, 100MB, 100MB, 100MB, 100MB, ..., 100MB]  ← 5GB split into ~50 partitions
```

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 5)  # partition is skewed if > 5x median
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

### 9.6 AQE in the Physical Plan

```
== Physical Plan ==
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   *(3) HashAggregate(keys=[name], functions=[count(1)])
   +- AQEShuffleRead coalesced          ← AQE coalesced partitions
      +- ShuffleQueryStage 0
         +- Exchange hashpartitioning(name, 200)
            +- *(1) HashAggregate(keys=[name], functions=[partial_count(1)])
```

`AdaptiveSparkPlan` wraps the plan. `AQEShuffleRead coalesced` shows that AQE reduced the partition count.

---

## 10. PySpark vs Scala Spark Performance

### 10.1 When PySpark = Scala Spark (No Difference)

When using the **DataFrame API or Spark SQL** without Python UDFs:
- PySpark and Scala Spark produce the **exact same physical plan**
- All execution happens in the JVM
- The only overhead is Py4J calls during plan construction (microseconds)

```python
# PySpark
df.filter(col("age") > 30).groupBy("city").count()

# Scala Spark
df.filter($"age" > 30).groupBy("city").count()

# Both produce identical JVM execution -- same speed
```

### 10.2 When PySpark Is Slower

**Python UDFs**: Data must cross the JVM-Python boundary:
```
JVM Executor  →  serialize to pickle  →  send over socket  →  Python Worker
                                                                    |
                                                              run Python UDF
                                                                    |
JVM Executor  ←  deserialize from pickle  ←  send over socket  ←  return
```

This serialization/deserialization per partition adds significant overhead (~10-100x slower than native Spark functions).

**RDD operations in Python**:
```python
# RDD map in Python -- data crosses JVM-Python boundary for every partition
rdd.map(lambda x: x * 2)

# DataFrame operation -- stays entirely in JVM
df.select(col("value") * 2)  # much faster
```

### 10.3 Mitigating PySpark Overhead

| Technique | How |
|-----------|-----|
| Use DataFrame API / SQL | Execution stays in JVM |
| Use Pandas UDFs over regular UDFs | Apache Arrow (columnar, zero-copy) instead of pickle (row-by-row) |
| Use built-in Spark functions | `F.when()`, `F.regexp_extract()`, `F.array()`, etc. |
| Broadcast variables | Serialize once, not per task |
| Avoid `collect()` on large data | collect() sends all data from executors to Python driver |
| Avoid RDD operations | Use DataFrame/Dataset API |

### 10.4 Pandas UDFs -- The Best of Both Worlds

Pandas UDFs use **Apache Arrow** instead of pickle:
```
JVM Executor  →  Arrow IPC (columnar, zero-copy)  →  Python Worker
                                                          |
                                                     Pandas operation
                                                     (vectorized)
                                                          |
JVM Executor  ←  Arrow IPC (columnar, zero-copy)  ←  return
```

Arrow vs pickle performance:
- Arrow: columnar format, ~100x faster serialization, zero-copy when possible
- Pickle: row-by-row, Python object overhead, GC pressure

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# Regular UDF: ~10x slower
@udf(DoubleType())
def slow_udf(value):
    return value * 2.0

# Pandas UDF: ~2x overhead vs native (vs ~10-50x for regular UDF)
@pandas_udf(DoubleType())
def fast_udf(s: pd.Series) -> pd.Series:
    return s * 2.0
```

---

## 11. Quick-Reference Cheat Sheet

### Catalyst Phases

| Phase | Input | Output | What Happens |
|-------|-------|--------|-------------|
| Analysis | Unresolved plan | Analyzed plan | Resolve names, types, functions |
| Logical Optimization | Analyzed plan | Optimized plan | Predicate pushdown, column pruning, constant folding |
| Physical Planning | Optimized plan | Physical plan | Choose join algorithms, aggregation strategies |
| Code Generation | Physical plan | Generated Java code | WholeStageCodegen, operator fusion |

### Key Optimizations

| Optimization | What | Impact |
|-------------|------|--------|
| Predicate Pushdown | Move filters to data source | Skip data at read time |
| Column Pruning | Read only needed columns | 2-50x less I/O for wide tables |
| Constant Folding | Evaluate constants at compile | Tiny, but simplifies plan |
| Join Reordering | Optimal join sequence | Can be orders of magnitude |
| Broadcast Join | Broadcast small table | Eliminates shuffle |
| AQE Coalescing | Merge small partitions | Reduces task overhead |
| AQE Skew Join | Split skewed partitions | Fixes straggler tasks |

### explain() Cheat Sheet

| Element | Meaning |
|---------|---------|
| `*(n)` | WholeStageCodegen stage n |
| `Exchange` | Shuffle (expensive!) |
| `BroadcastExchange` | Small table broadcast |
| `Sort` | Sorting (expensive for large data) |
| `FileScan` | Reading files |
| `PushedFilters` | Filters sent to data source |
| `ReadSchema` | Columns actually read |
| `PartitionFilters` | Partition pruning filters |
| `AdaptiveSparkPlan` | AQE is active |
| `AQEShuffleRead coalesced` | AQE merged partitions |

### PySpark Performance Rules

```
1. Use DataFrame API / SQL → execution stays in JVM
2. Avoid Python UDFs → use built-in Spark functions instead
3. If UDF is needed → use Pandas UDF (Arrow) over regular UDF (pickle)
4. Never collect() large data → process in Spark, export to storage
5. Use broadcast() for small lookup tables → eliminates shuffle
6. Enable AQE → handles partition sizing and skew automatically
7. Check explain() → verify pushdown, join strategy, partition count
```

### Config Quick Reference

| Config | Default | Purpose |
|--------|---------|---------|
| `spark.sql.autoBroadcastJoinThreshold` | 10 MB | Max table size for auto-broadcast |
| `spark.sql.shuffle.partitions` | 200 | Number of partitions after shuffle |
| `spark.sql.adaptive.enabled` | true (3.2+) | Enable AQE |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | AQE auto-partition sizing |
| `spark.sql.adaptive.skewJoin.enabled` | true | AQE skew handling |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64 MB | AQE target partition size |
| `spark.memory.offHeap.enabled` | false | Enable off-heap memory (Tungsten) |
