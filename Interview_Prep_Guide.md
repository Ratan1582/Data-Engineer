# Technical Experience → Interview Preparation Guide

> Tailored for: **Vemula Ratan** | Data Engineer | Jio Platforms Limited
> Covers: Python, Pandas, PySpark, Spark Optimization, Cassandra, MongoDB, HDFS, Pipeline Architecture
> Based on: Battery Analytics Platform & Smart Meter Data Analytics Pipeline (DAP)

---

## Table of Contents

1. [Section 1 — Python & Pandas](#section-1--python--pandas)
2. [Section 2 — PySpark Transformations & Operations](#section-2--pyspark-transformations--operations)
3. [Section 3 — applyInPandas & pandas_udf](#section-3--applyinpandas--pandas_udf)
4. [Section 4 — Spark Performance Optimization](#section-4--spark-performance-optimization)
5. [Section 5 — Data Pipeline Architecture & ETL Design](#section-5--data-pipeline-architecture--etl-design)
6. [Section 6 — Databases: Cassandra, MongoDB, HDFS](#section-6--databases-cassandra-mongodb-hdfs)
7. [Section 7 — Terminology Mapping Table](#section-7--terminology-mapping-table)
8. [Section 8 — Learning Resources](#section-8--learning-resources)

---

# Section 1 — Python & Pandas

## 1.1 What It Is & Why It Matters

**Python** is the backbone scripting language for data engineering. In your role, Python was the first tool you used to build complete data processing workflows before migrating to Spark.

**Pandas** is a single-node, in-memory data manipulation library. It excels at:
- Row-by-row logic (iterating, stateful transformations)
- Complex algorithmic operations that are hard to express in SQL/Spark
- Rapid prototyping before scaling to distributed systems

**Why interviewers ask about it:** They want to know (a) you can write clean Python logic, (b) you understand the LIMITS of Pandas (single-node, memory-bound), and (c) you know WHEN to use Pandas vs Spark.

## 1.2 Where You Used It (Your Real Projects)

### Battery Team — Charging/Discharging Cycle Estimation

In `daily_aggregation_metrics.py`, you built an algorithm that:

1. Takes a battery's SoC (State of Charge) time-series for one day
2. Detects local maxima and minima in the SoC curve
3. Pairs them into charging cycles (minima → maxima) and discharging cycles (maxima → minima)
4. Merges short gaps (< 6 min) between cycles
5. Computes per-cycle KPIs: duration, C-rate (min/max/avg), temperature, pack voltage

**Why this needed Pandas, not Spark:**
The algorithm is inherently **stateful** and **sequential** — you iterate over sorted SoC values, maintain a `last_standby` tracker, compare each point against the previous one, and build up a list of (maxima, minima) pairs. This cannot be expressed as a pure Spark transformation because each row depends on the result of processing the previous row.

```python
# Your actual pattern (simplified):
for i in range(2, len(pack_vol)):
    if pack_vol[values[-1]] == pack_vol[i]:  # stagnant SoC
        if cycle_status == 'Discharging' and is_maxima[-1]:
            values[-1] = i  # move maxima forward
    elif pack_vol[values[-1]] < pack_vol[i]:
        if is_maxima[-1]:
            values[-1] = i  # replace maxima
        else:
            values.append(i)  # add new maxima
            is_maxima.append(True)
```

### Battery Team — Energy Estimation

In `charging_energy_estimation` and `discharging_energy_estimation`, you computed energy as:
```
Energy (kWh) = Σ (current × voltage × time_delta) / (1000 × 3600)
```
This required sorted DataFrame operations with `.diff()`, status filtering, and threshold capping (data loss > 120 seconds capped).

### Battery Team — Data Quality Report

`generate_data_report` computed per-battery data completeness ratios. If a field (SoC, current, temperature) had < 5% non-null values, downstream calculations were skipped. This is a **data quality gate** — a concept interviewers love to hear about.

### Battery Team — Operational Comfort Zones

`operational_limits` computed what percentage of time a battery stayed within safe ranges:
- SoC comfort zone: 20%–80%
- Temperature comfort zone: 0°C–40°C
- C-rate comfort zone: ≤ 0.5C

This used Pandas filtering + time-diff accumulation — a pattern that appears simple but requires careful handling of time gaps and null values.

## 1.3 Interview Questions & Answers

### Q1: "Tell me about a complex data processing pipeline you built in Python."

**Your Answer:**
"I built a battery analytics pipeline that processed daily telemetry data from 100K+ IoT battery systems. The core challenge was detecting charging and discharging cycles from raw State of Charge (SoC) time-series data. The algorithm I wrote identifies local maxima and minima in the SoC curve, pairs them into cycles, merges short gaps (under 6 minutes), and then computes per-cycle KPIs like duration, C-rate, temperature range, and energy consumed. Initially, this was a pure Python/Pandas pipeline, but as the fleet grew from a few thousand to 100K batteries, I migrated the processing to Spark using `applyInPandas` — keeping the algorithmic core in Pandas while distributing the per-battery processing across the cluster."

### Q2: "What are the limitations of Pandas? When would you NOT use it?"

**Your Answer:**
"Pandas is single-threaded and loads everything into memory. In my battery project, when we had a small fleet, Pandas was fine — a day's data for a few hundred batteries fit in memory. But as we scaled to 100K batteries, a single day's raw data was hundreds of GBs. Pandas couldn't handle it. The key limits are:
1. **Memory-bound** — the entire DataFrame must fit in RAM
2. **Single-core** — no parallelism, so processing 100K groups sequentially is too slow
3. **No lazy evaluation** — every operation executes immediately, so you can't optimize a chain of operations

That's why I migrated to Spark — the data gets partitioned across the cluster, each executor processes a subset of batteries in parallel, and Spark's lazy evaluation optimizes the execution plan."

### Q3: "How do you handle data quality issues in your pipeline?"

**Your Answer:**
"In my battery pipeline, I built a data quality report as the first step of aggregation. For each battery, I compute a completeness ratio for every field — SoC, current, voltage, temperature. If a critical field like SoC has less than 5% non-null values, I skip that battery's aggregation entirely because the results would be meaningless. For example, if the BMS wasn't reporting current data, I can't compute C-rate or energy, so those fields get set to null rather than incorrect zeros. This prevents bad data from polluting downstream dashboards and alerts. The data report is broadcast to all executors so each battery's processing logic can check it before running expensive calculations."

### Q4: "Explain a time you optimized a slow Python/Pandas pipeline."

**Your Answer:**
"The original battery aggregation took several hours running sequentially in Pandas. The bottleneck was the cycle detection algorithm — it iterated over every battery's SoC values one by one. I optimized it in two phases:
1. **Vectorized where possible** — replaced row-by-row `.iterrows()` on some computations with vectorized Pandas operations (like `.diff()` for time deltas, boolean masking for filtering by battery status)
2. **Migrated to Spark** — wrapped the per-battery logic in `applyInPandas`, which distributes the batteries across executors. On 8 cores, this gave roughly a 6-7x speedup because batteries are independent — there's no cross-battery dependency.

The key insight is: the within-battery logic is sequential (you need sorted time-series), but across batteries it's embarrassingly parallel."

### Q5: "What is the difference between `.apply()`, `.map()`, and `.applymap()` in Pandas?"

**Your Answer:**
- `.apply()` — works on a column (Series) or entire DataFrame. I used it extensively: `battery_df['time_diff'].apply(lambda td: min(td, cap_threshold))` to cap time differences at a maximum value
- `.map()` — works on a Series only, element-by-element. Useful for simple value transformations like `temperature = list(map(lambda x: round(x, 2), temperature))`
- `.applymap()` — works element-by-element on an entire DataFrame. I used it less, but it's useful when you need to transform every cell (deprecated in newer Pandas in favor of `.map()` on DataFrames)

In practice, I mostly used `.apply()` because my operations needed the context of the entire group (all columns together), not just individual values."

### Q6: "How do you handle datetime operations in Pandas?"

**Your Answer:**
"Heavily. Battery data is time-series data, so datetime manipulation is central:
- **Parsing**: `pd.to_datetime(tms_vals)` to convert raw timestamps
- **Sorting**: `battery_df.sort_values(by='event_time')` — critical because cycle detection depends on time order
- **Time differences**: `battery_df['event_time'].diff().dt.total_seconds()` to compute intervals between readings
- **Duration capping**: If a gap exceeds the expected data frequency (e.g., 3x the normal interval), I cap it — this handles data loss periods without inflating durations
- **Timedelta arithmetic**: `row['start_time'] - prev_row[1] < timedelta(minutes=6)` to merge nearby cycles

One subtle issue: when computing battery utilization hours, you can't just do `end_time - start_time` because there might be data gaps. I accumulated time by summing capped inter-reading intervals, which gives more accurate results."

---

### Scenario-Based Questions

### S1: "You have a Pandas pipeline that processes 50GB of data but your machine only has 16GB RAM. What do you do?"

**Your Answer:**
"This is exactly what happened in my battery project. The options I considered:
1. **Chunked processing** — read parquet files in chunks using `pd.read_parquet` with row groups, process each chunk, aggregate results. Works if each chunk is independent.
2. **Column pruning** — only load the columns you actually need. Our raw battery data had 30+ columns but most aggregations only needed 6-7.
3. **Migrate to Spark** — which is what I ultimately did. Spark reads parquet lazily, partitions the data across executors, and only materializes what's needed. The per-battery processing logic stayed in Pandas via `applyInPandas`, but the data distribution is handled by Spark.

The migration approach was the right call because: (a) our data was growing monthly, (b) we needed to run this daily as a scheduled job, and (c) Spark's fault tolerance handles node failures automatically."

### S2: "Your Pandas groupby-apply is taking 3 hours. How do you speed it up?"

**Your Answer:**
"I faced this. The cycle estimation on 100K batteries was extremely slow. My approach:
1. **Profile first** — identify which function inside the apply is slow. In my case, the nested for-loop over SoC values was the bottleneck, not the groupby itself.
2. **Vectorize inner logic** — I replaced some of the loop-based calculations (like time-diff accumulation) with vectorized `.diff()` and `.cumsum()`.
3. **Reduce data early** — filter out batteries with insufficient data BEFORE the groupby. The data quality report helps here — if SoC completeness < 5%, skip the battery entirely.
4. **Parallelize across groups** — move to Spark's `applyInPandas`, which distributes groups across executors. Each executor runs the Pandas function on its subset of batteries.

The result went from ~3 hours to ~25 minutes."

---

# Section 2 — PySpark Transformations & Operations

## 2.1 What It Is & Why It Matters

PySpark is the Python API for Apache Spark. It enables distributed data processing on clusters. The key mental model:

- **DataFrame** — a distributed collection of rows organized into columns, partitioned across the cluster
- **Transformation** — a lazy operation that defines WHAT to do (not executed immediately): `filter`, `select`, `groupBy`, `join`, `withColumn`
- **Action** — triggers actual computation: `count`, `show`, `collect`, `write`
- **Lazy Evaluation** — Spark builds a logical plan (DAG) and only executes when an action is called, allowing it to optimize the entire chain

**Why interviewers ask:** Spark knowledge is the #1 requirement for data engineering roles. They want to see you understand distributed computing, not just syntax.

## 2.2 Where You Used It (Your Real Projects)

### Smart Meter Team — Block Aggregation (Pivot Pattern)

In `data_aggregate_pyspark.py`, you process smart meter block load profiles. The raw data comes as flat rows:
```
(IMEI, Vendor, Timestamp, Register, Value)
```

You need to pivot this into:
```
(IMEI, Vendor, Timestamp, ActiveEnergyImport, ActiveEnergyExport, Voltage, ...)
```

Your optimization: **Pre-aggregate BEFORE pivoting** — this is a critical performance pattern.

```python
# Step 1: Pre-aggregate per register (reduces rows drastically before pivot)
pre_agg = df.groupBy("IMEI", "Vendor", "Date", "Hour", "Register").agg(
    F.sum("Value").alias("sum_value"),
    F.avg("Value").alias("avg_value"),
)

# Step 2: Choose sum or avg based on register type
pre_agg = pre_agg.withColumn(
    "Value",
    F.when(F.col("Register") == self.VOLTAGE_REGISTER, F.round(F.col("avg_value"), 2))
     .otherwise(F.round(F.col("sum_value"), 2))
).drop("sum_value", "avg_value")

# Step 3: Pivot on pre-aggregated data (explicit values avoid extra discovery job)
pivoted = pre_agg.groupBy("IMEI", "Vendor", "Date", "Hour") \
    .pivot("Register", self.BLOCK_REGISTERS) \
    .agg(spark_first("Value"))
```

**Why this matters:** Without pre-aggregation, Spark would pivot on millions of raw rows, causing a massive shuffle. Pre-aggregating first reduces the data volume by 80-90% before the expensive pivot operation.

### Smart Meter Team — Daily Aggregation (Join-Based Delta)

In `hdfs_daily_data`, you compute daily energy consumption by subtracting two consecutive days' cumulative readings:

```python
# Join today's readings with yesterday's readings
final_df = df1.alias('df1').join(df2.alias('df2'), on=['IMEI', 'Vendor'], how='inner')

# Compute deltas
final_df = final_df.withColumn('Active energy Import',
    F.col('df1.`1-0.1.8.0.255`') - F.col('df2.`1-0.1.8.0.255`'))

# Sanity check: filter out negative values (meter resets, bad data)
final_df = final_df.withColumn('min_val', F.least(
    'Active energy Import', 'Active energy export', ...
)).filter(F.col('min_val') >= 0).drop('min_val')
```

### Smart Meter Team — SBD Data Completeness (Conditional Aggregation)

In `sbd.py`, you compute data completeness percentages with time-based flags:

```python
# For each hour deadline (7, 8, 11, 12, 22, 24):
for hour in hours:
    deadline = col("date").cast("timestamp") + expr(f"INTERVAL {24 + hour} HOURS")
    work_df = work_df.withColumn(
        f"flag_{hour}",
        when(col("Receive_time").isNotNull() & (col("Receive_time") < deadline), lit(1))
        .otherwise(lit(0))
    )

# Aggregate per device
sbd_count_df = work_df.groupBy("IMEI", "date", "Profile", "Vendor", COL_INSTALLATION_DATE) \
    .agg(*[spark_sum(col(f"flag_{h}")).alias(str(h)) for h in hours],
         countDistinct(col("Measure_time")).alias("total_count"))
```

### Smart Meter Team — Window Functions

In `data_aggregate_pyspark.py` (monthly billing), you use Window functions to pick the right reading:

```python
w = Window.partitionBy('IMEI', 'Vendor', 'Date').orderBy(F.col('Timestamp').desc())
monthly_df = df.withColumn('rn', F.row_number().over(w)).filter('rn = 1').drop('rn')
```

This selects the latest reading per IMEI per day — a de-duplication pattern using Window functions.

### Battery Team — Spark Aggregations

In `daily_aggregation_metrics.py`, you use Spark's built-in aggregation functions:

```python
aggregation_df = grouped_df.agg(
    functions.round(functions.count("*"), 0).alias("payload_count"),
    functions.round(functions.min("soc"), 0).alias("min_soc"),
    functions.round(functions.max("soc"), 0).alias("max_soc"),
    functions.round(functions.last("soh", ignorenulls=True), 0).alias("bms_soh"),
    functions.round(functions.min("pack_voltage")/1, 2).alias("min_pack_voltage"),
    ...
)
```

## 2.3 Interview Questions & Answers

### Q1: "Explain the difference between a transformation and an action in Spark."

**Your Answer:**
"A transformation is a lazy operation — it defines what should happen but doesn't execute. An action triggers the actual computation.

In my smart meter pipeline:
- `df.filter(col('Profile') == '1-0.99.1.0.255')` — transformation, just adds to the plan
- `df.groupBy(...).agg(...)` — transformation
- `df.write.parquet(path)` — ACTION, this triggers the entire chain to execute

Spark builds a DAG (Directed Acyclic Graph) of transformations. When an action is called, Spark's Catalyst optimizer analyzes the entire DAG, reorders operations, prunes unnecessary columns, and generates an optimized physical plan. This is why lazy evaluation matters — Spark can optimize the ENTIRE pipeline, not just individual steps."

### Q2: "What is the difference between narrow and wide transformations?"

**Your Answer:**
"Narrow transformations don't require data movement between partitions — each output partition depends on only one input partition. Examples: `filter`, `select`, `withColumn`, `map`.

Wide transformations require shuffling data across the network — output partitions depend on multiple input partitions. Examples: `groupBy`, `join`, `repartition`, `pivot`.

In my block aggregation pipeline, I have two wide transformations back-to-back: the `groupBy` for pre-aggregation and then the `pivot`. Originally, this caused two shuffles. I optimized by ensuring both `groupBy` operations use the same key columns (IMEI, Vendor, Date, Hour), so the second shuffle is minimal because data is already co-located from the first.

The practical impact: in my pipeline processing 4M meters, reducing one unnecessary shuffle saved ~15 minutes per run."

### Q3: "Explain how `groupBy` + `agg` works in Spark."

**Your Answer:**
"groupBy creates groups of rows that share the same key values. agg then computes aggregate functions within each group. Under the hood:

1. **Map phase** — each partition computes partial aggregates locally
2. **Shuffle** — partial results are redistributed so all data for the same key ends up on the same executor
3. **Reduce phase** — partial aggregates are combined into final results

In my battery pipeline:
```python
aggregation_df = grouped_df.agg(
    functions.min('soc').alias('min_soc'),
    functions.max('soc').alias('max_soc'),
    functions.mean('max_cell_temperature').alias('avg_cell_temperature')
)
```
This computes SoC range and average temperature for each battery. The key is 'uid' (battery identifier). Spark partitions the data by uid, and each partition computes these aggregates independently — which is why it scales linearly with the cluster size."

### Q4: "Explain `pivot` in Spark. Why is it expensive?"

**Your Answer:**
"Pivot transposes unique values of a column into separate columns. In my smart meter pipeline, raw data has a 'Register' column with values like '1-0.1.29.0.255' (Active Energy Import), '1-0.12.27.0.255' (Voltage), etc. Pivot creates separate columns for each register.

It's expensive because:
1. **Discovery** — Spark must first scan the data to find all unique values of the pivot column (unless you specify them explicitly)
2. **Shuffle** — data must be redistributed by the groupBy keys
3. **Memory** — wide DataFrames with many pivot columns consume more memory

My optimization:
```python
# Explicit pivot values — avoids the discovery scan
.pivot('Register', self.BLOCK_REGISTERS)
```
And pre-aggregating before pivot reduces the data volume that needs to be shuffled. This turned a 40-minute operation into a 5-minute one."

### Q5: "How do Window functions work? When do you use them?"

**Your Answer:**
"Window functions compute values across a 'window' of rows related to the current row, without collapsing the rows (unlike groupBy-agg which collapses).

I use them for:
1. **De-duplication** — selecting the latest reading per meter per day:
   ```python
   w = Window.partitionBy('IMEI', 'Date').orderBy(col('Timestamp').desc())
   df.withColumn('rn', row_number().over(w)).filter('rn = 1')
   ```
2. **Running totals** — in battery cycle detection, I needed lag/lead operations
3. **Forward-fill nulls** — in battery preprocessing:
   ```python
   window_spec = Window.orderBy('event_time').rowsBetween(Window.unboundedPreceding, 0)
   df.withColumn('soc', last('soc', ignorenulls=True).over(window_spec))
   ```

The key difference from groupBy: Window preserves all rows. groupBy collapses them. Use Window when you need the computed value alongside the original row."

### Q6: "What are the different types of joins in Spark? When do you use each?"

**Your Answer:**
"The main join types I've used:

1. **Inner join** — only matching rows. In my daily aggregation, I join today's and yesterday's readings: `df1.join(df2, on=['IMEI', 'Vendor'], how='inner')`. Only meters with BOTH days' readings get a daily aggregate.

2. **Left anti join** — rows from left that DON'T match right. I use this for incremental processing:
   ```python
   result_agg_df = daily_agg_df.join(
       existing_devices_df.select('IMEI').distinct(),
       on='IMEI', how='left_anti'
   )
   ```
   This filters out meters that have already been processed — avoiding duplicate writes.

3. **Right join** — all rows from right, matching from left. In SBD processing, I join meter data with metadata using right join so every registered meter appears even if it had no data that day (to count it as 0% completeness).

4. **Broadcast join** — when one side is small enough to fit in executor memory:
   ```python
   final_sbd_data_df = sbd_data_df.join(broadcast(metadata_spark_df), on=['IMEI', 'Vendor', 'Profile'], how='right')
   ```
   This avoids shuffling the large dataset — the metadata is broadcast to all executors."

---

### Scenario-Based Questions

### S1: "You need to compute daily energy consumption from cumulative meter readings. How would you design this in Spark?"

**Your Answer:**
"This is exactly what I built. Cumulative readings only go up — so daily consumption = today's reading minus yesterday's reading.

My approach:
1. **Fetch two days' data** — today's and yesterday's cumulative readings from HDFS parquet
2. **Process each day** — pivot the register values, handle duplicates, select latest reading per meter
3. **Join on IMEI + Vendor** — inner join ensures we only compute deltas for meters with both days' readings
4. **Subtract** — `df1['Active energy Import'] - df2['Active energy Import']`
5. **Validate** — filter out negative values (meter resets, replacements) using `F.least()` check
6. **De-duplicate** — use `left_anti` join against already-processed device list to avoid re-inserting into Cassandra

Key consideration: what if yesterday's reading is missing? I use `unionByName` with consumer-type data as a fallback source, so daily aggregates are as complete as possible."

### S2: "You have raw IoT data arriving as flat key-value pairs. How do you efficiently convert it to a columnar format?"

**Your Answer:**
"This is the smart meter block data problem. Raw data arrives as (IMEI, Timestamp, Register, Value) — one row per register per reading. I need (IMEI, Timestamp, ActiveEnergyImport, Voltage, ...) — one row per reading with registers as columns.

The naive approach: just `pivot('Register')`. But with 4M meters and 96 readings/day and 5 registers, that's ~2 billion rows to pivot — extremely slow.

My optimization:
1. Pre-aggregate by (IMEI, Vendor, Date, Hour, Register) — this collapses duplicate readings within the same hour
2. Apply register-specific logic (sum for energy, average for voltage) during pre-aggregation
3. Pivot the much smaller pre-aggregated dataset with explicit pivot values (avoids the discovery scan)
4. Rename columns from OBIS codes to human-readable names

This reduced the pivot from ~40 minutes to ~5 minutes."

---

# Section 3 — applyInPandas & pandas_udf

## 3.1 What It Is & Why It Matters

These are Spark's mechanisms for running Python/Pandas code within a distributed Spark pipeline:

### applyInPandas (Grouped Map)
- Groups the Spark DataFrame by key columns
- Sends each group as a Pandas DataFrame to a Python function
- Collects the results back into a Spark DataFrame
- Use when: you need complex, stateful, per-group logic that's hard/impossible to express in Spark SQL

### pandas_udf (Scalar or Grouped)
- Vectorized UDF that operates on Pandas Series or DataFrames
- Much faster than regular Python UDFs because data is transferred in Arrow batches, not row-by-row
- Use when: you need custom per-row or per-batch transformations, especially involving external libraries (like PyTorch)

### Regular Python UDF
- Row-by-row Python function, serialized via pickle
- Slowest option — data is serialized/deserialized per row
- Use as last resort

**Interview buzzword:** "Arrow-optimized vectorized UDFs" — this is what `pandas_udf` is under the hood.

## 3.2 Where You Used It (Your Real Projects)

### Battery Team — applyInPandas (6+ use cases)

In `daily_aggregation_metrics.py`, you use `applyInPandas` extensively:

```python
grouped_df = spark_df.repartition("uid").persist()
grouped_df = grouped_df.groupBy("uid")

# 1. Data quality report
data_report_df = grouped_df.applyInPandas(generate_data_report, schema=data_report_schema)

# 2. Charging cycles
charging_cycle_df = grouped_df.applyInPandas(charging_cycles_estimation, schema=cycles_schema)

# 3. Discharging cycles
discharging_cycle_df = grouped_df.applyInPandas(discharging_cycles_estimation, schema=cycles_schema)

# 4. Charging energy
charging_energy_df = grouped_df.applyInPandas(charging_energy_estimation, schema=charging_energy_schema)

# 5. Standby hours
standby_hrs_df = grouped_df.applyInPandas(standby_hrs_estimation, schema=standby_hrs_schema)

# 6. Operational limits (comfort zones)
operational_df = grouped_df.applyInPandas(operational_limits, schema=operational_schema)

# 7. Fluctuation count
fluctuation_df = grouped_df.applyInPandas(get_fluctuation_count, schema=fluctuation_schema)
```

**Architecture pattern:**
1. Repartition by `uid` (battery ID) — ensures each battery's data is on one partition
2. GroupBy `uid`
3. Apply 7+ different Pandas functions, each producing a different result schema
4. Join all results back together on `uid`

This is the **"Fan-out, Process, Fan-in"** pattern — a key distributed computing concept.

### Smart Meter Team — pandas_udf for ML Inference

In `Disaggregate.py`, you use `pandas_udf` to run PyTorch model predictions inside Spark:

```python
@pandas_udf(DoubleType())
def predict_udf(data_batch, aux_batch):
    input_data = np.array(data_batch.tolist(), dtype=np.float32).reshape(-1, 1, seq_len)
    aux_data = np.array(aux_batch.tolist(), dtype=np.float32).reshape(-1, 4)
    model = model_broadcast.value
    with torch.no_grad():
        predictions = model(
            torch.tensor(input_data, device=device),
            torch.tensor(aux_data, device=device)
        ).cpu().numpy().flatten()
    return pd.Series(predictions)
```

**Key design decisions:**
- The PyTorch model is **broadcast** to all executors — loaded once, used everywhere
- `pandas_udf` processes data in **batches** (Arrow columnar format), not row-by-row
- GPU support: `device = torch.device("cuda" if torch.cuda.is_available() else "cpu")`

### Battery Team — Broadcast Variables for Shared State

```python
broadcast_data_report_df = spark.sparkContext.broadcast(data_report_df)

# Inside applyInPandas function:
def charging_cycles_estimation(battery_df):
    bat_uid = battery_df['uid'].iloc[0]
    data_report_df = broadcast_data_report_df.value  # Access broadcast variable
    data_report = data_report_df[data_report_df['uid'] == bat_uid]
    if data_report.empty or (data_report['soc'].values[0] < 0.05):
        return pd.DataFrame()  # Skip this battery
    ...
```

The data quality report is small (one row per battery) but needed by every function. Broadcasting it avoids sending it as part of the closure (which would serialize it per task).

## 3.3 Interview Questions & Answers

### Q1: "What are the different types of UDFs in Spark? When do you use each?"

**Your Answer:**
"There are three types:

1. **Regular Python UDF** (`@udf`) — processes one row at a time. Serializes data via pickle. Slowest. I avoid this whenever possible.

2. **pandas_udf (Scalar)** — processes a batch of rows as a Pandas Series. Uses Apache Arrow for efficient serialization. I use this for ML model inference in my disaggregation pipeline — the PyTorch model runs predictions on a batch of sequences, which is much more efficient than row-by-row.

3. **applyInPandas (Grouped Map)** — receives an entire group as a Pandas DataFrame, returns a Pandas DataFrame. I use this for per-battery cycle estimation because the algorithm is stateful — it needs the entire sorted time-series of a single battery to detect maxima/minima patterns.

The rule of thumb:
- Can you express it in native Spark SQL? → Do that (fastest)
- Need custom per-row logic with external libraries? → `pandas_udf`
- Need complex per-group logic with state? → `applyInPandas`
- Nothing else works? → Regular UDF (last resort)"

### Q2: "Explain how applyInPandas works under the hood."

**Your Answer:**
"When you call `groupBy('uid').applyInPandas(func, schema)`:

1. **Shuffle** — Spark redistributes data so all rows with the same `uid` end up on the same partition
2. **Serialize to Arrow** — each group is converted from Spark's internal format to Apache Arrow columnar format
3. **Deserialize to Pandas** — Arrow data is zero-copy converted to a Pandas DataFrame
4. **Execute Python** — your function runs on the Pandas DataFrame (single-threaded per group)
5. **Serialize back** — the returned Pandas DataFrame is converted to Arrow and back to Spark's format

Performance implications:
- The shuffle is the most expensive step — I mitigate this by `repartition('uid')` BEFORE the groupBy, so the shuffle only happens once and all subsequent `applyInPandas` calls reuse the same partitioning
- The Arrow serialization is efficient (columnar, zero-copy where possible)
- The Python execution is single-threaded per group but runs in PARALLEL across groups (one per executor core)

In my pipeline, I `persist()` the grouped DataFrame after repartitioning, then run 7+ different `applyInPandas` calls on it — this avoids re-shuffling for each one."

### Q3: "How did you integrate a PyTorch model into a Spark pipeline?"

**Your Answer:**
"In my energy disaggregation pipeline, I needed to run a CNN model (1D convolution) to break down total household energy into appliance-level consumption (fridge, geyser, washing machine).

The architecture:
1. **Broadcast the model** — load the PyTorch model on the driver, broadcast the model object to all executors:
   ```python
   model = Model(sequence_length)
   model.load_state_dict(checkpoint['model_state_dict'])
   model_broadcast = spark.sparkContext.broadcast(model)
   ```
2. **Create a pandas_udf** — takes a batch of input sequences and auxiliary features, runs batch prediction:
   ```python
   @pandas_udf(DoubleType())
   def predict_udf(data_batch, aux_batch):
       model = model_broadcast.value
       input_tensor = torch.tensor(np.array(data_batch.tolist()), device=device)
       with torch.no_grad():
           predictions = model(input_tensor, aux_tensor).cpu().numpy().flatten()
       return pd.Series(predictions)
   ```
3. **Apply to DataFrame** — `df.withColumn('predictions', predict_udf(df['grouped_power'], df['x_aux']))`

Key decisions:
- **Broadcast, not load-per-task** — loading the model per task would be extremely slow
- **Batch prediction, not row-by-row** — neural networks are optimized for batch inference
- **CPU fallback** — `device = torch.device('cuda' if ... else 'cpu')` because not all clusters have GPUs
- **torch.no_grad()** — disables gradient computation for inference (saves memory and speeds up)"

### Q4: "Why didn't you use Spark's native MLlib instead of PyTorch?"

**Your Answer:**
"MLlib doesn't support the model architecture we needed. Our disaggregation model is a sequence-to-point CNN — it takes a window of energy readings (1D convolution layers) plus auxiliary features (mean, std, max, min of the window) and predicts the appliance's energy at the center point. This is a custom deep learning architecture that doesn't exist in MLlib.

MLlib is great for classical ML (linear regression, random forests, k-means), but for custom neural networks, you need a deep learning framework. PyTorch was chosen because:
1. The model was developed and trained outside of Spark by the data science team
2. We just needed inference, not training
3. `pandas_udf` provides a clean integration point — handle distribution with Spark, handle inference with PyTorch"

### Q5: "What's the performance difference between regular UDFs and pandas_udf?"

**Your Answer:**
"In practice, `pandas_udf` is 3-100x faster than regular UDFs, depending on the operation. The reason:

- **Regular UDF**: Spark serializes each row to Python (pickle), calls your function, serializes the result back. For 1 billion rows, that's 2 billion serialization operations.
- **pandas_udf**: Spark converts a batch of rows to Arrow format (one operation), zero-copy transfers to Pandas, processes the entire batch, converts back. For 1 billion rows in batches of 10K, that's 200K serialization operations — a 10,000x reduction.

In my disaggregation pipeline, the model prediction processes sequences in batches. Using a regular UDF would mean loading the model input one row at a time, making one prediction, returning it. With `pandas_udf`, I send a batch of 10K sequences, run batch prediction on PyTorch (which internally uses vectorized matrix operations), and return 10K results. The PyTorch batch inference alone is 50-100x faster than sequential prediction."

---

### Scenario-Based Questions

### S1: "You have complex per-group logic that currently uses applyInPandas. Your manager says it's too slow. How do you optimize?"

**Your Answer:**
"I've dealt with this in my battery pipeline. My optimization checklist:

1. **Repartition once, reuse** — `repartition('uid').persist()` before the groupBy. This ensures the shuffle happens ONCE and all subsequent `applyInPandas` calls reuse the same partitioning.

2. **Reduce data before applyInPandas** — use native Spark aggregations for what you can (min, max, avg, count), and only use `applyInPandas` for the truly stateful logic. In my pipeline, SoC/voltage/temperature aggregations are done with Spark's built-in `agg()`, while cycle detection uses `applyInPandas`.

3. **Optimize the Pandas function itself** — profile it. Replace `.iterrows()` with vectorized operations where possible. Use numpy arrays for the heavy loop.

4. **Skip empty/bad groups early** — return `pd.DataFrame()` immediately for batteries with insufficient data. This avoids wasting compute on groups that won't produce results.

5. **Broadcast shared data** — instead of passing large DataFrames through the closure, broadcast them. I broadcast the data_report and metadata DataFrames.

6. **Increase parallelism** — more partitions = more concurrent groups being processed. But balance with partition overhead."

### S2: "When would you choose applyInPandas over a Window function?"

**Your Answer:**
"Window functions are faster because they stay in the JVM and benefit from Catalyst optimization. Use them when you can express the logic as SQL-like window operations (lag, lead, row_number, cumsum, running avg).

Use `applyInPandas` when:
1. **Logic is stateful and complex** — my cycle detection maintains a stack of maxima/minima and makes decisions based on history. You can't express this as a Window function.
2. **Output schema differs from input** — `applyInPandas` can return a completely different schema. My charging_cycles function takes raw battery readings and returns cycle-level summaries (different number of rows AND different columns).
3. **External library needed** — if you need numpy, scipy, or PyTorch inside the logic.

In my pipeline, the basic aggregations (min SoC, max temperature, count) use native Spark `agg()`. The cycle detection uses `applyInPandas`. This hybrid approach gives the best of both worlds."

---

# Section 4 — Spark Performance Optimization

## 4.1 What It Is & Why It Matters

Spark performance optimization is about making your jobs run faster, use less memory, and cost less on a cluster. It is the **single most asked topic in data engineering interviews** because every company has slow Spark jobs.

The three pillars:
1. **Minimize shuffles** — data movement across the network is the #1 bottleneck
2. **Manage memory** — caching, persistence, and avoiding OOM (Out of Memory) errors
3. **Right-size parallelism** — partition count, executor configuration, cluster sizing

**Why interviewers focus on this:** Anyone can write `df.groupBy().agg()`. The difference between a junior and senior data engineer is knowing WHY it's slow and HOW to fix it.

## 4.2 Where You Used It (Your Real Projects)

### 4.2.1 Repartition Before GroupBy (Battery Pipeline)

```python
grouped_df = spark_df.repartition("uid").persist()
grouped_df = grouped_df.groupBy("uid")
```

**What's happening:**
- `repartition("uid")` — redistributes all data so rows with the same battery ID are on the same partition. This triggers a shuffle NOW.
- `.persist()` — caches the repartitioned DataFrame in memory+disk
- Subsequent `applyInPandas` calls DON'T need to shuffle again because data is already partitioned by `uid`

**Why it matters:** Without this, every `applyInPandas` call would trigger its own shuffle. With 7+ calls, that's 7 shuffles of the entire dataset. With repartition+persist, it's just 1 shuffle.

**Trade-off:** The persist consumes memory. If the dataset is too large for memory, it spills to disk (which is why `StorageLevel.MEMORY_AND_DISK` is used, not `MEMORY_ONLY`).

### 4.2.2 Pre-Aggregation Before Pivot (Smart Meter Pipeline)

```python
# BAD: Pivot on raw data (millions of rows)
pivoted = df.groupBy("IMEI", "Vendor", "Date", "Hour").pivot("Register").agg(first("Value"))

# GOOD: Pre-aggregate first, then pivot (reduced dataset)
pre_agg = df.groupBy("IMEI", "Vendor", "Date", "Hour", "Register").agg(
    F.sum("Value").alias("sum_value"),
    F.avg("Value").alias("avg_value"),
)
pivoted = pre_agg.groupBy("IMEI", "Vendor", "Date", "Hour") \
    .pivot("Register", self.BLOCK_REGISTERS) \
    .agg(spark_first("Value"))
```

**Impact:** For 4M meters with 96 readings/day and 5 registers, the raw data has ~2 billion rows. Pre-aggregation reduces it to ~4M * 24 hours * 5 registers = ~480M rows. The pivot then operates on 480M rows instead of 2B — a **75% reduction** in shuffle data.

### 4.2.3 Broadcast Joins (Disaggregation & SLA Pipelines)

```python
# Metadata: ~50K rows (small)
metadata_df = broadcast(metadata_df).persist(StorageLevel.MEMORY_AND_DISK)
metadata_df.count()  # Materialize the broadcast

# Join: large data + small metadata
parsed_data_df = parsed_data_df.join(metadata_df, on=["device"], how="right")
```

**What's happening:**
- `broadcast()` tells Spark to send the entire metadata DataFrame to every executor
- The join happens locally on each executor — NO shuffle of the large DataFrame
- `.count()` forces materialization so the broadcast happens once

**When to use:** When one side of the join is small (< 10MB by default, configurable via `spark.sql.autoBroadcastJoinThreshold`). My metadata DataFrames are typically 50K-200K rows — perfect for broadcast.

**In the SBD pipeline:**
```python
final_sbd_data_df = sbd_data_df.join(
    broadcast(metadata_spark_df),
    on=["IMEI", "Vendor", "Profile"],
    how="right"
)
```

### 4.2.4 Persist and Unpersist (Memory Management)

```python
# Persist when reused multiple times
preprocess_df.persist(StorageLevel.MEMORY_AND_DISK)
preprocess_df.show()  # Triggers materialization

# ... use preprocess_df for fridge, geyser, washing machine predictions ...

# Unpersist when done
preprocess_df.unpersist()
```

**Your pattern across all pipelines:**
- `persist()` after expensive computations that are reused (repartitioned data, joined data)
- `unpersist()` after the last usage to free memory for subsequent stages
- `StorageLevel.MEMORY_AND_DISK` — if memory is full, spill to disk rather than recomputing

**In the disaggregation pipeline:** The preprocessed data is used 3 times (fridge, washing machine, geyser predictions). Without persist, Spark would recompute the preprocessing 3 times.

### 4.2.5 Spark Configurations

```python
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
spark.conf.set("spark.sql.files.ignoreMissingFiles", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

- `ignoreCorruptFiles` / `ignoreMissingFiles` — critical for production pipelines reading from HDFS where files can be corrupted or deleted mid-read
- `arrow.pyspark.enabled` — enables Apache Arrow for `pandas_udf` data transfer (significant speedup)

### 4.2.6 Explicit Schema for Pivot Values

```python
# BAD: Spark discovers pivot values by scanning the data
.pivot("Register")

# GOOD: Provide values explicitly
.pivot("Register", self.BLOCK_REGISTERS)
```

This avoids an extra scan of the entire dataset just to find the unique values of the pivot column. For 4M meters, this saves several minutes.

## 4.3 Interview Questions & Answers

### Q1: "What is a shuffle in Spark? Why is it expensive?"

**Your Answer:**
"A shuffle is when Spark redistributes data across partitions — data moves over the network from one executor to another. It's the most expensive operation because:

1. **Network I/O** — data must be serialized, sent over the network, deserialized
2. **Disk I/O** — shuffle data is written to disk (shuffle files) before being read by the next stage
3. **GC pressure** — creating and destroying many intermediate objects

In my pipeline, the biggest shuffle is the initial `repartition('uid')` for 100K batteries. The trick is to do this shuffle ONCE and persist the result, then run all subsequent operations on the persisted data. I turned 7 shuffles (one per `applyInPandas` call) into 1 shuffle."

### Q2: "How do you decide the number of partitions?"

**Your Answer:**
"The rule of thumb:
- Each partition should be 128MB–256MB for optimal parallelism
- Number of partitions should be 2-3x the total number of cores in the cluster
- Too few partitions → underutilization, potential OOM (partitions too large)
- Too many partitions → overhead of task scheduling, too many small files on write

In my environment with 8 cores and ~50GB memory:
- For battery data (100K batteries, ~10GB/day): 200 partitions works well — each is ~50MB
- For smart meter data (4M meters, ~100GB/day): I let Spark's Adaptive Query Execution (AQE) handle it, or set `spark.sql.shuffle.partitions` to 500-1000

I also use `repartition(1)` when writing small result files (like the last-read-path tracking file) to avoid creating hundreds of tiny files."

### Q3: "Explain StorageLevel options. When do you use MEMORY_AND_DISK vs MEMORY_ONLY?"

**Your Answer:**
"Storage levels control how persisted data is stored:

- `MEMORY_ONLY` — keep in RAM. If it doesn't fit, partitions that don't fit are recomputed when needed. Risk: if memory is tight, you get OOM or constant recomputation.
- `MEMORY_AND_DISK` — keep in RAM, spill overflow to disk. Slower than pure memory but safer. This is what I use everywhere because my data volumes are large relative to available memory.
- `DISK_ONLY` — everything on disk. Slowest but useful for very large datasets.
- `MEMORY_ONLY_SER` / `MEMORY_AND_DISK_SER` — serialized storage. Uses less memory but requires deserialization on read.

In my pipelines, I consistently use `MEMORY_AND_DISK` because:
1. My cluster has limited memory (~50GB total, 8 cores)
2. I persist data that's reused multiple times (repartitioned battery data, preprocessing results)
3. Spilling to disk is acceptable — it's slower than memory but much faster than recomputing a complex join+preprocessing chain"

### Q4: "Your Spark job is running out of memory. How do you debug and fix it?"

**Your Answer:**
"I've faced this multiple times. My debugging approach:

1. **Check the Spark UI** — look at the Storage tab for cached DataFrames consuming memory, the Stages tab for skewed tasks (one partition much larger than others)

2. **Identify the bottleneck:**
   - **Driver OOM** — usually caused by `collect()`, `toPandas()`, or broadcast of a large DataFrame. Fix: reduce what you collect, or increase driver memory.
   - **Executor OOM** — usually caused by a skewed partition (one battery with 10x more data than others) or too many cached DataFrames. Fix: `repartition` to balance, `unpersist` unused DataFrames.

3. **Specific fixes I've applied:**
   - `unpersist()` DataFrames after use — in my disaggregation pipeline, I unpersist `preprocess_df` after all three model predictions are done
   - `repartition` to avoid skew — `repartition('uid')` ensures even distribution
   - Reduce broadcast size — only broadcast the columns you need, not the entire DataFrame
   - Spill to disk — use `MEMORY_AND_DISK` instead of `MEMORY_ONLY`
   - Increase `spark.executor.memoryOverhead` — needed when `pandas_udf` or `applyInPandas` uses large amounts of off-heap memory (PyTorch tensors, numpy arrays)"

### Q5: "What is Adaptive Query Execution (AQE)?"

**Your Answer:**
"AQE is Spark's ability to re-optimize the query plan at runtime based on actual data statistics (not just estimates). It was introduced in Spark 3.0 and is enabled by default in Spark 3.2+.

Key features:
1. **Coalescing shuffle partitions** — if some post-shuffle partitions are too small, AQE merges them. This prevents the 'too many small partitions' problem.
2. **Converting sort-merge joins to broadcast joins** — if one side of a join turns out to be small at runtime, AQE converts it to a broadcast join automatically.
3. **Optimizing skewed joins** — if one partition key has much more data than others, AQE splits that partition into smaller sub-partitions.

In my pipeline, AQE helps with the smart meter data where vendor distribution is skewed — some vendors have 2M meters, others have 50K. Without AQE, the large vendor's partition becomes a bottleneck. With AQE, it gets split automatically."

### Q6: "What is data skew? How do you handle it?"

**Your Answer:**
"Data skew is when some partitions have much more data than others. The job runs as fast as the SLOWEST partition.

In my experience:
- **Battery data**: some OEMs report every 1 second (BMS), others every 10 seconds (Zabbix). A BMS battery has 86,400 readings/day; a Zabbix battery has 8,640. When I repartition by `uid`, BMS battery partitions are 10x larger.
- **Smart meter data**: some vendors have 2M meters, others have 50K. GroupBy vendor creates massive skew.

My solutions:
1. **Salting** — add a random suffix to the key to distribute data across more partitions. For skewed vendor joins:
   ```
   df.withColumn('salt', F.expr('floor(rand() * 10)'))
   ```
2. **Two-phase aggregation** — aggregate within salted groups first, then aggregate across salt values
3. **Filter early** — if you know which groups are large, process them separately with more resources
4. **AQE** — let Spark handle it automatically when possible
5. **Repartition by a higher-cardinality column** — I repartition by `uid` (100K+ values) not by `eid` (5 values)"

---

### Scenario-Based Questions

### S1: "You have a Spark job processing 4M smart meters daily. It takes 3 hours. Your target is 45 minutes. Walk me through your optimization approach."

**Your Answer:**
"This is close to a real situation I faced. My systematic approach:

**Step 1: Profile** — Check Spark UI for:
- Which stages take the most time? (likely the shuffle stages)
- Are there data skew issues? (one task taking 10x longer)
- How much data is being shuffled? (GBs in shuffle read/write)
- Are there spills to disk? (indicates memory pressure)

**Step 2: Reduce shuffle data**
- Pre-aggregate before pivot (reduces 2B rows to 480M)
- Use explicit pivot values (avoids discovery scan)
- Filter early: drop unnecessary columns and rows BEFORE groupBy

**Step 3: Optimize joins**
- Broadcast small tables (metadata, device lists)
- Use `left_anti` join for incremental processing instead of re-processing everything

**Step 4: Memory management**
- Persist strategically — only DataFrames reused multiple times
- Unpersist immediately after last use
- Set appropriate storage levels

**Step 5: Partition tuning**
- `spark.sql.shuffle.partitions` — default is 200, may need 500-1000 for this volume
- Ensure no single partition is > 256MB

**Step 6: Configuration**
- Enable AQE: `spark.sql.adaptive.enabled=true`
- Arrow for UDFs: `spark.sql.execution.arrow.pyspark.enabled=true`
- Increase `spark.executor.memoryOverhead` if using pandas_udf

In my actual experience, the biggest wins came from: pre-aggregation before pivot (saved ~40 minutes), broadcast joins for metadata (saved ~15 minutes), and repartition+persist before multiple applyInPandas calls (saved ~20 minutes)."

### S2: "Your pipeline works fine with 100K devices but breaks with 1M devices. What changed?"

**Your Answer:**
"Several things break at scale:

1. **Driver memory** — if you `collect()` results to the driver for MongoDB writes (like I did in the battery pipeline), 1M devices produces 10x more data. Fix: write directly from executors, or increase driver memory.

2. **Broadcast threshold exceeded** — a metadata DataFrame that was 5MB at 100K becomes 50MB at 1M. It may exceed `spark.sql.autoBroadcastJoinThreshold` (default 10MB), causing Spark to fall back to shuffle join. Fix: increase the threshold or explicitly use `broadcast()`.

3. **Shuffle explosion** — groupBy on 1M groups generates 10x more shuffle data. Fix: increase partition count, use AQE coalescing.

4. **Skew amplification** — at 100K, the largest group might be 2x the average. At 1M, it might be 50x. Fix: salting or repartitioning.

5. **File system pressure** — 1M devices writing to HDFS creates many more files. Fix: coalesce before write, or use bucketing.

The key lesson from my migration experience: always design for 10x your current scale."

---

# Section 5 — Data Pipeline Architecture & ETL Design

## 5.1 What It Is & Why It Matters

Data pipeline architecture is about designing the end-to-end flow: ingestion → processing → storage → serving. Interviewers want to know you can think about the WHOLE system, not just write transformations.

Key concepts:
- **ETL** (Extract, Transform, Load) vs **ELT** (Extract, Load, Transform)
- **Batch vs Streaming** — your pipelines are batch
- **Incremental vs Full Refresh** — whether you reprocess all data or only new data
- **Idempotency** — running the pipeline twice produces the same result
- **Data Quality** — validating data before processing

## 5.2 Where You Used It (Your Real Projects)

### 5.2.1 Incremental Ingestion with Last-Read-Path Tracking

In `data_fetch_pyspark.py`, you built an incremental ingestion system:

```python
# Read the last-read-path tracking file
last_read_path_df = spark.read.parquet(last_read_path)

# For each vendor, find new files since the last read
all_files = all_files_df.inputFiles()
last_path = vendor_lrp.select("FilePath").first()["FilePath"]
new_files = [f for f in all_files if f > last_path]

# Read only the new files
combined_df = spark.read.parquet(*new_files)

# Update the tracking file
last_read_path_df = last_read_path_df.groupBy("Vendor", "Date") \
    .agg(spark_max("FilePath").alias("FilePath"))
last_read_path_df.write.mode("overwrite").parquet(last_read_path)
```

**Interview term:** This is a **bookmark/watermark-based incremental ingestion** pattern. Instead of reprocessing all files every day, you track the last file you read and only process new files.

**Why it matters at scale:** With 4M meters and multiple vendors pushing data throughout the day, re-reading all files would be extremely wasteful. The last-read-path pattern reduces redundant reads by ~60%.

### 5.2.2 Data Quality Gates (Battery Pipeline)

```python
data_report_df = grouped_df.applyInPandas(generate_data_report, schema=data_report_schema)
broadcast_data_report_df = spark.sparkContext.broadcast(data_report_df)

# Inside each processing function:
data_report = data_report_df[data_report_df['uid'] == bat_uid]
if data_report.empty or (data_report['soc'].values[0] < 0.05):
    return pd.DataFrame()  # Skip this battery
```

**Interview term:** This is a **data quality gate** — a checkpoint that prevents bad data from propagating downstream. Each battery is checked for data completeness before processing.

### 5.2.3 Schema-Agnostic Preprocessing (Multi-OEM Normalization)

In `preprocessing.py` / `fetching_hadoop.py`, you handle multiple battery OEM data formats:

```python
def transform_coloumn(df, eid):
    if 'zabbix' in eid:
        df = df.withColumnRenamed("temperature", "max_cell_temperature") \
               .withColumnRenamed("bat_uid", "uid") \
               .withColumn("current_", col("charging_current") - col("discharging_current"))
        df = df.withColumn("battery_status",
            when(col("current_") > 0, "Charging")
            .when(col("current_") < 0, "Discharging")
            .otherwise("Stand By"))
    if eid == 'bms':
        df = df.withColumn("battery_status",
            when(col("battery_status") == "IDLE", "Stand By")
            .when(col("battery_status") == "CHARGING", "Charging")
            ...)
    return df
```

**Interview term:** This is a **normalization layer** or **adapter pattern** — transforming heterogeneous source schemas into a unified canonical schema. The downstream processing logic doesn't need to know which OEM the data came from.

### 5.2.4 Idempotent Writes with Deduplication

In `data_aggregate_pyspark.py`:

```python
def filter_cassandra_data(self, daily_agg_df, req_date, logger):
    existing_devices_df = self.spark.read.parquet(hdfs_path)
    result_agg_df = daily_agg_df.join(
        existing_devices_df.select("IMEI").distinct(),
        on="IMEI", how="left_anti"
    )
    # Update device list
    updated_device_df = existing_devices_df.unionByName(
        result_agg_df.select("IMEI")
    ).dropDuplicates()
```

**Interview term:** This is an **idempotent write** pattern — running the pipeline twice on the same day doesn't insert duplicate records. The device list acts as a "processed" registry.

### 5.2.5 Fan-Out Architecture (Battery Aggregation)

The battery daily aggregation has a distinctive architecture:

```
Raw Data → Repartition by uid → Persist
    ├── applyInPandas(data_report)        → broadcast
    ├── applyInPandas(charging_cycles)     → charging_aggregation
    ├── applyInPandas(discharging_cycles)  → discharging_aggregation
    ├── applyInPandas(charging_energy)     → energy
    ├── applyInPandas(discharging_energy)  → energy
    ├── applyInPandas(standby_hours)       → utilization
    ├── applyInPandas(operational_limits)  → comfort zones
    ├── applyInPandas(fluctuation_count)   → health
    ├── applyInPandas(cell_voltage_stats)  → cell health
    ├── Spark agg(min, max, avg)           → basic metrics
    └── applyInPandas(hashing)             → data integrity
           ↓
    JOIN all results on uid → restructure_schema → write to MongoDB
```

**Interview term:** This is a **fan-out/fan-in** (or **scatter-gather**) architecture. You scatter the work across multiple parallel processing paths, then gather the results into a single output.

## 5.3 Interview Questions & Answers

### Q1: "Describe the data pipeline architecture you built."

**Your Answer:**
"I'll describe my smart meter pipeline, which processes 4M+ meters daily.

**Ingestion layer:** Parquet files land on HDFS from multiple vendors throughout the day. My pipeline uses an incremental ingestion pattern — I maintain a last-read-path tracking file per vendor that records the last parquet file processed. On each run, I discover new files by comparing file paths against the tracked position.

**Processing layer:** Three types of aggregation:
1. **Block** — hourly energy and voltage from load profile data. Uses pre-aggregate-then-pivot optimization.
2. **Daily** — daily energy consumption computed as delta between consecutive days' cumulative readings.
3. **Monthly** — billing period energy from billing profile data, using delta from previous month's cumulative.

**SLA layer:** Two monitoring pipelines (ODR and SBD) compute data completeness metrics — what percentage of expected readings were actually received, broken down by hour windows and vendor.

**Storage layer:** Results go to Cassandra (for real-time dashboard queries) and HDFS parquet (for historical analysis and reprocessing). SLA reports also go to Azure Blob for CSV exports.

**Orchestration:** Shell scripts schedule each pipeline (block runs at 3 AM, daily runs after block, SLA runs after daily). Each writes logs to HDFS for auditing."

### Q2: "What is incremental processing? How did you implement it?"

**Your Answer:**
"Incremental processing means only processing NEW data since the last run, not reprocessing everything.

My implementation uses a **file-level bookmark**:
1. Each vendor's data arrives as timestamped parquet files on HDFS
2. I maintain a tracking DataFrame with columns: (Vendor, Date, FilePath) — recording the last file processed
3. On each run, I list all files for the vendor/date, filter for files with paths > last-read-path
4. Only those new files are read and processed
5. After successful processing, the tracking file is updated

This is different from event-time watermarking (used in streaming). Since my pipelines are batch, file-path ordering is sufficient. The parquet files are named with timestamps, so lexicographic comparison gives chronological ordering.

Benefit: for vendors pushing data in hourly batches, I process only the latest batch instead of re-reading the entire day's data."

### Q3: "How do you ensure your pipeline is idempotent?"

**Your Answer:**
"Idempotency means running the pipeline multiple times produces the same result. This is critical because jobs can fail and need to be restarted.

My approach:
1. **Deduplication before write** — I maintain a 'processed devices' list on HDFS. Before writing to Cassandra, I `left_anti` join against this list to exclude already-processed devices.
2. **Overwrite semantics for tracking files** — the last-read-path and device list files are written with `mode('overwrite')`, so a re-run replaces them with the correct state.
3. **Upsert semantics for Cassandra** — Cassandra's `INSERT` is an upsert by design (writing the same primary key just updates the row), so duplicate writes don't create duplicate records.
4. **Date-partitioned HDFS writes** — each day's results go to a date-specific folder. Re-running overwrites that folder only.

One subtlety: the last-read-path tracking file has a race condition if the pipeline fails AFTER processing data but BEFORE updating the tracker. On restart, it would re-process the same files. Since my downstream writes are idempotent (Cassandra upserts), this is acceptable — it's redundant work, not incorrect results."

### Q4: "How do you handle schema evolution when source data changes?"

**Your Answer:**
"This happened in my battery project when a new OEM (Zabbix) was onboarded. Their data had a completely different schema from the existing BMS data:
- BMS: separate `current_` column, `battery_status` as text (IDLE/CHARGING/DISCHARGING)
- Zabbix: separate `charging_current` and `discharging_current` columns, no `battery_status`

My solution: a **normalization layer** that transforms each OEM's schema into a canonical schema:
```python
if 'zabbix' in eid:
    df = df.withColumn('current_', col('charging_current') - col('discharging_current'))
    df = df.withColumn('battery_status', when(col('current_') > 0, 'Charging')...)
elif eid == 'bms':
    df = df.withColumn('battery_status', when(col('battery_status') == 'IDLE', 'Stand By')...)
```

All downstream logic (cycle detection, energy estimation, KPI generation) works on the canonical schema. Adding a new OEM requires only adding a new normalization rule, not changing any downstream logic.

For the smart meter pipeline, `match_columns()` handles missing columns by adding them as nulls with the correct type — this handles schema differences between vendors."

### Q5: "Batch vs streaming — when would you choose each?"

**Your Answer:**
"My pipelines are all batch. The decision came down to:

**Why batch was right for battery analytics:**
- Battery KPIs are daily aggregates (charging hours, energy consumed) — no need for real-time
- The downstream consumers (dashboards, alerts) update once per day
- Data arrives in HDFS as daily partitions — naturally batch
- Batch is simpler to debug, test, and maintain

**Why batch was right for smart meter SLA:**
- SLA metrics are computed at the end of the business day (7hr, 8hr, ... 24hr windows)
- The reporting is daily — stakeholders review yesterday's completeness
- Regulatory reporting is monthly — aggregated from daily batches

**When I would choose streaming:**
- If we needed real-time battery alerts (overheating, voltage breach) within minutes
- If smart meter data needed to be available for billing in real-time
- If data freshness was a competitive advantage

The move to Databricks opens up Structured Streaming as an option — I could convert my daily batch pipelines to micro-batch streaming with minimal code changes if the business needs change."

---

### Scenario-Based Questions

### S1: "Your daily pipeline failed at 4 AM. Data was partially processed. How do you recover?"

**Your Answer:**
"This has happened. My recovery approach:

1. **Check the logs** — each pipeline writes logs to HDFS with timestamps. Identify which stage failed and why.

2. **Determine the state:**
   - If it failed BEFORE writing results → safe to re-run from scratch
   - If it failed AFTER partial writes → need to handle duplicates

3. **Re-run with idempotency:**
   - The last-read-path tracker was NOT updated (since the job failed), so re-running will re-process the same data
   - Cassandra writes are upserts — duplicates are harmless
   - HDFS writes use `mode('overwrite')` on date-partitioned paths — re-writing overwrites the partial output

4. **Fix the root cause** — common failures:
   - HDFS path not found → vendor didn't push data, log a warning and skip
   - OOM on executor → increase memory or add repartitioning
   - Corrupt parquet file → `ignoreCorruptFiles=true` handles this

5. **Validate after recovery** — compare output row counts with expected counts (e.g., should be close to yesterday's count for the same vendor)"

### S2: "Design a data pipeline for processing IoT device data from 500K devices, each sending data every minute."

**Your Answer:**
"This is very close to my battery analytics pipeline. Here's my design:

**Ingestion:**
- Devices send data to an API gateway → messages queue in Kafka → landed as parquet on HDFS (daily partitions)
- Or: direct API ingestion (what we did — Python script pulling from REST API → writing to HDFS)

**Daily Batch Processing:**
1. Read day's data from HDFS: `spark.read.parquet(hdfs_path/{year}/{month}/{day})`
2. Normalize schemas (handle multiple device types)
3. Data quality check — compute completeness per device, broadcast result
4. Repartition by device_id, persist
5. Compute per-device KPIs using applyInPandas for complex logic, native Spark agg for simple metrics
6. Join all results
7. Write to operational DB (Cassandra/MongoDB) for dashboards

**SLA Monitoring:**
- Separate pipeline computing what % of devices reported data
- Written to Cassandra for real-time dashboards

**Resource estimation:**
- 500K devices × 1440 readings/day × 10 columns × 8 bytes ≈ 57GB/day raw
- Processing needs: ~8 cores, 64GB memory, 2-3 hours batch window
- Scale-up options: more cores for parallelism, more memory for caching"

---

# Section 6 — Databases: Cassandra, MongoDB, HDFS

## 6.1 What They Are & Why They Matter

### Apache Cassandra
- **Distributed wide-column store** optimized for write-heavy workloads and time-series data
- **No single point of failure** — every node is equal (peer-to-peer architecture)
- **Partitioned by partition key** — data for the same partition key lives on the same node
- **Eventually consistent** (tunable) — trades consistency for availability

### MongoDB
- **Document-oriented NoSQL database** — stores data as flexible JSON-like documents
- **Rich query support** — can query nested fields, arrays, embedded documents
- **Good for complex, varying schemas** — each document can have different fields

### HDFS (Hadoop Distributed File System) + Parquet
- **Distributed file system** for storing large files across a cluster
- **Parquet** — columnar storage format optimized for analytical queries (column pruning, predicate pushdown)
- **Write once, read many** — HDFS is optimized for large sequential reads

## 6.2 Where You Used Each (Your Real Projects)

### Cassandra — SLA Dashboard Serving Layer

In the smart meter pipeline, all SLA results go to Cassandra:

```python
# Block aggregation results
data_push_instance.push_block_data_to_cassandra(result_df, 'dap_block', req_date, logger)

# SBD completeness reports
sbd_report_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table=config.cassandra['sbd_table'], keyspace=config.cassandra['keyspace']) \
    .save()

# ODR completeness reports
pivot_report_df.write \
    .format("org.apache.spark.sql.cassandra") \
    .mode("append") \
    .options(table=config.cassandra['odr_table'], keyspace=config.cassandra['keyspace']) \
    .save()
```

**Why Cassandra for SLA:**
- Time-series queries: "show me completeness for vendor X from Jan 1 to Jan 31"
- High write throughput: pipeline writes millions of rows per day
- Partition key is likely (vendor, date) — queries by vendor+date are fast
- Dashboards need low-latency reads (< 10ms)

### MongoDB — Battery KPI Storage

In the battery pipeline, aggregated KPIs go to MongoDB:

```python
collection = client['smart_battery']['daily_summaries_temp_dr']
x = collection.insert_many(results_dict)

collection = client['smart_battery']['recommendations_temp_dr']
x = collection.insert_many(results_dict)
```

**Why MongoDB for battery KPIs:**
- The aggregated schema is deeply nested (charging_cycles containing arrays of cycle objects, each with start_time, end_time, c_rate, etc.)
- MongoDB natively stores this as a JSON document — no need to flatten
- The downstream full-stack application queries by `bat_uid` and `summary_date` — MongoDB indexes handle this efficiently
- Flexible schema: different OEMs have different fields (BMS has cell_voltage_mv, Zabbix doesn't)

### HDFS + Parquet — Bulk Storage and Processing Layer

Every pipeline reads from and writes to HDFS parquet:

```python
# Read
spark_df = spark.read.parquet(f"hdfs://{hadoop_uri}:8020/.../eid={eid}/year={year}/month={month}/day={day}")

# Write intermediate results
df.write.mode("overwrite").parquet(f"{parent_folder}/{formatted_date}/{filename}")

# Write last-read-path tracking
last_read_path_df.write.mode("overwrite").parquet(last_read_path)
```

**Why HDFS + Parquet:**
- Parquet is columnar — reading only `soc`, `event_time`, `current_` skips all other columns (column pruning)
- Parquet supports predicate pushdown — `filter(col('eid') == 'bms')` is pushed into the file reader
- HDFS provides distributed storage with replication (fault tolerance)
- Partitioned by year/month/day/eid — enables partition pruning

## 6.3 Interview Questions & Answers

### Q1: "Why did you choose Cassandra over a relational database for your SLA data?"

**Your Answer:**
"Three reasons:

1. **Write throughput** — my pipeline writes millions of SLA records daily (one per device per profile per day). Cassandra handles high write volumes because writes go to the commit log and memtable (in-memory), then flush to SSTable asynchronously. A relational DB would struggle with lock contention at this volume.

2. **Time-series queries** — SLA dashboards query by vendor + date range. Cassandra's partition key (vendor) and clustering key (date) make these queries extremely fast — it reads a contiguous range of data from a single partition.

3. **Horizontal scaling** — as we onboard more meters (from 1M to 4M), Cassandra scales by adding nodes. No downtime, no re-sharding. A relational DB would need vertical scaling or complex sharding.

The trade-off: Cassandra doesn't support complex joins or ad-hoc queries well. For analytical queries, I keep data in HDFS/Parquet and use Spark."

### Q2: "Explain Cassandra's partition key and clustering key."

**Your Answer:**
"The partition key determines WHICH node stores the data. The clustering key determines HOW data is sorted within that partition.

In my SLA tables:
- **Partition key**: `vendor` — all data for a vendor lives on the same node
- **Clustering key**: `timestamp` — within a vendor's partition, records are sorted by date

This means:
- `SELECT * FROM dap_sbd WHERE vendor = 'VendorA' AND timestamp >= '2025-01-01'` → reads one partition, sequential scan — fast
- `SELECT * FROM dap_sbd WHERE timestamp >= '2025-01-01'` → full table scan across all partitions — slow (no partition key specified)

Design principle: model your tables around your query patterns, not around relationships. If the dashboard always queries by vendor + date, that's your partition + clustering key."

### Q3: "When would you use MongoDB vs Cassandra?"

**Your Answer:**
"From my experience:

**MongoDB when:**
- Data has complex, nested, varying schemas (my battery KPIs have charging_cycles arrays, nested aggregated objects, optional cell_voltage fields)
- You need rich querying on nested fields (query by `aggregated.temperature_celsius.max > 60`)
- Schema evolves frequently (new OEMs add different fields)
- Read patterns are key-value-like (get by bat_uid + date)

**Cassandra when:**
- Data is tabular / flat (my SLA metrics: vendor, date, completeness %)
- Write volume is very high (millions of inserts/day)
- Queries are predictable and partition-key-oriented
- You need time-series ordering (clustering by timestamp)
- Availability > consistency (SLA dashboards can show slightly stale data)

In my system, I use BOTH:
- MongoDB for battery team: complex document model, moderate write volume
- Cassandra for meter team: flat model, high write volume, time-series queries"

### Q4: "Why Parquet? What are its advantages over CSV or JSON?"

**Your Answer:**
"Parquet is a columnar binary format. Advantages:

1. **Column pruning** — reading 5 columns out of 30 reads only those 5 columns' data. My battery data has 30+ columns but most aggregations need 6-7. Parquet skips the rest entirely. CSV would read every column.

2. **Predicate pushdown** — `filter(col('soc') > 50)` is pushed into the Parquet reader. Only row groups containing matching data are read. Enormous savings on large datasets.

3. **Compression** — columnar storage compresses well because similar values are stored together. My battery data compresses ~5:1 vs raw CSV.

4. **Schema enforcement** — Parquet files carry their schema. No parsing errors, no type mismatches.

5. **Partitioning** — Parquet files on HDFS are partitioned by year/month/day. Reading one day's data only touches that day's directory.

In my pipeline, switching from reading CSV to Parquet reduced I/O time by ~80% and storage by ~70%."

### Q5: "How do you write from Spark to Cassandra efficiently?"

**Your Answer:**
"Using the Spark-Cassandra connector:

```python
df.write.format('org.apache.spark.sql.cassandra') \
    .mode('append') \
    .options(table='dap_block', keyspace='smart_meter') \
    .save()
```

Optimization considerations:
1. **Batch size** — the connector batches writes to Cassandra. Default works for most cases, but for very high volumes you might tune `spark.cassandra.output.batch.size.rows`.

2. **Parallelism** — ensure your Spark DataFrame has enough partitions so writes are distributed across Cassandra nodes. Too few partitions → hotspot on one Cassandra node.

3. **Token-aware routing** — the connector is token-aware, meaning it writes data directly to the Cassandra node that owns the partition. This avoids network hops.

4. **Consistency level** — for my SLA writes, I use LOCAL_QUORUM (write to majority of replicas in the local datacenter). This balances durability with write speed.

5. **Deduplication** — since Cassandra writes are upserts (same primary key = update), running the pipeline twice doesn't create duplicates. This makes my pipeline naturally idempotent."

---

### Scenario-Based Questions

### S1: "You need to store data that's queried in two different ways: (a) by device_id for device details, and (b) by date for daily reports. How do you design this in Cassandra?"

**Your Answer:**
"In Cassandra, you create separate tables for each query pattern:

**Table 1: device_daily_summary** (query by device + date)
- Partition key: device_id
- Clustering key: date (DESC)
- Use case: 'Show me device X's metrics for the last 7 days'

**Table 2: daily_report** (query by date + vendor)
- Partition key: (date, vendor)
- Clustering key: device_id
- Use case: 'Show me all devices for vendor Y on date Z'

This is called **query-driven modeling** — you denormalize and duplicate data to optimize for your access patterns. The same data is written to both tables by the pipeline. Storage is cheap; slow queries are expensive.

In my SLA pipeline, I effectively have this pattern: the detailed per-device data is in HDFS (for reprocessing), and the aggregated per-vendor data is in Cassandra (for dashboards)."

---

# Section 7 — Terminology Mapping Table

> **Purpose:** You have hands-on experience but may lack the formal vocabulary interviewers expect. This table maps what you DO to what it's CALLED.

| # | What You Did (Your Words) | Interview Term | Where You Used It |
|---|---------------------------|----------------|-------------------|
| 1 | "I used groupby and applied logic on each battery" | **Grouped Map UDF / applyInPandas** | Battery daily aggregation — cycle detection, energy estimation |
| 2 | "I read parquet files from HDFS" | **Distributed File System I/O with Columnar Storage** | All pipelines — `spark.read.parquet(hdfs_path)` |
| 3 | "I fixed slow jobs by splitting data across workers" | **Data Partitioning / Shuffle Optimization** | `repartition("uid")` before groupBy in battery pipeline |
| 4 | "I stored results in Cassandra" | **Sink Stage of ETL Pipeline / Materialized View** | SLA scripts writing to `dap_block`, `dap_sbd` tables |
| 5 | "I tracked which files I already processed" | **Incremental Ingestion with Bookmarking/Watermarking** | `data_fetch_pyspark.py` — last-read-path tracking |
| 6 | "I converted Python scripts to Spark" | **Migration from Single-Node to Distributed Computing / Horizontal Scaling** | Battery pipeline: Pandas → PySpark migration |
| 7 | "I cached data so it doesn't recompute" | **Persist / Materialization / Caching** | `persist(StorageLevel.MEMORY_AND_DISK)` across all pipelines |
| 8 | "I sent a small table to all workers" | **Broadcast Variable / Broadcast Join** | `broadcast(metadata_df)` in disaggregation and SBD |
| 9 | "I handled different data formats from different vendors" | **Schema Normalization / Adapter Pattern / Canonical Data Model** | `preprocessing.py` — BMS vs Zabbix unification |
| 10 | "I checked data quality before processing" | **Data Quality Gate / Validation Layer** | Battery data report — skip batteries with < 5% completeness |
| 11 | "I made sure rerunning doesn't create duplicates" | **Idempotent Pipeline / Exactly-Once Semantics** | `left_anti` join against processed device list |
| 12 | "I turned rows into columns" | **Pivot / Transpose / Reshaping** | Block aggregation — Register values becoming column names |
| 13 | "I combined results from multiple processes" | **Fan-Out/Fan-In / Scatter-Gather Architecture** | Battery pipeline — 7+ applyInPandas → join all on uid |
| 14 | "I removed rows already in the database" | **Deduplication / Left Anti Join** | `filter_cassandra_data` — exclude already-processed IMEIs |
| 15 | "I computed running totals / looked at previous row" | **Window Function (lag, lead, row_number, cumsum)** | Monthly billing — `row_number().over(w).filter('rn = 1')` |
| 16 | "I subtracted today's reading from yesterday's" | **Delta/Differential Computation** | Daily aggregation — cumulative reading differences |
| 17 | "I ran ML model predictions inside Spark" | **Distributed Model Inference / pandas_udf with PyTorch** | Disaggregation — CNN seq2point inference |
| 18 | "I loaded the model once and sent it to all workers" | **Model Broadcasting / Distributed Model Serving** | `spark.sparkContext.broadcast(model)` in disaggregation |
| 19 | "I computed how long battery was in safe range" | **Operational Threshold / Comfort Zone Analysis** | `operational_limits` — SoC 20-80%, temp 0-40°C |
| 20 | "I found charging and discharging patterns" | **Cycle Detection / Peak-Valley Detection Algorithm** | Battery cycle estimation — maxima/minima detection |
| 21 | "I skipped bad data rows" | **Null Handling / Data Filtering / Predicate Pushdown** | `dropna(subset=['soc'])`, `filter(col('soc').isNotNull())` |
| 22 | "I wrote the output to different places" | **Multi-Sink Pipeline / Polyglot Persistence** | Results → Cassandra + HDFS + Azure Blob simultaneously |
| 23 | "I aggregated before pivot to make it faster" | **Pre-Aggregation / Combiner Pattern / Map-Side Reduce** | Block aggregation — `groupBy(Register).agg()` before `pivot()` |
| 24 | "I gave explicit values to pivot" | **Explicit Pivot Values / Avoiding Discovery Scan** | `.pivot("Register", self.BLOCK_REGISTERS)` |
| 25 | "I freed memory after I was done with data" | **Resource Lifecycle Management / Unpersist** | `preprocess_df.unpersist()` after model predictions |
| 26 | "I hashed the data for integrity check" | **Data Fingerprinting / Checksum / Change Detection** | `hashing_func` — SHA-256 hash of battery daily data |
| 27 | "I handled missing time gaps in the data" | **Data Imputation / Gap Handling / Time-Series Interpolation** | Battery: capping time_diff at 3x data_freq for data loss |
| 28 | "I structured the output into nested JSON" | **Schema Restructuring / Nested Document Modeling** | `restructure_schema` — building nested Spark structs |
| 29 | "I used if-else to clean different vendor data" | **Conditional Transformation / Schema Reconciliation** | `when/otherwise` chains in preprocessing |
| 30 | "I organized results by date folders" | **Partition-Based Storage / Hive-Style Partitioning** | HDFS: `year={}/month={}/day={}` directory structure |
| 31 | "I ran the same logic for fridge, geyser, washing machine" | **Parameterized Pipeline / Template Method Pattern** | `Disaggregate(vendor, "Fridge", 7)` — same class, different params |
| 32 | "I computed SLA % for different time windows" | **Service Level Agreement Monitoring / Data Completeness Metrics** | SBD — 7hr, 8hr, 11hr, 12hr, 22hr, 24hr completeness |

---

# Section 8 — Learning Resources

## 8.1 Spark Internals (MUST LEARN — You Use Spark Daily But Need to Explain HOW It Works)

### YouTube

| Resource | Why It's Useful | What to Focus On |
|----------|----------------|------------------|
| **[Rock the JVM — Spark Essentials](https://youtube.com/rockthejvm)** | Daniel Ciocirlan explains Spark internals with code. Best balance of theory + practice. | Watch: "How Spark Executes a Query", "Spark Joins Internals", "Spark Partitioning" |
| **[Data Savvy — Spark Performance Tuning](https://youtube.com/@datasavvy)** | Hindi + English. Very practical, focuses on optimization. | Watch: "Shuffle in Spark Explained", "Broadcast Join", "AQE Deep Dive" |
| **[Advancing Analytics — Spark Under the Hood](https://youtube.com/@AdvancingAnalytics)** | Deep dives into Catalyst optimizer, Tungsten, physical plans. | Watch: "How Spark SQL Works", "Understanding Spark Physical Plans" |

### Key Topics to Study

1. **DAG, Stages, Tasks** — When you write `df.groupBy().agg().join().write()`, Spark creates a DAG (Directed Acyclic Graph). Each wide transformation (shuffle) creates a new stage. Within each stage, tasks run in parallel (one per partition). You MUST be able to draw this on a whiteboard.

2. **Catalyst Optimizer** — Spark's query optimizer. It converts your logical plan into an optimized physical plan. Key optimizations: predicate pushdown (filter early), column pruning (read only needed columns), join reordering. When asked "how does Spark optimize queries?", this is the answer.

3. **Tungsten Execution Engine** — Spark's memory management and code generation layer. It avoids JVM object overhead by using off-heap memory and generating specialized bytecode. This is why Spark DataFrames are faster than RDDs.

4. **Shuffle Architecture** — Map tasks write shuffle files to local disk. Reduce tasks fetch these files over the network. The shuffle is the boundary between stages. Understanding this explains why `repartition` + `persist` saves time.

5. **Adaptive Query Execution (AQE)** — Runtime optimization in Spark 3.x. Coalesces small partitions, converts joins, handles skew. Since you use Spark 3.x, you benefit from this — know how to explain it.

### Courses

| Course | Platform | Cost | Why |
|--------|----------|------|-----|
| **Spark and Python for Big Data with PySpark** (Jose Portilla) | Udemy | ~$15 on sale | Good refresher, covers basics through optimization. Skip if you know the basics, jump to optimization sections. |
| **Apache Spark 3 — Beyond Basics** (Prashant Pandey) | Udemy | ~$15 | Focuses on internals: DAG, stages, memory management. Fills your knowledge gaps. |
| **Databricks Academy — Apache Spark Programming** | Databricks | Free | Official content, covers Spark on Databricks specifically. Good for Databricks interviews. |

## 8.2 PySpark Specific (Strengthen What You Already Do)

### YouTube

| Resource | Why It's Useful | What to Focus On |
|----------|----------------|------------------|
| **[Krish Naik — PySpark Tutorials](https://youtube.com/@krishnaik06)** | Step-by-step PySpark tutorials in Hindi/English. Practical. | Watch: "PySpark Window Functions", "PySpark Joins", "PySpark UDFs" |
| **[Data Engineering Simplified](https://youtube.com/@dataengineering)** | Short, focused videos on specific PySpark operations. | Watch: "Pivot in PySpark", "Broadcast Join", "applyInPandas" |

### Key Topics to Study

1. **All join types with examples** — Be able to explain inner, outer, left, right, cross, left_anti, left_semi with examples from YOUR projects
2. **Window functions** — `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `sum/avg over window`. Practice writing them from memory.
3. **Aggregation functions** — `groupBy().agg()`, `pivot`, `rollup`, `cube`. Know when each is appropriate.
4. **UDF types** — Regular UDF vs pandas_udf vs applyInPandas. Performance comparison. When to use each.

## 8.3 Data Pipeline Design (Level Up Your Architecture Vocabulary)

### YouTube

| Resource | Why It's Useful | What to Focus On |
|----------|----------------|------------------|
| **[Seattle Data Guy](https://youtube.com/@SeattleDataGuy)** | Practical data engineering advice from an industry practitioner. | Watch: "Data Pipeline Design Patterns", "Batch vs Streaming", "Data Quality" |
| **[Zach Wilson — Data Engineering](https://youtube.com/@EcZachly)** | Former Netflix data engineer. Real-world pipeline design. | Watch: "How to Design a Data Pipeline", "Idempotent Pipelines" |
| **[Darshil Parmar](https://youtube.com/@DarshilParmar)** | Hindi. Great for interview prep, covers system design for DE. | Watch: "Data Engineering Interview Questions", "Pipeline Architecture" |

### Books / Articles

| Resource | Why |
|----------|-----|
| **Fundamentals of Data Engineering** (Joe Reis, Matt Housley) | THE book for data engineering concepts. Read chapters on: Storage, Ingestion, Transformation, Serving. Maps exactly to what you do. |
| **Designing Data-Intensive Applications** (Martin Kleppmann) | Advanced. Read chapters on: Storage Engines (explains Cassandra/MongoDB internals), Batch Processing (explains Spark's model). |

## 8.4 Cassandra & MongoDB (Know Enough for Interviews)

### YouTube

| Resource | Why It's Useful | What to Focus On |
|----------|----------------|------------------|
| **[DataStax Academy — Cassandra Fundamentals](https://academy.datastax.com/)** | Free. Official Cassandra training. | Watch: "Data Modeling", "Partition Keys", "Query Patterns" |
| **[Nana — MongoDB Crash Course](https://youtube.com/@TechWorldwithNana)** | Quick, practical MongoDB overview. | Watch: "MongoDB in 1 Hour", focus on document design and indexing |

### Key Topics

1. **Cassandra**: Partition key vs clustering key, consistency levels, denormalization, write path (commit log → memtable → SSTable), compaction strategies
2. **MongoDB**: Document design, embedded vs referenced documents, indexes (single field, compound, text), aggregation pipeline

## 8.5 Study Priority Order

Given your current experience level, here is the recommended priority:

| Priority | Topic | Time | Reason |
|----------|-------|------|--------|
| 1 | Spark Internals (DAG, Stages, Catalyst) | 1 week | You use Spark daily but need to EXPLAIN how it works internally |
| 2 | Spark Optimization (Shuffle, Partitioning, AQE) | 1 week | Most asked topic in interviews. Connect theory to your real optimizations |
| 3 | Data Pipeline Design Patterns | 3-4 days | Learn the vocabulary for what you already do (idempotency, incremental, fan-out) |
| 4 | PySpark Advanced (Window, joins, UDFs) | 3-4 days | Practice writing them from memory, not just reading code |
| 5 | Cassandra Data Modeling | 2-3 days | Know partition key design, query-driven modeling |
| 6 | SQL (if weak) | 1 week | Many interviews have SQL rounds. Practice on LeetCode/HackerRank |
| 7 | System Design for Data Engineering | Ongoing | Watch 1-2 videos per week on pipeline design, build vocabulary |

---

**END OF DOCUMENT 1**

> Next: See `Databricks_Interview_Guide.md` for the Databricks-specific preparation guide.
