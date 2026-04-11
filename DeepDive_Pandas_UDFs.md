# Pandas UDFs and applyInPandas -- A Complete Deep Dive

**From Python UDF Overhead to Arrow-Powered Vectorized Processing**

---

## Table of Contents

1. [Why UDFs Exist](#1-why-udfs-exist)
2. [Regular Python UDFs](#2-regular-python-udfs)
3. [Apache Arrow -- The Bridge](#3-apache-arrow----the-bridge)
4. [Pandas UDFs (Vectorized UDFs)](#4-pandas-udfs-vectorized-udfs)
5. [applyInPandas -- Grouped Map](#5-applyinpandas----grouped-map)
6. [mapInPandas -- Streaming Map](#6-mapinpandas----streaming-map)
7. [Performance Comparison](#7-performance-comparison)
8. [Type Mapping](#8-type-mapping)
9. [Complex Use Cases](#9-complex-use-cases)
10. [Error Handling and Testing](#10-error-handling-and-testing)
11. [Anti-Patterns and Pitfalls](#11-anti-patterns-and-pitfalls)
12. [Quick-Reference Cheat Sheet](#12-quick-reference-cheat-sheet)

---

## 1. Why UDFs Exist

### 1.1 The Limits of Built-in Functions

Spark provides hundreds of built-in functions (`pyspark.sql.functions`): `sum`, `avg`, `when`, `regexp_extract`, `window`, `explode`, etc. These run entirely in the JVM, are optimized by Catalyst, and are the fastest option.

But sometimes you need custom logic that built-in functions cannot express:
- **Custom ML inference**: Run a PyTorch model on each row or group
- **Complex statistical computations**: Scipy functions, custom rolling statistics
- **Library integration**: Use Pandas, NumPy, or domain-specific Python libraries
- **Business logic**: Complex conditional logic that would require dozens of nested `when()` calls

### 1.2 The UDF Spectrum

```
Speed:  Fastest ──────────────────────────────────────────── Slowest
        Built-in Functions  >  Pandas UDF  >  applyInPandas  >  Python UDF  >  RDD map
        
        [JVM-only]            [Arrow+Pandas]  [Arrow+Pandas]    [Pickle+Python]  [Pickle+Python]
```

---

## 2. Regular Python UDFs

### 2.1 How They Work

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def categorize(value):
    if value > 100:
        return "high"
    elif value > 50:
        return "medium"
    else:
        return "low"

df = df.withColumn("category", categorize(col("value")))
```

Behind the scenes, for EACH partition:
```
JVM Executor
  │
  ├── Serialize partition data (pickle, row by row)
  ├── Send over socket to Python worker process
  │
  ▼
Python Worker
  ├── Deserialize rows (pickle)
  ├── For each row: call categorize(value)
  ├── Serialize results (pickle)
  ├── Send over socket back to JVM
  │
  ▼
JVM Executor
  └── Deserialize results, continue processing
```

### 2.2 Why Regular UDFs Are Slow

1. **Row-by-row processing**: Each row is serialized individually (Python object overhead per row)
2. **Pickle serialization**: Converting between JVM types and Python objects is expensive
3. **Python process overhead**: Spawning/maintaining Python workers on each executor
4. **No Catalyst optimization**: Spark cannot look inside the UDF to optimize (no predicate pushdown, no constant folding)
5. **GIL**: Each Python worker is single-threaded

Benchmark: A regular UDF is typically **5-100x slower** than the equivalent built-in function.

### 2.3 When You Have No Choice

Sometimes a regular UDF is the only option:
- When you need to return a complex type that Pandas UDFs don't support well
- When the function has significant state or side effects
- When the computation cannot be vectorized (depends on previous row's result)

---

## 3. Apache Arrow -- The Bridge

### 3.1 What Is Arrow

Apache Arrow is a **cross-language columnar memory format** for flat and hierarchical data. It is the key technology that makes Pandas UDFs fast.

```
Row Format (pickle):          Columnar Format (Arrow):
Row 1: {id: 1, val: 10.5}    Column "id":  [1, 2, 3, 4, 5]
Row 2: {id: 2, val: 20.3}    Column "val": [10.5, 20.3, 15.7, 8.2, 30.1]
Row 3: {id: 3, val: 15.7}
Row 4: {id: 4, val: 8.2}
Row 5: {id: 5, val: 30.1}
```

### 3.2 Why Arrow Is Fast

1. **Columnar layout**: Values of the same type are stored contiguously. This is cache-friendly and allows SIMD (CPU processes multiple values per instruction).

2. **Zero-copy**: Arrow data in JVM memory can be read by Python without copying. The JVM and Python process share the same memory region.

3. **Batch transfer**: Instead of serializing one row at a time, Arrow transfers entire columns of data in one batch (typically 10,000 rows per batch).

4. **Native Pandas integration**: Pandas internally uses NumPy arrays, and Arrow can convert to/from NumPy without per-element overhead.

### 3.3 Arrow Data Flow

```
JVM Executor                    Python Worker
+------------------+            +------------------+
| Spark DataFrame  |            | Pandas DataFrame |
| (UnsafeRow)      |            | (NumPy arrays)   |
|                  |            |                  |
| Convert to Arrow | ------→   | Arrow to Pandas  |
| RecordBatch      | (IPC)     | (zero-copy)      |
| (columnar)       |            |                  |
|                  | ←------   | Pandas to Arrow  |
| Arrow to         | (IPC)     | (zero-copy)      |
| UnsafeRow        |            |                  |
+------------------+            +------------------+
```

### 3.4 Enabling Arrow

```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", True)
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", True)
spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", 10000)  # rows per batch
```

---

## 4. Pandas UDFs (Vectorized UDFs)

### 4.1 Series to Series

The most common type. Takes one or more Pandas Series and returns a Pandas Series of the same length.

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def celsius_to_fahrenheit(celsius: pd.Series) -> pd.Series:
    return celsius * 9 / 5 + 32

df = df.withColumn("temp_f", celsius_to_fahrenheit(col("temp_c")))
```

How it works:
1. Spark splits partition data into Arrow batches (e.g., 10,000 rows each)
2. Each batch is converted to a Pandas Series via Arrow (fast, zero-copy)
3. Your function receives the entire batch as a Series (not row-by-row)
4. Your function returns a Series of the same length
5. The result is converted back to Arrow and then to Spark's UnsafeRow

### 4.2 Multiple Input Columns

```python
@pandas_udf("double")
def weighted_score(score: pd.Series, weight: pd.Series) -> pd.Series:
    return score * weight / weight.sum()

df = df.withColumn("weighted", weighted_score(col("score"), col("weight")))
```

### 4.3 Iterator of Series

For expensive initialization (loading a model, establishing a connection), use the iterator variant to amortize setup cost:

```python
from typing import Iterator

@pandas_udf("double")
def predict(batch_iter: Iterator[pd.Series]) -> Iterator[pd.Series]:
    model = load_model("/models/latest")  # loaded ONCE per partition, not per batch
    for batch in batch_iter:
        yield pd.Series(model.predict(batch.values))

df = df.withColumn("prediction", predict(col("features")))
```

### 4.4 Iterator of Multiple Series

```python
from typing import Iterator, Tuple

@pandas_udf("double")
def predict_multi(batch_iter: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
    model = load_model()
    for feature1, feature2 in batch_iter:
        X = pd.DataFrame({"f1": feature1, "f2": feature2})
        yield pd.Series(model.predict(X))

df = df.withColumn("pred", predict_multi(col("feature1"), col("feature2")))
```

### 4.5 Aggregate Pandas UDF

Aggregate across all rows in a group (like a custom aggregate function):

```python
@pandas_udf("double")
def custom_median(values: pd.Series) -> float:
    return values.median()

df.groupBy("dept").agg(custom_median(col("salary")).alias("median_salary"))
```

---

## 5. applyInPandas -- Grouped Map

### 5.1 What It Does

`applyInPandas` applies a function to each **group** of a grouped DataFrame. The function receives a Pandas DataFrame (one group) and returns a Pandas DataFrame (can have different schema and different number of rows).

```python
def normalize(pdf: pd.DataFrame) -> pd.DataFrame:
    result = pdf.copy()
    result["normalized_value"] = (pdf["value"] - pdf["value"].mean()) / pdf["value"].std()
    return result

output_schema = "device_id string, timestamp timestamp, value double, normalized_value double"

df.groupBy("device_id").applyInPandas(normalize, schema=output_schema)
```

### 5.2 How It Works

```
Spark DataFrame grouped by "device_id"
├── Group: device_001 (5000 rows) → convert to Pandas DF → normalize() → convert back
├── Group: device_002 (3000 rows) → convert to Pandas DF → normalize() → convert back
├── Group: device_003 (8000 rows) → convert to Pandas DF → normalize() → convert back
└── ...

Each group runs as a Python task on an executor
Results combined back into a Spark DataFrame
```

### 5.3 Returning Different Schemas

Unlike Pandas UDFs, `applyInPandas` can return a DataFrame with a completely different schema:

```python
def compute_stats(pdf: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        "device_id": [pdf["device_id"].iloc[0]],
        "mean_value": [pdf["value"].mean()],
        "std_value": [pdf["value"].std()],
        "count": [len(pdf)],
        "min_value": [pdf["value"].min()],
        "max_value": [pdf["value"].max()]
    })

stats_schema = "device_id string, mean_value double, std_value double, count long, min_value double, max_value double"
stats = df.groupBy("device_id").applyInPandas(compute_stats, schema=stats_schema)
```

### 5.4 Returning Different Row Counts

```python
def expand_readings(pdf: pd.DataFrame) -> pd.DataFrame:
    results = []
    for _, row in pdf.iterrows():
        start = row["start_time"]
        end = row["end_time"]
        interval = pd.date_range(start, end, freq="1H")
        for ts in interval:
            results.append({"device_id": row["device_id"], "timestamp": ts, "value": row["value"]})
    return pd.DataFrame(results)

expanded_schema = "device_id string, timestamp timestamp, value double"
expanded = df.groupBy("device_id").applyInPandas(expand_readings, schema=expanded_schema)
```

### 5.5 Memory Considerations

The entire group must fit in the Python worker's memory as a Pandas DataFrame. If a group has 100M rows, the worker will OOM.

**Mitigation strategies**:
1. Ensure groups are not too large (check group sizes before `applyInPandas`)
2. Increase executor memory and `spark.executor.memoryOverhead`
3. Use `mapInPandas` instead if you don't need grouping

```python
# Check group sizes
df.groupBy("device_id").count().orderBy(col("count").desc()).show(20)
```

### 5.6 Co-grouped applyInPandas

Join two DataFrames and apply a function to each co-group:

```python
def merge_and_compute(key, left_pdf, right_pdf):
    merged = pd.merge(left_pdf, right_pdf, on="timestamp", how="outer")
    merged["diff"] = merged["value_x"] - merged["value_y"]
    return merged

result = df1.groupBy("device_id").cogroup(
    df2.groupBy("device_id")
).applyInPandas(merge_and_compute, schema=output_schema)
```

---

## 6. mapInPandas -- Streaming Map

### 6.1 What It Does

`mapInPandas` applies a function to the DataFrame in a streaming fashion, processing batches of rows (not groups). It receives an iterator of Pandas DataFrames and yields Pandas DataFrames.

```python
def add_length(batch_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for pdf in batch_iter:
        pdf["name_length"] = pdf["name"].str.len()
        yield pdf

result = df.mapInPandas(add_length, schema="name string, age int, name_length int")
```

### 6.2 When to Use mapInPandas vs applyInPandas

| Feature | mapInPandas | applyInPandas |
|---------|-------------|---------------|
| **Input** | Iterator of batches (no grouping) | One Pandas DF per group |
| **Grouping** | No | Yes (groupBy required) |
| **Memory** | Only one batch in memory at a time | Entire group in memory |
| **Use case** | Row-level transforms, batch processing | Per-group analysis, per-group models |
| **Schema change** | Yes | Yes |
| **Row count change** | Yes | Yes |

### 6.3 Streaming Model Inference

```python
def batch_predict(batch_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    model = load_model("/models/v2")
    for pdf in batch_iter:
        predictions = model.predict(pdf[["feature1", "feature2"]].values)
        pdf["prediction"] = predictions
        yield pdf[["id", "prediction"]]

result = df.mapInPandas(batch_predict, schema="id long, prediction double")
```

---

## 7. Performance Comparison

### 7.1 Benchmark: Simple Math Operation

Task: Multiply a column by 2 and add 1, on 10 million rows.

```python
# Method 1: Built-in function
df.withColumn("result", col("value") * 2 + 1)
# Time: 1.2s (baseline)

# Method 2: Pandas UDF
@pandas_udf("double")
def fast_math(s: pd.Series) -> pd.Series:
    return s * 2 + 1
df.withColumn("result", fast_math(col("value")))
# Time: 2.5s (~2x slower than built-in)

# Method 3: Regular Python UDF
@udf(DoubleType())
def slow_math(value):
    return value * 2 + 1
df.withColumn("result", slow_math(col("value")))
# Time: 45s (~37x slower than built-in)
```

### 7.2 Benchmark: Complex Operation (Regex + Lookup)

```python
# Built-in:
df.withColumn("domain", regexp_extract(col("email"), "@(.+)", 1))
# Time: 3s

# Pandas UDF:
@pandas_udf("string")
def extract_domain(emails: pd.Series) -> pd.Series:
    return emails.str.extract(r"@(.+)")[0]
# Time: 5s

# Regular UDF:
@udf(StringType())
def extract_domain_slow(email):
    import re
    m = re.search(r"@(.+)", email or "")
    return m.group(1) if m else None
# Time: 60s
```

### 7.3 Summary

| Method | Relative Speed | JVM/Python Boundary | Best For |
|--------|---------------|-------------------|----------|
| Built-in functions | 1x (fastest) | None (JVM only) | Everything they can express |
| Pandas UDF | 2-5x slower | Arrow (batch) | Custom vectorized logic |
| applyInPandas | 5-10x slower | Arrow (per group) | Per-group analysis |
| Regular Python UDF | 10-100x slower | Pickle (per row) | Last resort |
| RDD map | 10-100x slower | Pickle (per partition) | Legacy code |

---

## 8. Type Mapping

### 8.1 Spark ↔ Pandas ↔ Arrow Type Mapping

| Spark Type | Pandas Type | Arrow Type |
|-----------|-------------|-----------|
| `BooleanType` | `bool` / `object` | `bool_` |
| `ByteType` | `int8` | `int8` |
| `ShortType` | `int16` | `int16` |
| `IntegerType` | `int32` | `int32` |
| `LongType` | `int64` | `int64` |
| `FloatType` | `float32` | `float32` |
| `DoubleType` | `float64` | `float64` |
| `StringType` | `object` (str) | `string` |
| `BinaryType` | `object` (bytes) | `binary` |
| `DateType` | `datetime64[ns]` | `date32` |
| `TimestampType` | `datetime64[ns]` | `timestamp[ns]` |
| `ArrayType` | `object` (list) | `list` |
| `MapType` | `object` (dict) | `map` |
| `StructType` | `object` (dict) | `struct` |

### 8.2 Timestamp Gotchas

Spark timestamps are microsecond precision. Pandas timestamps are nanosecond precision. This can cause overflow for dates far in the future or past:

```python
# Spark Timestamp: 2024-01-15 14:30:00.123456  (microseconds)
# Pandas datetime64[ns]: 2024-01-15 14:30:00.123456000 (nanoseconds)

# Potential overflow for dates outside ~1677-2262 range in nanoseconds
# Use spark.conf to handle:
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", True)
spark.conf.set("spark.sql.session.timeZone", "UTC")
```

### 8.3 NULL Handling

Spark NULLs become Pandas `NaN` (for numeric) or `None` (for strings). Be careful:

```python
@pandas_udf("double")
def safe_divide(a: pd.Series, b: pd.Series) -> pd.Series:
    # b might contain NaN (from Spark NULLs)
    # Division by NaN returns NaN (correct)
    # Division by 0 returns inf (might want NaN instead)
    return a / b.replace(0, float("nan"))
```

---

## 9. Complex Use Cases

### 9.1 Per-Device Anomaly Detection

```python
def detect_anomalies(pdf: pd.DataFrame) -> pd.DataFrame:
    mean = pdf["reading"].mean()
    std = pdf["reading"].std()
    
    pdf["z_score"] = (pdf["reading"] - mean) / std if std > 0 else 0
    pdf["is_anomaly"] = pdf["z_score"].abs() > 3
    return pdf

schema = "device_id string, timestamp timestamp, reading double, z_score double, is_anomaly boolean"
anomalies = df.groupBy("device_id").applyInPandas(detect_anomalies, schema=schema)
```

### 9.2 Rolling Statistics Per Group

```python
def rolling_stats(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values("timestamp")
    pdf["rolling_mean_7d"] = pdf["value"].rolling(window=7, min_periods=1).mean()
    pdf["rolling_std_7d"] = pdf["value"].rolling(window=7, min_periods=1).std().fillna(0)
    pdf["rolling_max_7d"] = pdf["value"].rolling(window=7, min_periods=1).max()
    return pdf

schema = "device_id string, timestamp timestamp, value double, rolling_mean_7d double, rolling_std_7d double, rolling_max_7d double"
result = df.groupBy("device_id").applyInPandas(rolling_stats, schema=schema)
```

### 9.3 Custom ML Inference with PyTorch

```python
import torch

def batch_inference(batch_iter: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    model = torch.load("/models/anomaly_detector.pt")
    model.eval()
    
    for pdf in batch_iter:
        features = torch.tensor(pdf[["f1", "f2", "f3"]].values, dtype=torch.float32)
        with torch.no_grad():
            predictions = model(features).numpy()
        pdf["score"] = predictions[:, 0]
        yield pdf[["id", "score"]]

result = df.mapInPandas(batch_inference, schema="id long, score double")
```

### 9.4 Gap Filling / Interpolation

```python
def fill_gaps(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values("timestamp")
    full_range = pd.date_range(pdf["timestamp"].min(), pdf["timestamp"].max(), freq="1H")
    pdf = pdf.set_index("timestamp").reindex(full_range)
    pdf["value"] = pdf["value"].interpolate(method="linear")
    pdf["device_id"] = pdf["device_id"].ffill()
    pdf = pdf.reset_index().rename(columns={"index": "timestamp"})
    return pdf

schema = "timestamp timestamp, device_id string, value double"
filled = df.groupBy("device_id").applyInPandas(fill_gaps, schema=schema)
```

### 9.5 Custom Aggregation with Scipy

```python
from scipy import stats

def compute_statistics(pdf: pd.DataFrame) -> pd.DataFrame:
    values = pdf["value"].dropna()
    return pd.DataFrame({
        "device_id": [pdf["device_id"].iloc[0]],
        "mean": [values.mean()],
        "median": [values.median()],
        "skewness": [stats.skew(values)],
        "kurtosis": [stats.kurtosis(values)],
        "ks_stat": [stats.kstest(values, "norm", args=(values.mean(), values.std())).statistic]
    })

schema = "device_id string, mean double, median double, skewness double, kurtosis double, ks_stat double"
stats_df = df.groupBy("device_id").applyInPandas(compute_statistics, schema=schema)
```

---

## 10. Error Handling and Testing

### 10.1 Error Handling in UDFs

```python
@pandas_udf("double")
def safe_transform(values: pd.Series) -> pd.Series:
    try:
        result = values * 2 + 1
        result = result.where(result.notna(), None)
        return result
    except Exception as e:
        return pd.Series([None] * len(values))
```

For `applyInPandas`, handle errors per group:
```python
def process_with_fallback(pdf: pd.DataFrame) -> pd.DataFrame:
    try:
        pdf["result"] = complex_computation(pdf["value"])
    except Exception:
        pdf["result"] = None
    return pdf
```

### 10.2 Logging in UDFs

```python
import logging

def process_group(pdf: pd.DataFrame) -> pd.DataFrame:
    logger = logging.getLogger("my_pipeline")
    device_id = pdf["device_id"].iloc[0]
    
    if len(pdf) == 0:
        logger.warning(f"Empty group for device {device_id}")
        return pdf
    
    logger.info(f"Processing device {device_id} with {len(pdf)} rows")
    pdf["result"] = pdf["value"].apply(transform)
    return pdf
```

Note: Logs from UDFs appear in **executor logs**, not driver logs. Check the Spark UI → Executors → stderr.

### 10.3 Testing UDFs

```python
import pandas as pd
import pytest

def test_celsius_to_fahrenheit():
    input_series = pd.Series([0, 100, -40])
    expected = pd.Series([32.0, 212.0, -40.0])
    result = celsius_to_fahrenheit.func(input_series)
    pd.testing.assert_series_equal(result, expected)

def test_detect_anomalies():
    pdf = pd.DataFrame({
        "device_id": ["d1"] * 10,
        "timestamp": pd.date_range("2024-01-01", periods=10, freq="1H"),
        "reading": [10, 11, 10, 12, 10, 11, 100, 10, 11, 10]  # 100 is anomaly
    })
    result = detect_anomalies(pdf)
    assert result["is_anomaly"].sum() == 1
    assert result.loc[result["reading"] == 100, "is_anomaly"].iloc[0] == True

def test_normalize_empty_group():
    pdf = pd.DataFrame({"device_id": [], "timestamp": [], "value": []})
    result = normalize(pdf)
    assert len(result) == 0
```

---

## 11. Anti-Patterns and Pitfalls

### 11.1 Using UDF When Built-in Functions Suffice

```python
# BAD:
@udf(StringType())
def lower(s):
    return s.lower() if s else None

# GOOD:
from pyspark.sql.functions import lower
df.withColumn("name_lower", lower(col("name")))
```

Common built-in replacements:
| UDF Pattern | Built-in Replacement |
|------------|---------------------|
| `s.upper()` | `upper(col)` |
| `s.lower()` | `lower(col)` |
| `s.strip()` | `trim(col)` |
| `len(s)` | `length(col)` |
| `s.replace(a, b)` | `regexp_replace(col, a, b)` |
| `s.split(d)` | `split(col, d)` |
| `abs(x)` | `abs(col)` |
| `round(x, n)` | `round(col, n)` |
| `x if cond else y` | `when(cond, x).otherwise(y)` |
| `max(a, b)` | `greatest(col_a, col_b)` |
| `math.log(x)` | `log(col)` |

### 11.2 Wrong Return Schema in applyInPandas

```python
# BUG: Schema says "value double" but function returns integer values
def broken_func(pdf):
    pdf["value"] = pdf["value"].astype(int)  # returns int, schema says double
    return pdf

# This causes silent type coercion or errors depending on Spark version
# FIX: ensure return types match the schema
```

### 11.3 Non-Deterministic UDFs

```python
# BAD: random value different on retry
@udf(DoubleType())
def add_noise(value):
    import random
    return value + random.gauss(0, 1)  # different result each time

# If a task is retried (failure, speculation), you get different values
# GOOD: use Spark's built-in rand()
df.withColumn("noisy", col("value") + randn())
```

### 11.4 Forgetting to Handle NULLs

```python
# CRASHES on NULL values
@udf(IntegerType())
def string_length(s):
    return len(s)  # TypeError: object of type 'NoneType' has no len()

# FIX:
@udf(IntegerType())
def string_length(s):
    return len(s) if s is not None else None
```

### 11.5 Large Groups in applyInPandas

```python
# If one device has 100M rows, this OOMs the Python worker
df.groupBy("device_id").applyInPandas(process, schema=...)

# FIX: sub-partition large groups
df.withColumn("sub_group", (monotonically_increasing_id() % 100).cast("int"))
df.groupBy("device_id", "sub_group").applyInPandas(process, schema=...)
```

### 11.6 Returning Wrong Number of Rows in Pandas UDF

```python
# BUG: Pandas UDF (Series to Series) must return same number of rows
@pandas_udf("double")
def broken_filter(values: pd.Series) -> pd.Series:
    return values[values > 0]  # fewer rows than input!

# This will crash with: "Result vector from pandas_udf was not the required length"
# FIX: use built-in filter for row removal; UDFs transform, not filter
```

---

## 12. Quick-Reference Cheat Sheet

### Decision Tree

```
Can you express it with built-in Spark functions?
├── YES → Use built-in functions (fastest)
└── NO
    ├── Is it a row-level transformation (same rows in, same rows out)?
    │   ├── YES → Pandas UDF (Series to Series)
    │   └── Need expensive initialization? → Pandas UDF (Iterator variant)
    ├── Is it a per-group computation?
    │   ├── YES → applyInPandas (grouped map)
    │   └── Need co-grouping? → cogroup().applyInPandas
    ├── Is it a custom aggregation?
    │   └── YES → Aggregate Pandas UDF
    └── Is it a streaming row transform (no grouping)?
        └── YES → mapInPandas
```

### UDF Type Quick Reference

| Type | Decorator | Input | Output | Rows Change? |
|------|----------|-------|--------|-------------|
| Regular UDF | `@udf` | Single value | Single value | No |
| Pandas UDF (Series) | `@pandas_udf` | `pd.Series` | `pd.Series` (same length) | No |
| Pandas UDF (Iterator) | `@pandas_udf` | `Iterator[pd.Series]` | `Iterator[pd.Series]` | No |
| Pandas UDF (Aggregate) | `@pandas_udf` | `pd.Series` | Scalar | Yes (reduces) |
| applyInPandas | `.applyInPandas()` | `pd.DataFrame` (group) | `pd.DataFrame` (any) | Yes |
| mapInPandas | `.mapInPandas()` | `Iterator[pd.DataFrame]` | `Iterator[pd.DataFrame]` | Yes |

### Performance Tips

```
1. Always try built-in functions first
2. Use Pandas UDF over regular UDF (10-50x faster)
3. Use Iterator variant for expensive init (model loading)
4. Check group sizes before applyInPandas (avoid OOM)
5. Enable Arrow: spark.sql.execution.arrow.pyspark.enabled = true
6. Increase memoryOverhead for Python-heavy workloads
7. Test UDFs with Pandas directly (faster iteration)
8. Handle NULLs explicitly in every UDF
```

### Common Configs

| Config | Default | Purpose |
|--------|---------|---------|
| `spark.sql.execution.arrow.pyspark.enabled` | true (3.x) | Enable Arrow for Pandas UDFs |
| `spark.sql.execution.arrow.maxRecordsPerBatch` | 10000 | Rows per Arrow batch |
| `spark.sql.execution.arrow.pyspark.fallback.enabled` | true | Fall back to pickle if Arrow fails |
| `spark.executor.memoryOverhead` | 10% | Extra memory for Python workers |
| `spark.python.worker.memory` | 512m | Python worker memory limit |
