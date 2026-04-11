# Schema Variability and Data Quality -- A Complete Deep Dive

**From Multi-Vendor Chaos to Governed, Trustworthy Data**

---

## Table of Contents

1. [Why Schema Variability Happens](#1-why-schema-variability-happens)
2. [Schema Inference in Spark](#2-schema-inference-in-spark)
3. [Schema Enforcement](#3-schema-enforcement)
4. [Schema Evolution Patterns](#4-schema-evolution-patterns)
5. [Delta Lake Schema Evolution](#5-delta-lake-schema-evolution)
6. [Handling Missing and Extra Columns](#6-handling-missing-and-extra-columns)
7. [NULL Handling](#7-null-handling)
8. [Data Quality Dimensions](#8-data-quality-dimensions)
9. [Building a Data Quality Pipeline](#9-building-a-data-quality-pipeline)
10. [Data Validation Frameworks](#10-data-validation-frameworks)
11. [Data Contracts](#11-data-contracts)
12. [Data Observability](#12-data-observability)
13. [Real-World Patterns](#13-real-world-patterns)
14. [Quick-Reference Cheat Sheet](#14-quick-reference-cheat-sheet)

---

## 1. Why Schema Variability Happens

### 1.1 Multi-Vendor Data

In IoT and enterprise systems, data comes from multiple vendors, each with their own format:

```
Vendor A (meters):  {"device_id": "A001", "kwh": 42.5, "timestamp": "2024-01-15T10:00:00Z"}
Vendor B (meters):  {"meter_id": "B001", "energy_kwh": 42.5, "reading_time": 1705312800}
Vendor C (meters):  {"id": "C001", "reading": {"kwh": 42.5}, "ts": "15-Jan-2024 10:00"}
```

Same concept (meter reading), three different schemas: different field names, types, nesting, date formats.

### 1.2 API Evolution

REST APIs change over time:
```
v1: {"user": "alice", "score": 85}
v2: {"user": "alice", "score": 85, "grade": "A"}           ← new field
v3: {"user_name": "alice", "score": 85.0, "grade": "A"}    ← renamed field, type changed
```

### 1.3 User-Generated Data

Form submissions, CSV uploads, and manual data entry are inherently messy:
- Missing required fields
- Extra unexpected fields
- Wrong types (string where number expected)
- Inconsistent formatting ("Jan 15, 2024" vs "2024-01-15" vs "15/01/2024")

### 1.4 Acquisitions and Migrations

When companies merge or migrate systems, you inherit legacy schemas that don't match your standard.

---

## 2. Schema Inference in Spark

### 2.1 How Spark Infers Schema

When you read data without specifying a schema, Spark **infers** it:

```python
# Schema inference: Spark reads a sample of the data to determine types
df = spark.read.json("/data/events/")      # infers from data
df = spark.read.csv("/data/events/", header=True, inferSchema=True)
df = spark.read.parquet("/data/events/")   # Parquet has embedded schema (no inference needed)
```

For JSON/CSV, Spark reads a sample (or the entire file for small data) and guesses types:
- `"42"` → might be String or Integer (depends on `inferSchema` setting)
- `"2024-01-15"` → might be String or Date
- `"true"` → might be String or Boolean

### 2.2 Inference Risks

**Risk 1: Type instability**
```python
# Batch 1: all values are integers → Spark infers IntegerType
{"value": 1}, {"value": 2}, {"value": 3}

# Batch 2: one value is a float → Spark infers DoubleType
{"value": 1}, {"value": 2.5}, {"value": 3}

# Merging these produces a schema mismatch error
```

**Risk 2: Sampling misses edge cases**
```python
# Sample of 100 rows: all "status" values are "active" or "inactive" → StringType
# Row 101: "status" is null → now have nullable issue
# Row 1000: "status" is 1 (integer) → type conflict
```

**Risk 3: Performance**
Schema inference requires an extra pass over the data. For large files, this doubles the read time.

### 2.3 Always Specify Schema

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

schema = StructType([
    StructField("device_id", StringType(), nullable=False),
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("value", DoubleType(), nullable=True),
    StructField("unit", StringType(), nullable=True)
])

df = spark.read.schema(schema).json("/data/events/")
```

Benefits:
- Deterministic: schema never changes based on data content
- Faster: no inference pass needed
- Safer: type mismatches are caught at read time

---

## 3. Schema Enforcement

### 3.1 Read Modes

Spark supports three modes for handling malformed records:

```python
# PERMISSIVE (default): put malformed records in a special column
df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .schema(schema.add("_corrupt_record", StringType())) \
    .json("/data/")
# Malformed rows: all fields NULL except _corrupt_record (contains the raw record)

# DROPMALFORMED: silently drop records that don't match schema
df = spark.read.option("mode", "DROPMALFORMED").schema(schema).json("/data/")
# Bad records are simply gone -- you won't know how many were dropped

# FAILFAST: throw exception on first malformed record
df = spark.read.option("mode", "FAILFAST").schema(schema).json("/data/")
# Pipeline crashes immediately on bad data -- good for strict environments
```

### 3.2 Delta Lake Schema Enforcement

Delta Lake enforces schema on write by default:

```python
# This will FAIL if df has columns not in the target schema
df.write.format("delta").mode("append").save("/data/table/")
# AnalysisException: A schema mismatch detected when writing to the Delta table

# To allow new columns:
df.write.format("delta").mode("append").option("mergeSchema", True).save("/data/table/")
```

---

## 4. Schema Evolution Patterns

### 4.1 Adding Columns (Backward Compatible)

The most common change. New data has additional columns; old data lacks them.

```python
# Old schema: id, name, age
# New schema: id, name, age, email

# Spark with mergeSchema:
df = spark.read.option("mergeSchema", True).parquet("/data/")
# Old partitions: email is NULL
# New partitions: email has values
```

### 4.2 Removing Columns

Dropping a column from the source. Downstream pipelines that reference the column will break.

Strategy: Don't remove columns from the physical schema. Instead, stop populating them (set to NULL). Mark as deprecated in documentation.

### 4.3 Renaming Columns

```python
# Old: {"user_name": "Alice"}
# New: {"username": "Alice"}

# Strategy: Map old names to new names in the ingestion layer
column_mapping = {"user_name": "username", "email_addr": "email"}

for old_name, new_name in column_mapping.items():
    if old_name in df.columns:
        df = df.withColumnRenamed(old_name, new_name)
```

### 4.4 Type Changes

Changing a column's type (e.g., IntegerType to DoubleType):

```python
# Safe type changes (widening): int → long, float → double, int → double
df = df.withColumn("value", col("value").cast("double"))

# Unsafe type changes (narrowing or incompatible): string → int, double → int
# Requires validation:
df = df.withColumn("value_parsed", col("value_str").cast("int"))
df = df.withColumn("parse_failed", col("value_parsed").isNull() & col("value_str").isNotNull())
```

---

## 5. Delta Lake Schema Evolution

### 5.1 mergeSchema

Automatically adds new columns:

```python
# Existing table: columns A, B
# New data: columns A, B, C
df_new.write.format("delta") \
    .option("mergeSchema", True) \
    .mode("append") \
    .save("/data/table/")
# Table now has columns A, B, C (C is NULL for old rows)
```

### 5.2 overwriteSchema

Replace the entire schema:

```python
df_new.write.format("delta") \
    .option("overwriteSchema", True) \
    .mode("overwrite") \
    .save("/data/table/")
# Table schema is completely replaced with df_new's schema
```

### 5.3 Schema Evolution with MERGE

```sql
MERGE INTO target t
USING source s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

With schema evolution enabled, new columns in the source are automatically added to the target.

### 5.4 Auto Loader Schema Evolution

```python
spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/") \
    .load("/data/landing/")
```

Modes:
- `addNewColumns`: Add new columns automatically
- `failOnNewColumns`: Fail if new columns detected
- `rescue`: Put unrecognized columns in `_rescued_data`
- `none`: Ignore schema changes

---

## 6. Handling Missing and Extra Columns

### 6.1 Missing Columns

When data from different sources has different columns:

```python
# Ensure all expected columns exist
expected_columns = ["device_id", "timestamp", "value", "unit", "location"]

for col_name in expected_columns:
    if col_name not in df.columns:
        df = df.withColumn(col_name, lit(None).cast(expected_types[col_name]))
```

### 6.2 Extra Columns

```python
# Keep only expected columns
df = df.select([col(c) for c in expected_columns if c in df.columns])

# Or use rescue data pattern (save unexpected columns for analysis)
extra_cols = [c for c in df.columns if c not in expected_columns]
if extra_cols:
    df = df.withColumn("_extra_data", to_json(struct(*[col(c) for c in extra_cols])))
    df = df.drop(*extra_cols)
```

### 6.3 Column Normalization

Map vendor-specific column names to a standard schema:

```python
vendor_schemas = {
    "vendor_a": {"device_id": "device_id", "kwh": "energy_kwh", "timestamp": "reading_time"},
    "vendor_b": {"meter_id": "device_id", "energy_kwh": "energy_kwh", "reading_time": "reading_time"},
    "vendor_c": {"id": "device_id", "reading.kwh": "energy_kwh", "ts": "reading_time"}
}

def normalize(df, vendor):
    mapping = vendor_schemas[vendor]
    for source_col, target_col in mapping.items():
        if "." in source_col:
            df = df.withColumn(target_col, col(source_col))
        else:
            df = df.withColumnRenamed(source_col, target_col)
    return df.select("device_id", "energy_kwh", "reading_time")
```

---

## 7. NULL Handling

### 7.1 NULL Semantics in Spark

NULL means "unknown" or "missing." Any comparison with NULL yields NULL (unknown):

```sql
NULL = NULL    → NULL (not TRUE)
NULL > 5       → NULL
NULL AND TRUE  → NULL
NULL OR TRUE   → TRUE
```

### 7.2 Common NULL Patterns

```python
# Fill NULLs with defaults
df = df.fillna({"value": 0.0, "status": "unknown", "count": 0})

# Fill with column-specific logic
df = df.withColumn("value", coalesce(col("value"), col("default_value"), lit(0.0)))

# Drop rows with NULLs in critical columns
df = df.dropna(subset=["device_id", "timestamp"])

# Drop rows where ALL columns are NULL
df = df.dropna(how="all")

# Replace specific values with NULL
df = df.withColumn("value", when(col("value") == -999, None).otherwise(col("value")))
```

### 7.3 NULL-Safe Comparisons

```python
# Standard comparison: NULL != NULL → NULL (filtered out)
df.filter(col("a") == col("b"))  # skips rows where a or b is NULL

# NULL-safe: NULL <=> NULL → TRUE
df.filter(col("a").eqNullSafe(col("b")))
```

### 7.4 NULL Aggregation Behavior

```python
# SUM ignores NULLs: SUM([1, NULL, 3]) = 4
# AVG ignores NULLs: AVG([1, NULL, 3]) = 2 (not 4/3)
# COUNT(col) ignores NULLs: COUNT([1, NULL, 3]) = 2
# COUNT(*) counts all rows: COUNT(*) = 3

# MIN/MAX ignore NULLs
# COLLECT_LIST includes NULLs, COLLECT_SET does not
```

---

## 8. Data Quality Dimensions

### 8.1 The Six Dimensions

| Dimension | Question | Example Metric |
|-----------|---------|---------------|
| **Completeness** | Is all expected data present? | % of non-null values per column |
| **Accuracy** | Is the data correct? | % of values within expected range |
| **Consistency** | Does data agree across sources? | Sum(parts) == total |
| **Timeliness** | Is the data fresh enough? | Time since last update |
| **Uniqueness** | Are there duplicates? | Count == distinct count |
| **Validity** | Do values conform to rules? | % of values matching regex/enum |

### 8.2 Implementing Checks Per Dimension

```python
def check_completeness(df, column, threshold=0.99):
    total = df.count()
    non_null = df.filter(col(column).isNotNull()).count()
    rate = non_null / total if total > 0 else 0
    return rate >= threshold, rate

def check_uniqueness(df, key_columns):
    total = df.count()
    distinct = df.select(key_columns).distinct().count()
    return total == distinct, total - distinct

def check_freshness(df, time_column, max_delay_hours=2):
    latest = df.agg(max(col(time_column))).collect()[0][0]
    delay = (datetime.now() - latest).total_seconds() / 3600
    return delay <= max_delay_hours, delay

def check_validity(df, column, valid_values):
    total = df.count()
    valid = df.filter(col(column).isin(valid_values)).count()
    rate = valid / total if total > 0 else 0
    return rate >= 0.99, rate

def check_range(df, column, min_val, max_val):
    out_of_range = df.filter((col(column) < min_val) | (col(column) > max_val)).count()
    return out_of_range == 0, out_of_range
```

---

## 9. Building a Data Quality Pipeline

### 9.1 Architecture

```
Raw Data → Profiling → Validation → Scoring → Routing → Alerting
                                      ↓
                              ┌───────┴───────┐
                              │               │
                          Good Data       Bad Data
                          (Silver)         (DLQ)
```

### 9.2 Step 1: Profiling

Compute statistics about the data before validation:

```python
def profile(df):
    stats = {}
    stats["row_count"] = df.count()
    stats["column_count"] = len(df.columns)
    
    for col_name in df.columns:
        col_stats = {}
        col_stats["null_count"] = df.filter(col(col_name).isNull()).count()
        col_stats["null_rate"] = col_stats["null_count"] / stats["row_count"]
        col_stats["distinct_count"] = df.select(col_name).distinct().count()
        
        if df.schema[col_name].dataType in (DoubleType(), IntegerType(), LongType()):
            summary = df.select(col_name).summary("min", "max", "mean", "stddev").collect()
            col_stats["min"] = summary[0][1]
            col_stats["max"] = summary[1][1]
            col_stats["mean"] = summary[2][1]
        
        stats[col_name] = col_stats
    
    return stats
```

### 9.3 Step 2: Rule-Based Validation

```python
class DataQualityRuleSet:
    def __init__(self):
        self.rules = []
    
    def add_not_null(self, column):
        self.rules.append(("not_null", column, col(column).isNotNull()))
    
    def add_range(self, column, min_val, max_val):
        self.rules.append(("range", column, (col(column) >= min_val) & (col(column) <= max_val)))
    
    def add_regex(self, column, pattern):
        self.rules.append(("regex", column, col(column).rlike(pattern)))
    
    def add_enum(self, column, values):
        self.rules.append(("enum", column, col(column).isin(values)))
    
    def validate(self, df):
        for rule_name, column, condition in self.rules:
            df = df.withColumn(f"_valid_{rule_name}_{column}", condition)
        
        valid_conditions = [col(c) for c in df.columns if c.startswith("_valid_")]
        df = df.withColumn("_is_valid", reduce(lambda a, b: a & b, valid_conditions))
        
        good = df.filter(col("_is_valid")).drop(*[c for c in df.columns if c.startswith("_valid_") or c == "_is_valid"])
        bad = df.filter(~col("_is_valid"))
        
        return good, bad

# Usage
rules = DataQualityRuleSet()
rules.add_not_null("device_id")
rules.add_not_null("timestamp")
rules.add_range("value", 0, 100000)
rules.add_enum("status", ["active", "inactive"])

good_data, bad_data = rules.validate(df)
```

### 9.4 Step 3: Scoring

Assign a quality score to each record or batch:

```python
def quality_score(df, rules):
    total_checks = len(rules)
    score_col = lit(0)
    
    for rule_name, column, condition in rules:
        score_col = score_col + when(condition, 1).otherwise(0)
    
    df = df.withColumn("quality_score", score_col / total_checks)
    return df
```

### 9.5 Step 4: Routing

```python
# Route based on quality score
high_quality = df.filter(col("quality_score") >= 0.95)   # → Silver
medium_quality = df.filter((col("quality_score") >= 0.7) & (col("quality_score") < 0.95))  # → Quarantine
low_quality = df.filter(col("quality_score") < 0.7)       # → DLQ

high_quality.write.format("delta").mode("append").save("/data/silver/")
medium_quality.write.format("delta").mode("append").save("/data/quarantine/")
low_quality.write.format("delta").mode("append").save("/data/dlq/")
```

---

## 10. Data Validation Frameworks

### 10.1 Great Expectations (Conceptual Pattern)

```python
# Define expectations (rules)
expectations = [
    {"type": "expect_column_to_exist", "column": "device_id"},
    {"type": "expect_column_values_to_not_be_null", "column": "device_id"},
    {"type": "expect_column_values_to_be_between", "column": "value", "min": 0, "max": 100000},
    {"type": "expect_column_values_to_be_unique", "column": "transaction_id"},
    {"type": "expect_table_row_count_to_be_between", "min": 1000, "max": 10000000},
]

# Validate
results = validate(df, expectations)
# Returns: {passed: 4, failed: 1, details: [...]}
```

### 10.2 Deequ (Amazon)

Open-source library built on Spark for data quality:
```python
# Conceptual (Deequ is Scala-based, with PySpark wrapper)
from pydeequ.checks import Check
from pydeequ.verification import VerificationSuite

check = Check(spark, CheckLevel.Error, "data_quality") \
    .hasSize(lambda x: x >= 1000) \
    .isComplete("device_id") \
    .isUnique("transaction_id") \
    .isNonNegative("value") \
    .isContainedIn("status", ["active", "inactive"])

result = VerificationSuite(spark) \
    .onData(df) \
    .addCheck(check) \
    .run()
```

### 10.3 Delta Live Tables Expectations

```sql
CREATE LIVE TABLE silver_events (
    CONSTRAINT valid_device EXPECT (device_id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_value EXPECT (value BETWEEN 0 AND 100000) ON VIOLATION DROP ROW,
    CONSTRAINT valid_timestamp EXPECT (timestamp > '2020-01-01') ON VIOLATION FAIL UPDATE
)
AS SELECT * FROM LIVE.bronze_events;
```

---

## 11. Data Contracts

### 11.1 What Is a Data Contract

A data contract is a formal agreement between a data **producer** and **consumer** about:
- Schema (column names, types, nullable)
- Quality guarantees (null rate, freshness, uniqueness)
- SLA (update frequency, latency)
- Ownership (who to contact when things break)

### 11.2 Contract Definition

```yaml
# data_contract.yaml
contract:
  name: meter_readings
  version: 2.0
  owner: smart-meter-team
  
schema:
  - name: device_id
    type: string
    nullable: false
    description: "Unique meter identifier"
  - name: timestamp
    type: timestamp
    nullable: false
  - name: value
    type: double
    nullable: true
    constraints:
      min: 0
      max: 100000

quality:
  freshness: "1 hour"
  completeness:
    device_id: 1.0
    timestamp: 1.0
    value: 0.99
  uniqueness:
    keys: ["device_id", "timestamp"]

sla:
  update_frequency: "every 15 minutes"
  latency: "< 30 minutes from source"
```

### 11.3 Contract Enforcement

```python
def enforce_contract(df, contract):
    errors = []
    
    # Schema checks
    for field in contract["schema"]:
        if field["name"] not in df.columns:
            errors.append(f"Missing column: {field['name']}")
        elif str(df.schema[field["name"]].dataType) != field["type"]:
            errors.append(f"Type mismatch: {field['name']} expected {field['type']}")
    
    # Quality checks
    quality = contract["quality"]
    for col_name, min_rate in quality.get("completeness", {}).items():
        actual = df.filter(col(col_name).isNotNull()).count() / df.count()
        if actual < min_rate:
            errors.append(f"Completeness: {col_name} = {actual:.2%} < {min_rate:.2%}")
    
    if errors:
        raise ContractViolationError(errors)
    
    return df
```

---

## 12. Data Observability

### 12.1 The Five Pillars

| Pillar | What to Monitor | How |
|--------|----------------|-----|
| **Freshness** | When was data last updated? | Track max(timestamp) per table |
| **Volume** | How many rows/bytes? | Track row count trends |
| **Schema** | Did the schema change? | Compare schema to previous run |
| **Distribution** | Did value distributions shift? | Track percentiles, histograms |
| **Lineage** | Where did data come from? | Track source → transform → target |

### 12.2 Anomaly Detection on Metrics

```python
import numpy as np

def detect_anomaly(current_value, historical_values, std_threshold=3):
    mean = np.mean(historical_values)
    std = np.std(historical_values)
    z_score = abs(current_value - mean) / std if std > 0 else 0
    return z_score > std_threshold, z_score

# Check if today's row count is anomalous
today_count = df.count()
historical_counts = get_daily_counts(table="events", last_n_days=30)
is_anomaly, z = detect_anomaly(today_count, historical_counts)

if is_anomaly:
    alert(f"Row count anomaly: {today_count} (z-score: {z:.2f})")
```

### 12.3 Lineage Tracking

```python
# Add metadata columns for lineage
df = df.withColumn("_source_system", lit("vendor_a")) \
       .withColumn("_pipeline_name", lit("meter_ingestion")) \
       .withColumn("_pipeline_version", lit("2.3.0")) \
       .withColumn("_processed_at", current_timestamp()) \
       .withColumn("_source_file", input_file_name())
```

---

## 13. Real-World Patterns

### 13.1 Handling 50+ Device Types with Different JSON Structures

```python
def unified_ingestion(df_raw):
    # Step 1: Parse JSON with the most permissive schema
    df = df_raw.select(
        get_json_object(col("raw_json"), "$.device_id").alias("device_id"),
        get_json_object(col("raw_json"), "$.meter_id").alias("meter_id"),
        get_json_object(col("raw_json"), "$.id").alias("alt_id"),
        get_json_object(col("raw_json"), "$.value").alias("value"),
        get_json_object(col("raw_json"), "$.reading").alias("reading"),
        get_json_object(col("raw_json"), "$.kwh").alias("kwh"),
        col("raw_json").alias("_raw")
    )
    
    # Step 2: Coalesce across naming variations
    df = df.withColumn("device_id", coalesce(col("device_id"), col("meter_id"), col("alt_id")))
    df = df.withColumn("energy_kwh", coalesce(col("value"), col("reading"), col("kwh")).cast("double"))
    
    # Step 3: Validate
    df = df.filter(col("device_id").isNotNull() & col("energy_kwh").isNotNull())
    
    return df.select("device_id", "energy_kwh", "_raw")
```

### 13.2 Schema-Agnostic Pipeline

Store raw data without enforcing a schema, validate lazily:

```python
# Bronze: store raw JSON as-is
raw.write.format("delta").mode("append").save("/bronze/events/")

# Silver: apply schema per known device type
device_schemas = load_device_schemas()  # from config

for device_type, schema in device_schemas.items():
    df = spark.read.format("delta").load("/bronze/events/") \
        .filter(col("device_type") == device_type)
    
    validated = df.select(from_json(col("raw_payload"), schema).alias("parsed")).select("parsed.*")
    validated.write.format("delta").mode("overwrite") \
        .option("replaceWhere", f"device_type = '{device_type}'") \
        .save("/silver/events/")
```

### 13.3 Rescue Data Column Pattern

```python
# Auto Loader: unrecognized fields go to _rescued_data
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaEvolutionMode", "rescue") \
    .option("cloudFiles.schemaLocation", "/checkpoints/schema/") \
    .load("/landing/")

# _rescued_data contains JSON of all fields that don't match the inferred schema
# Periodically analyze _rescued_data to discover new fields and update schema
```

---

## 14. Quick-Reference Cheat Sheet

### Schema Strategy Decision Tree

```
Is the schema known and fixed?
├── YES → Define explicit StructType, enforce on read
└── NO
    ├── Is it a Delta Lake table?
    │   ├── YES → Use mergeSchema for additive changes
    │   └── overwriteSchema for breaking changes
    ├── Are there multiple vendors?
    │   └── YES → Coalesce column names, validate per vendor
    └── Is it streaming?
        └── YES → Auto Loader with schemaEvolutionMode=rescue
```

### Read Mode Selection

| Mode | Behavior | Use When |
|------|----------|---------|
| PERMISSIVE | Nullify bad fields, save raw in _corrupt_record | Default, flexible |
| DROPMALFORMED | Silently drop bad rows | Tolerant pipelines |
| FAILFAST | Crash on first bad row | Strict validation |

### Data Quality Checklist

```
□ Define explicit schema (never rely on inference in production)
□ Check completeness (null rates) for required columns
□ Check uniqueness of primary keys
□ Check value ranges and valid enums
□ Check row count vs historical baseline
□ Check data freshness (latest timestamp)
□ Route bad records to DLQ (never silently drop)
□ Add lineage metadata columns
□ Monitor schema changes
□ Alert on anomalies (volume, distribution shifts)
```

### NULL Handling Strategies

| Scenario | Strategy |
|----------|---------|
| Required field is NULL | Reject to DLQ |
| Optional field is NULL | Keep (fillna if needed for computation) |
| All fields NULL | Drop row |
| NULL in join key | Handle separately (anti-join or coalesce) |
| NULL in aggregation | Aware that SUM/AVG/COUNT ignore NULLs |
| NULL comparison | Use `<=>` (null-safe) or `IS NOT DISTINCT FROM` |
