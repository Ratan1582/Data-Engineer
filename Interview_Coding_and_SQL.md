# Data Engineer Interview -- Coding & SQL Practice

> Hands-on practice problems with solutions for PySpark, SQL, and data transformations.
> Each problem includes: problem statement, sample data, solution, and explanation.

---

## Table of Contents

### Part A -- PySpark Coding Problems
1. [Deduplicate records keeping the latest](#a1-deduplicate-records-keeping-the-latest)
2. [Compute running total with window function](#a2-compute-running-total-with-window-function)
3. [Pivot data: rows to columns](#a3-pivot-data-rows-to-columns)
4. [Unpivot data: columns to rows](#a4-unpivot-data-columns-to-rows)
5. [Find top N per group](#a5-find-top-n-per-group)
6. [Self-join: compare consecutive rows](#a6-self-join-compare-consecutive-rows)
7. [Explode nested arrays](#a7-explode-nested-arrays)
8. [Left anti join: find missing records](#a8-left-anti-join-find-missing-records)
9. [Fill gaps in time-series data](#a9-fill-gaps-in-time-series-data)
10. [Compute percentage of total per group](#a10-compute-percentage-of-total-per-group)
11. [Broadcast join with lookup table](#a11-broadcast-join-with-lookup-table)
12. [Custom UDF: categorize values](#a12-custom-udf-categorize-values)
13. [Handle schema mismatch with unionByName](#a13-handle-schema-mismatch-with-unionbyname)
14. [Incremental read: process only new files](#a14-incremental-read-process-only-new-files)
15. [Multi-column aggregation with pivot](#a15-multi-column-aggregation-with-pivot)
16. [Detect duplicates and flag them](#a16-detect-duplicates-and-flag-them)

### Part B -- SQL Interview Questions
17. [Window: Row Number, Rank, Dense Rank](#b1-window-row-number-rank-dense-rank)
18. [Window: Running total and moving average](#b2-window-running-total-and-moving-average)
19. [LAG and LEAD: compare with previous/next row](#b3-lag-and-lead-compare-with-previousnext-row)
20. [Find consecutive days of activity](#b4-find-consecutive-days-of-activity)
21. [Second highest salary](#b5-second-highest-salary)
22. [Employees earning more than their manager](#b6-employees-earning-more-than-their-manager)
23. [Find duplicate records](#b7-find-duplicate-records)
24. [Top N products per category](#b8-top-n-products-per-category)
25. [Year-over-year growth](#b9-year-over-year-growth)
26. [Cumulative sum and percentage](#b10-cumulative-sum-and-percentage)
27. [Gaps in sequential data](#b11-gaps-in-sequential-data)
28. [Pivot with aggregation](#b12-pivot-with-aggregation)
29. [Self-join: find pairs](#b13-self-join-find-pairs)
30. [Date manipulation: active users per month](#b14-date-manipulation-active-users-per-month)
31. [HAVING: filter after aggregation](#b15-having-filter-after-aggregation)
32. [CTE vs Subquery](#b16-cte-vs-subquery)
33. [EXISTS vs IN](#b17-exists-vs-in)
34. [COALESCE and NULL handling](#b18-coalesce-and-null-handling)
35. [UNION vs UNION ALL](#b19-union-vs-union-all)
36. [Median calculation](#b20-median-calculation)

### Part C -- Data Transformation Challenges
37. [Sessionization](#c1-sessionization)
38. [SCD Type 2 implementation](#c2-scd-type-2-implementation)
39. [Gap and island problem](#c3-gap-and-island-problem)
40. [Flatten JSON to tabular](#c4-flatten-json-to-tabular)
41. [Cumulative distinct count](#c5-cumulative-distinct-count)
42. [Bucket/bin values into ranges](#c6-bucketbin-values-into-ranges)
43. [De-normalize: combine child rows into parent](#c7-de-normalize-combine-child-rows-into-parent)
44. [Time-weighted average](#c8-time-weighted-average)
45. [Data completeness report](#c9-data-completeness-report)
46. [Cross-join for generating combinations](#c10-cross-join-for-generating-combinations)

---

# PART A -- PYSPARK CODING PROBLEMS

---

## A1. Deduplicate Records Keeping the Latest

**Problem**: Given a DataFrame with duplicate records per `id`, keep only the row with the latest `updated_at` timestamp.

**Input**:
```
+----+-------+-------------------+
| id | name  | updated_at        |
+----+-------+-------------------+
| 1  | Alice | 2024-01-01 10:00  |
| 1  | Alice | 2024-01-02 12:00  |  <-- keep this
| 2  | Bob   | 2024-01-01 09:00  |  <-- keep this
| 2  | Bob   | 2024-01-01 08:00  |
+----+-------+-------------------+
```

**Solution**:
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window = Window.partitionBy("id").orderBy(col("updated_at").desc())
deduped = df.withColumn("rn", row_number().over(window)) \
            .filter(col("rn") == 1) \
            .drop("rn")
```

**Explanation**: `row_number()` assigns 1 to the latest record per `id`. Filter for `rn == 1` keeps only the most recent.

**Alternative**: `df.dropDuplicates(["id"])` -- but this keeps an arbitrary row, not necessarily the latest.

---

## A2. Compute Running Total with Window Function

**Problem**: Compute a running total of `amount` per `account_id`, ordered by `date`.

**Solution**:
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum

window = Window.partitionBy("account_id").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df.withColumn("running_total", spark_sum("amount").over(window))
```

**Output**:
```
+------------+----------+--------+---------------+
| account_id | date     | amount | running_total |
+------------+----------+--------+---------------+
| A          | 2024-01-01 | 100  | 100           |
| A          | 2024-01-02 | 200  | 300           |
| A          | 2024-01-03 | 150  | 450           |
+------------+----------+--------+---------------+
```

---

## A3. Pivot Data: Rows to Columns

**Problem**: Convert row-based data into columns.

**Input**:
```
+--------+----------+-------+
| device | metric   | value |
+--------+----------+-------+
| D1     | temp     | 25.0  |
| D1     | humidity | 60.0  |
| D2     | temp     | 30.0  |
| D2     | humidity | 55.0  |
+--------+----------+-------+
```

**Solution**:
```python
from pyspark.sql.functions import first

pivoted = df.groupBy("device") \
            .pivot("metric", ["temp", "humidity"]) \
            .agg(first("value"))
```

**Output**:
```
+--------+------+----------+
| device | temp | humidity |
+--------+------+----------+
| D1     | 25.0 | 60.0     |
| D2     | 30.0 | 55.0     |
+--------+------+----------+
```

**Tip**: Always pass explicit pivot values (`["temp", "humidity"]`) to avoid an extra scan.

---

## A4. Unpivot Data: Columns to Rows

**Problem**: Convert columnar data back to rows.

**Input**:
```
+--------+------+----------+
| device | temp | humidity |
+--------+------+----------+
| D1     | 25.0 | 60.0     |
| D2     | 30.0 | 55.0     |
+--------+------+----------+
```

**Solution**:
```python
from pyspark.sql.functions import expr

unpivoted = df.select(
    "device",
    expr("stack(2, 'temp', temp, 'humidity', humidity) as (metric, value)")
)
```

**Output**:
```
+--------+----------+-------+
| device | metric   | value |
+--------+----------+-------+
| D1     | temp     | 25.0  |
| D1     | humidity | 60.0  |
| D2     | temp     | 30.0  |
| D2     | humidity | 55.0  |
+--------+----------+-------+
```

---

## A5. Find Top N Per Group

**Problem**: Find the top 3 highest-paid employees per department.

**Solution**:
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window = Window.partitionBy("department").orderBy(col("salary").desc())
top3 = df.withColumn("rank", row_number().over(window)) \
         .filter(col("rank") <= 3) \
         .drop("rank")
```

**Note**: Use `row_number()` for exactly N rows. Use `dense_rank()` if ties should share ranks.

---

## A6. Self-Join: Compare Consecutive Rows

**Problem**: Compute the difference between each row and the previous row per device.

**Solution**:
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col

window = Window.partitionBy("device_id").orderBy("timestamp")
df.withColumn("prev_value", lag("value", 1).over(window)) \
  .withColumn("diff", col("value") - col("prev_value"))
```

---

## A7. Explode Nested Arrays

**Problem**: A column contains arrays. Flatten each element into its own row.

**Input**:
```
+----+------------+
| id | tags       |
+----+------------+
| 1  | [a, b, c]  |
| 2  | [d, e]     |
+----+------------+
```

**Solution**:
```python
from pyspark.sql.functions import explode

df.select("id", explode("tags").alias("tag"))
```

**Output**:
```
+----+-----+
| id | tag |
+----+-----+
| 1  | a   |
| 1  | b   |
| 1  | c   |
| 2  | d   |
| 2  | e   |
+----+-----+
```

**Variant**: Use `explode_outer` to keep rows where the array is null/empty (with null tag).

---

## A8. Left Anti Join: Find Missing Records

**Problem**: Find devices in the expected list that are NOT in the received data.

**Solution**:
```python
missing = expected_df.join(received_df, "device_id", "left_anti")
```

This returns all rows from `expected_df` that have no match in `received_df`. Equivalent to SQL `WHERE NOT EXISTS`.

---

## A9. Fill Gaps in Time-Series Data

**Problem**: Devices send readings every hour. Some hours are missing. Fill gaps with null values.

**Solution**:
```python
from pyspark.sql.functions import explode, sequence, to_timestamp, col

# Generate complete hourly range per device
all_hours = df.groupBy("device_id").agg(
    min("timestamp").alias("start"),
    max("timestamp").alias("end")
).select(
    "device_id",
    explode(sequence(col("start"), col("end"), expr("INTERVAL 1 HOUR"))).alias("timestamp")
)

# Left join to fill gaps
filled = all_hours.join(df, ["device_id", "timestamp"], "left")
```

---

## A10. Compute Percentage of Total Per Group

**Problem**: For each product, compute its percentage of total sales within its category.

**Solution**:
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum, col

window = Window.partitionBy("category")
df.withColumn("category_total", spark_sum("sales").over(window)) \
  .withColumn("pct_of_category", (col("sales") / col("category_total") * 100).cast("decimal(5,2)"))
```

---

## A11. Broadcast Join with Lookup Table

**Problem**: Enrich a large transactions table with a small lookup table.

**Solution**:
```python
from pyspark.sql.functions import broadcast

enriched = transactions.join(
    broadcast(category_lookup),
    "category_id",
    "left"
)
```

**Why broadcast**: The lookup table is small. Broadcasting sends it to every executor, avoiding a shuffle of the large transactions table.

---

## A12. Custom UDF: Categorize Values

**Problem**: Categorize temperature readings into buckets.

**Solution (built-in, preferred)**:
```python
from pyspark.sql.functions import when, col

df.withColumn("category",
    when(col("temp") < 0, "freezing")
    .when(col("temp") < 20, "cold")
    .when(col("temp") < 35, "warm")
    .otherwise("hot")
)
```

**Solution (UDF, if logic is too complex for `when`)**:
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def categorize(temp):
    if temp is None: return None
    if temp < 0: return "freezing"
    if temp < 20: return "cold"
    if temp < 35: return "warm"
    return "hot"

df.withColumn("category", categorize(col("temp")))
```

**Always prefer built-in functions** -- 10-100x faster than UDFs.

---

## A13. Handle Schema Mismatch with unionByName

**Problem**: Two DataFrames have overlapping but different columns. Combine them.

**Solution**:
```python
combined = df1.unionByName(df2, allowMissingColumns=True)
```

Missing columns are filled with `null`. This is essential when combining data from different vendors or schema versions.

---

## A14. Incremental Read: Process Only New Files

**Problem**: HDFS has new parquet files daily. Process only files added since the last run.

**Solution pattern**:
```python
# Read all files, get their paths
all_files_df = spark.read.parquet(base_path)
all_files = set(all_files_df.inputFiles())

# Load last processed file path from state store
last_path = read_state("last_processed_file")

# Filter to new files only
new_files = sorted([f for f in all_files if f > last_path])

if new_files:
    new_data = spark.read.parquet(*new_files)
    # process new_data...
    write_state("last_processed_file", max(new_files))
```

---

## A15. Multi-Column Aggregation with Pivot

**Problem**: Compute both sum and count per category per date, pivoted by category.

**Solution**:
```python
from pyspark.sql.functions import sum as spark_sum, count

result = df.groupBy("date") \
           .pivot("category", ["A", "B", "C"]) \
           .agg(
               spark_sum("amount").alias("total"),
               count("*").alias("count")
           )
```

This produces columns like `A_total`, `A_count`, `B_total`, `B_count`, etc.

---

## A16. Detect Duplicates and Flag Them

**Problem**: Mark duplicate records instead of removing them.

**Solution**:
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import count, col

window = Window.partitionBy("id", "date")
df.withColumn("dup_count", count("*").over(window)) \
  .withColumn("is_duplicate", col("dup_count") > 1)
```

---

# PART B -- SQL INTERVIEW QUESTIONS

All solutions use standard SQL (compatible with Spark SQL, PostgreSQL, etc.).

---

## B1. Window: Row Number, Rank, Dense Rank

**Problem**: Given a table of employees with salaries, assign ranks within each department.

**Input**: `employees(id, name, department, salary)`

```sql
SELECT id, name, department, salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num,
    RANK()       OVER (PARTITION BY department ORDER BY salary DESC) AS rnk,
    DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rnk
FROM employees;
```

**Difference**:
| Salary | ROW_NUMBER | RANK | DENSE_RANK |
|--------|-----------|------|------------|
| 100K   | 1         | 1    | 1          |
| 100K   | 2         | 1    | 1          |
| 90K    | 3         | 3    | 2          |
| 80K    | 4         | 4    | 3          |

- `ROW_NUMBER`: Always unique (arbitrary for ties).
- `RANK`: Same rank for ties, skips next rank.
- `DENSE_RANK`: Same rank for ties, does not skip.

---

## B2. Window: Running Total and Moving Average

```sql
SELECT date, amount,
    SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
    AVG(amount) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_3
FROM transactions;
```

---

## B3. LAG and LEAD: Compare with Previous/Next Row

**Problem**: Compute day-over-day change in value.

```sql
SELECT date, value,
    value - LAG(value, 1) OVER (ORDER BY date) AS daily_change,
    LEAD(value, 1) OVER (ORDER BY date) AS next_value
FROM metrics;
```

---

## B4. Find Consecutive Days of Activity

**Problem**: Find users who were active for 3+ consecutive days.

```sql
WITH numbered AS (
    SELECT user_id, activity_date,
        activity_date - INTERVAL ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date) DAY AS grp
    FROM user_activity
),
streaks AS (
    SELECT user_id, grp, COUNT(*) AS streak_length
    FROM numbered
    GROUP BY user_id, grp
)
SELECT DISTINCT user_id
FROM streaks
WHERE streak_length >= 3;
```

**Explanation**: Subtracting the row number from the date creates a constant `grp` for consecutive dates. Same `grp` = same streak.

---

## B5. Second Highest Salary

**Problem**: Find the second highest salary in the company.

```sql
-- Method 1: DENSE_RANK
SELECT salary
FROM (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) AS rnk
    FROM employees
) ranked
WHERE rnk = 2;

-- Method 2: LIMIT/OFFSET
SELECT DISTINCT salary
FROM employees
ORDER BY salary DESC
LIMIT 1 OFFSET 1;

-- Method 3: Subquery
SELECT MAX(salary)
FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees);
```

---

## B6. Employees Earning More Than Their Manager

```sql
SELECT e.name AS employee, e.salary, m.name AS manager, m.salary AS manager_salary
FROM employees e
JOIN employees m ON e.manager_id = m.id
WHERE e.salary > m.salary;
```

---

## B7. Find Duplicate Records

```sql
-- Find duplicate emails
SELECT email, COUNT(*) AS cnt
FROM users
GROUP BY email
HAVING COUNT(*) > 1;

-- Return full rows of duplicates
SELECT *
FROM users
WHERE email IN (
    SELECT email FROM users GROUP BY email HAVING COUNT(*) > 1
);
```

---

## B8. Top N Products Per Category

**Problem**: Find top 3 products by revenue in each category.

```sql
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rn
    FROM products
)
SELECT * FROM ranked WHERE rn <= 3;
```

---

## B9. Year-over-Year Growth

```sql
WITH yearly AS (
    SELECT YEAR(order_date) AS yr, SUM(amount) AS total
    FROM orders
    GROUP BY YEAR(order_date)
)
SELECT yr, total,
    LAG(total) OVER (ORDER BY yr) AS prev_year,
    ROUND((total - LAG(total) OVER (ORDER BY yr)) / LAG(total) OVER (ORDER BY yr) * 100, 2) AS yoy_growth_pct
FROM yearly;
```

---

## B10. Cumulative Sum and Percentage

```sql
SELECT category, amount,
    SUM(amount) OVER (ORDER BY amount DESC) AS cumulative_sum,
    ROUND(SUM(amount) OVER (ORDER BY amount DESC) / SUM(amount) OVER () * 100, 2) AS cumulative_pct
FROM sales;
```

---

## B11. Gaps in Sequential Data

**Problem**: Find missing IDs in a sequence.

```sql
WITH seq AS (
    SELECT id, LEAD(id) OVER (ORDER BY id) AS next_id
    FROM records
)
SELECT id + 1 AS gap_start, next_id - 1 AS gap_end
FROM seq
WHERE next_id - id > 1;
```

---

## B12. Pivot with Aggregation

**Problem**: Show total sales per month as columns.

```sql
SELECT product,
    SUM(CASE WHEN month = 'Jan' THEN amount ELSE 0 END) AS Jan,
    SUM(CASE WHEN month = 'Feb' THEN amount ELSE 0 END) AS Feb,
    SUM(CASE WHEN month = 'Mar' THEN amount ELSE 0 END) AS Mar
FROM sales
GROUP BY product;
```

In Spark SQL, you can also use `PIVOT`:
```sql
SELECT * FROM sales
PIVOT (SUM(amount) FOR month IN ('Jan', 'Feb', 'Mar'));
```

---

## B13. Self-Join: Find Pairs

**Problem**: Find all pairs of employees in the same department.

```sql
SELECT a.name AS emp1, b.name AS emp2, a.department
FROM employees a
JOIN employees b ON a.department = b.department AND a.id < b.id;
```

`a.id < b.id` avoids duplicates (Alice-Bob and Bob-Alice).

---

## B14. Date Manipulation: Active Users Per Month

```sql
SELECT DATE_TRUNC('month', login_date) AS month,
       COUNT(DISTINCT user_id) AS active_users
FROM logins
GROUP BY DATE_TRUNC('month', login_date)
ORDER BY month;
```

---

## B15. HAVING: Filter After Aggregation

**Problem**: Find departments with average salary > 80K.

```sql
SELECT department, AVG(salary) AS avg_salary
FROM employees
GROUP BY department
HAVING AVG(salary) > 80000;
```

**Key**: `WHERE` filters rows before aggregation. `HAVING` filters groups after aggregation.

---

## B16. CTE vs Subquery

**CTE (Common Table Expression)** -- readable, reusable within the query:
```sql
WITH high_earners AS (
    SELECT * FROM employees WHERE salary > 100000
)
SELECT department, COUNT(*) FROM high_earners GROUP BY department;
```

**Subquery** -- inline:
```sql
SELECT department, COUNT(*)
FROM (SELECT * FROM employees WHERE salary > 100000) sub
GROUP BY department;
```

**CTEs** are preferred for readability and when referenced multiple times. Performance is usually identical.

---

## B17. EXISTS vs IN

```sql
-- IN: checks if value is in a list
SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE country = 'US');

-- EXISTS: checks if a correlated subquery returns any row
SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.country = 'US');
```

**Performance**: `EXISTS` can be faster when the subquery result is large (short-circuits on first match). `IN` can be faster when the subquery result is small.

---

## B18. COALESCE and NULL Handling

```sql
-- Return first non-null value
SELECT COALESCE(phone, email, 'no_contact') AS contact FROM users;

-- Filter nulls
SELECT * FROM users WHERE email IS NOT NULL;

-- Replace nulls
SELECT id, IFNULL(amount, 0) AS amount FROM transactions;  -- MySQL/Spark
SELECT id, COALESCE(amount, 0) AS amount FROM transactions; -- Standard SQL
```

---

## B19. UNION vs UNION ALL

```sql
-- UNION: removes duplicates (expensive -- requires sort/hash)
SELECT name FROM table1 UNION SELECT name FROM table2;

-- UNION ALL: keeps all rows including duplicates (fast -- just concatenates)
SELECT name FROM table1 UNION ALL SELECT name FROM table2;
```

**Rule**: Use `UNION ALL` unless you specifically need deduplication. It's much faster.

---

## B20. Median Calculation

```sql
-- Method 1: PERCENTILE (Spark/Hive)
SELECT PERCENTILE_APPROX(salary, 0.5) AS median_salary FROM employees;

-- Method 2: Window function (standard SQL)
WITH ranked AS (
    SELECT salary,
        ROW_NUMBER() OVER (ORDER BY salary) AS rn,
        COUNT(*) OVER () AS total
    FROM employees
)
SELECT AVG(salary) AS median_salary
FROM ranked
WHERE rn IN (FLOOR((total + 1) / 2.0), CEIL((total + 1) / 2.0));
```

---

# PART C -- DATA TRANSFORMATION CHALLENGES

---

## C1. Sessionization

**Problem**: Group user events into sessions. A new session starts if the gap between events exceeds 30 minutes.

**Solution (PySpark)**:
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col, when, sum as spark_sum, unix_timestamp

window = Window.partitionBy("user_id").orderBy("event_time")

df = df.withColumn("prev_time", lag("event_time").over(window))
df = df.withColumn("gap_seconds",
    unix_timestamp("event_time") - unix_timestamp("prev_time"))
df = df.withColumn("new_session",
    when((col("gap_seconds") > 1800) | col("prev_time").isNull(), 1).otherwise(0))
df = df.withColumn("session_id",
    spark_sum("new_session").over(window))
```

**SQL**:
```sql
WITH gaps AS (
    SELECT *,
        UNIX_TIMESTAMP(event_time) - UNIX_TIMESTAMP(LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time)) AS gap_sec
    FROM events
),
flags AS (
    SELECT *,
        CASE WHEN gap_sec > 1800 OR gap_sec IS NULL THEN 1 ELSE 0 END AS new_session
    FROM gaps
)
SELECT *,
    SUM(new_session) OVER (PARTITION BY user_id ORDER BY event_time) AS session_id
FROM flags;
```

---

## C2. SCD Type 2 Implementation

**Problem**: Implement Slowly Changing Dimension Type 2 using Delta Lake MERGE.

**Solution**:
```sql
-- Incoming new/changed records in `staging` table
-- Target table has: id, name, city, effective_from, effective_to, is_current

MERGE INTO dim_customer AS target
USING (
    SELECT s.id, s.name, s.city
    FROM staging s
    JOIN dim_customer t ON s.id = t.id AND t.is_current = true
    WHERE s.name != t.name OR s.city != t.city
) AS changes
ON target.id = changes.id AND target.is_current = true
WHEN MATCHED THEN
    UPDATE SET effective_to = CURRENT_DATE(), is_current = false;

-- Insert new current records
INSERT INTO dim_customer
SELECT s.id, s.name, s.city, CURRENT_DATE() AS effective_from, NULL AS effective_to, true AS is_current
FROM staging s
LEFT JOIN dim_customer t ON s.id = t.id AND t.is_current = true
WHERE t.id IS NULL OR s.name != t.name OR s.city != t.city;
```

**PySpark equivalent**:
```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "/dim_customer")

target.alias("t").merge(
    staging.alias("s"),
    "t.id = s.id AND t.is_current = true"
).whenMatchedUpdate(
    condition="s.name != t.name OR s.city != t.city",
    set={"effective_to": "current_date()", "is_current": "false"}
).whenNotMatchedInsert(
    values={
        "id": "s.id", "name": "s.name", "city": "s.city",
        "effective_from": "current_date()", "effective_to": "null", "is_current": "true"
    }
).execute()
```

---

## C3. Gap and Island Problem

**Problem**: Given a sequence of dates, find contiguous ranges (islands) and gaps.

**Input**: `activity(user_id, active_date)` with dates: Jan 1, 2, 3, 5, 6, 8

**Solution (SQL)**:
```sql
WITH numbered AS (
    SELECT user_id, active_date,
        active_date - INTERVAL ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY active_date) DAY AS island_id
    FROM activity
)
SELECT user_id,
    MIN(active_date) AS island_start,
    MAX(active_date) AS island_end,
    DATEDIFF(MAX(active_date), MIN(active_date)) + 1 AS island_length
FROM numbered
GROUP BY user_id, island_id
ORDER BY island_start;
```

**Output**:
```
island_start | island_end | island_length
Jan 1        | Jan 3      | 3
Jan 5        | Jan 6      | 2
Jan 8        | Jan 8      | 1
```

---

## C4. Flatten JSON to Tabular

**Problem**: JSON column with nested structure. Flatten to columns.

**Input**:
```
+----+------------------------------------------+
| id | data (string)                             |
+----+------------------------------------------+
| 1  | {"name":"Alice","address":{"city":"NYC"}} |
+----+------------------------------------------+
```

**Solution (PySpark)**:
```python
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("name", StringType()),
    StructField("address", StructType([
        StructField("city", StringType())
    ]))
])

df = df.withColumn("parsed", from_json(col("data"), schema))
df = df.select("id", "parsed.name", "parsed.address.city")
```

---

## C5. Cumulative Distinct Count

**Problem**: Count the number of distinct users seen so far, by date.

**Solution (SQL)**:
```sql
WITH first_seen AS (
    SELECT user_id, MIN(event_date) AS first_date
    FROM events
    GROUP BY user_id
)
SELECT d.event_date,
    COUNT(f.user_id) AS cumulative_distinct_users
FROM (SELECT DISTINCT event_date FROM events) d
JOIN first_seen f ON f.first_date <= d.event_date
GROUP BY d.event_date
ORDER BY d.event_date;
```

---

## C6. Bucket/Bin Values into Ranges

**Problem**: Group values into ranges (0-10, 10-20, 20-30, etc.).

**Solution (PySpark)**:
```python
from pyspark.sql.functions import floor, col, concat, lit

df.withColumn("bucket",
    concat(
        (floor(col("value") / 10) * 10).cast("int"),
        lit("-"),
        (floor(col("value") / 10) * 10 + 10).cast("int")
    )
).groupBy("bucket").count()
```

**Solution (SQL)**:
```sql
SELECT CONCAT(FLOOR(value / 10) * 10, '-', FLOOR(value / 10) * 10 + 10) AS bucket,
       COUNT(*) AS cnt
FROM data
GROUP BY FLOOR(value / 10)
ORDER BY FLOOR(value / 10);
```

---

## C7. De-normalize: Combine Child Rows into Parent

**Problem**: Combine all order items into a single row per order.

**Solution (SQL)**:
```sql
SELECT o.order_id, o.customer,
    COLLECT_LIST(i.product_name) AS products,
    SUM(i.amount) AS total_amount
FROM orders o
JOIN order_items i ON o.order_id = i.order_id
GROUP BY o.order_id, o.customer;
```

**PySpark**:
```python
from pyspark.sql.functions import collect_list, sum as spark_sum

orders.join(items, "order_id") \
    .groupBy("order_id", "customer") \
    .agg(
        collect_list("product_name").alias("products"),
        spark_sum("amount").alias("total_amount")
    )
```

---

## C8. Time-Weighted Average

**Problem**: Compute the average value weighted by the duration each value was active.

**Solution (PySpark)**:
```python
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, unix_timestamp, sum as spark_sum

window = Window.partitionBy("device_id").orderBy("timestamp")

df = df.withColumn("next_ts", lead("timestamp").over(window))
df = df.withColumn("duration_sec",
    unix_timestamp("next_ts") - unix_timestamp("timestamp"))
df = df.filter(col("duration_sec").isNotNull())
df = df.withColumn("weighted_value", col("value") * col("duration_sec"))

result = df.groupBy("device_id").agg(
    (spark_sum("weighted_value") / spark_sum("duration_sec")).alias("time_weighted_avg")
)
```

---

## C9. Data Completeness Report

**Problem**: For each device and date, compute what percentage of expected hourly readings were received.

**Solution (PySpark)**:
```python
from pyspark.sql.functions import countDistinct, lit, col

expected_readings_per_day = 24

completeness = df.groupBy("device_id", "date").agg(
    countDistinct("hour").alias("received_hours")
).withColumn("expected_hours", lit(expected_readings_per_day)) \
 .withColumn("completeness_pct",
    (col("received_hours") / col("expected_hours") * 100).cast("decimal(5,2)"))
```

---

## C10. Cross-Join for Generating Combinations

**Problem**: Generate a complete grid of all devices x all dates (to find gaps).

**Solution**:
```python
all_devices = df.select("device_id").distinct()
all_dates = df.select("date").distinct()

complete_grid = all_devices.crossJoin(all_dates)
result = complete_grid.join(df, ["device_id", "date"], "left")
# Null rows in result = missing data
```

**Caution**: Cross joins produce N x M rows. Only use when both sides are small enough.

---

## End of Document

This document covers 46 hands-on problems across PySpark, SQL, and data transformations. Practice writing solutions from scratch without looking at the answers -- that's what interviewers expect.

For conceptual preparation, see `Interview_Concepts_and_Questions.md`.
For project-specific preparation, see `Interview_Prep_Guide.md`.
For Databricks-specific preparation, see `Databricks_Interview_Guide.md`.
