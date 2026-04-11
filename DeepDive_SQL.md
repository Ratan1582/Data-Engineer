# SQL for Data Engineers -- A Complete Deep Dive

**From Query Execution Internals to Advanced Analytical Patterns**

---

## Table of Contents

1. [How SQL Executes](#1-how-sql-executes)
2. [Joins In Depth](#2-joins-in-depth)
3. [Window Functions Complete Guide](#3-window-functions-complete-guide)
4. [CTEs and Subqueries](#4-ctes-and-subqueries)
5. [Aggregation Patterns](#5-aggregation-patterns)
6. [Set Operations](#6-set-operations)
7. [NULL Semantics](#7-null-semantics)
8. [Advanced Analytical Patterns](#8-advanced-analytical-patterns)
9. [Date and Time Manipulation](#9-date-and-time-manipulation)
10. [String Functions and Regex](#10-string-functions-and-regex)
11. [Query Optimization](#11-query-optimization)
12. [Spark SQL Specifics](#12-spark-sql-specifics)
13. [50+ Practice Problems](#13-50-practice-problems)
14. [Quick-Reference Cheat Sheet](#14-quick-reference-cheat-sheet)

---

## 1. How SQL Executes

### 1.1 Execution Order

SQL does **not** execute in the order you write it. The logical execution order is:

```
Written Order:          Execution Order:
SELECT                  1. FROM / JOIN
FROM                    2. WHERE
WHERE                   3. GROUP BY
GROUP BY                4. HAVING
HAVING                  5. SELECT
ORDER BY                6. DISTINCT
LIMIT                   7. ORDER BY
                        8. LIMIT / OFFSET
```

This matters because:
- You cannot use a column alias from `SELECT` in `WHERE` (WHERE executes before SELECT)
- You CAN use a column alias in `ORDER BY` (ORDER BY executes after SELECT)
- Window functions execute after `WHERE` and `GROUP BY` but before `ORDER BY`

### 1.2 Query Processing Pipeline

When a database engine receives a SQL query:

```
SQL Text
  │
  ▼
Parser ─── Syntax check, build parse tree
  │
  ▼
Analyzer ─── Resolve table/column names, check types
  │
  ▼
Optimizer ─── Generate execution plans, pick cheapest (cost-based)
  │
  ▼
Physical Plan ─── Actual operations (hash join, index scan, sort, etc.)
  │
  ▼
Execution Engine ─── Run the plan, return results
```

In Spark SQL, the optimizer is **Catalyst**, and the execution engine is **Tungsten**. The same pipeline applies:
```python
df.filter(col("age") > 30).select("name", "age").explain(True)
```
This shows: Parsed Plan → Analyzed Plan → Optimized Plan → Physical Plan.

### 1.3 Cost-Based Optimization

The optimizer evaluates multiple execution plans and picks the cheapest based on:
- **Table statistics**: row count, column cardinality, data distribution
- **Join ordering**: which table to scan first
- **Join strategy**: hash join vs sort-merge join vs nested loop
- **Index usage**: whether to use an index or full table scan
- **Predicate pushdown**: push filters as close to the data source as possible

```sql
-- Spark: collect statistics for cost-based optimization
ANALYZE TABLE sales COMPUTE STATISTICS;
ANALYZE TABLE sales COMPUTE STATISTICS FOR COLUMNS product_id, amount;
```

---

## 2. Joins In Depth

### 2.1 Join Types

```
Table A: {1, 2, 3, 4}
Table B: {3, 4, 5, 6}

INNER JOIN:       {3, 4}         -- rows matching in BOTH tables
LEFT JOIN:        {1, 2, 3, 4}   -- all from A, matching from B (nulls for no match)
RIGHT JOIN:       {3, 4, 5, 6}   -- all from B, matching from A
FULL OUTER JOIN:  {1, 2, 3, 4, 5, 6}  -- all from both, nulls where no match
CROSS JOIN:       {(1,3),(1,4),(1,5),(1,6),(2,3),...}  -- every combination (cartesian)
LEFT SEMI JOIN:   {3, 4}         -- rows from A that have a match in B (no B columns)
LEFT ANTI JOIN:   {1, 2}         -- rows from A that do NOT match in B
```

### 2.2 SQL Examples

```sql
-- Setup
CREATE TABLE employees (id INT, name STRING, dept_id INT);
CREATE TABLE departments (id INT, dept_name STRING);

-- INNER JOIN: employees who belong to a department
SELECT e.name, d.dept_name
FROM employees e
INNER JOIN departments d ON e.dept_id = d.id;

-- LEFT JOIN: all employees, with department if available
SELECT e.name, d.dept_name
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id;

-- FULL OUTER JOIN: all employees and all departments
SELECT e.name, d.dept_name
FROM employees e
FULL OUTER JOIN departments d ON e.dept_id = d.id;

-- CROSS JOIN: every employee paired with every department
SELECT e.name, d.dept_name
FROM employees e
CROSS JOIN departments d;

-- LEFT SEMI JOIN (Spark SQL): employees who have a matching department
SELECT e.*
FROM employees e
LEFT SEMI JOIN departments d ON e.dept_id = d.id;
-- Equivalent to: SELECT * FROM employees WHERE dept_id IN (SELECT id FROM departments)

-- LEFT ANTI JOIN (Spark SQL): employees without a matching department
SELECT e.*
FROM employees e
LEFT ANTI JOIN departments d ON e.dept_id = d.id;
-- Equivalent to: SELECT * FROM employees WHERE dept_id NOT IN (SELECT id FROM departments)
```

### 2.3 Join Algorithms

**Nested Loop Join** (worst case):
```
For each row in A:
    For each row in B:
        If A.key == B.key: emit (A, B)
        
Complexity: O(|A| × |B|)
```
Only used when no better option exists (e.g., non-equi joins with no useful index).

**Hash Join**:
```
1. Build phase: Load smaller table (B) into a hash table, keyed by join column
2. Probe phase: For each row in larger table (A), look up B's hash table

Complexity: O(|A| + |B|)
Memory: must fit smaller table in memory
```

**Sort-Merge Join**:
```
1. Sort both A and B by join key
2. Merge: walk through both sorted datasets simultaneously

Complexity: O(|A| log |A| + |B| log |B|) for sorting, O(|A| + |B|) for merge
Works well when data is already sorted or for very large datasets
```

**Broadcast Join** (Spark-specific):
```
1. Collect the small table entirely on the driver
2. Broadcast it to every executor
3. Each executor joins its partition of the large table with the broadcast table locally

No shuffle of the large table required. Fastest join for small-large combinations.
Default threshold: 10 MB (spark.sql.autoBroadcastJoinThreshold)
```

### 2.4 Choosing the Right Join

```
Is one table small (< 10 MB)?
├── YES → Broadcast Hash Join (fastest, no shuffle)
└── NO
    ├── Are both tables sorted by join key?
    │   ├── YES → Sort-Merge Join (no sorting needed)
    │   └── NO → Sort-Merge Join or Shuffle Hash Join
    │       └── Is one table much smaller but > 10 MB?
    │           ├── YES → Shuffle Hash Join (build hash on smaller)
    │           └── NO → Sort-Merge Join (default for large-large)
```

### 2.5 Common Join Pitfalls

**1. Cartesian product from wrong join condition**:
```sql
-- WRONG: missing ON clause creates cartesian product
SELECT * FROM orders, customers;  -- |orders| × |customers| rows

-- RIGHT: specify join condition
SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id;
```

**2. Duplicate rows from many-to-many joins**:
```sql
-- If both tables have duplicates on the join key, rows multiply
-- orders: {(1, A), (1, B)}
-- payments: {(1, X), (1, Y)}
-- Result: {(1,A,X), (1,A,Y), (1,B,X), (1,B,Y)} -- 4 rows, not 2!
```

**3. NULL handling in joins**:
```sql
-- NULL != NULL in join conditions
-- Rows with NULL join keys will NOT match in inner/outer joins
SELECT * FROM a JOIN b ON a.key = b.key;
-- If a.key is NULL and b.key is NULL, they do NOT join

-- Use null-safe comparison if you want NULLs to match:
SELECT * FROM a JOIN b ON a.key <=> b.key;  -- Spark SQL null-safe equal
```

---

## 3. Window Functions Complete Guide

### 3.1 What Are Window Functions

A window function computes a value for each row based on a "window" of related rows, **without collapsing rows** (unlike GROUP BY which reduces to one row per group).

```sql
-- GROUP BY: collapses rows
SELECT dept, AVG(salary) FROM employees GROUP BY dept;
-- Result: one row per department

-- Window function: keeps all rows, adds computed column
SELECT name, dept, salary,
       AVG(salary) OVER (PARTITION BY dept) as dept_avg
FROM employees;
-- Result: one row per employee, with department average alongside
```

### 3.2 Window Function Syntax

```sql
function_name(args) OVER (
    [PARTITION BY col1, col2, ...]     -- divide rows into groups (like GROUP BY but no collapse)
    [ORDER BY col3, col4, ...]          -- sort rows within each partition
    [frame_clause]                      -- define the window boundaries
)
```

### 3.3 Ranking Functions

**ROW_NUMBER()**: Assigns a unique sequential number to each row within a partition. No ties.

```sql
SELECT name, dept, salary,
       ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn
FROM employees;

-- Result:
-- Alice  | Sales  | 80000 | 1
-- Bob    | Sales  | 70000 | 2
-- Carol  | Sales  | 70000 | 3  ← different from Bob despite same salary
-- Dave   | Eng    | 90000 | 1
-- Eve    | Eng    | 85000 | 2
```

**RANK()**: Like ROW_NUMBER but ties get the same rank. Gaps after ties.

```sql
SELECT name, dept, salary,
       RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rnk
FROM employees;

-- Bob and Carol both get rank 2 (tied salary)
-- Next person would get rank 4 (skips 3)
```

**DENSE_RANK()**: Like RANK but no gaps.

```sql
SELECT name, dept, salary,
       DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as dense_rnk
FROM employees;

-- Bob and Carol both get rank 2
-- Next person gets rank 3 (no gap)
```

**NTILE(n)**: Divides rows into n approximately equal buckets.

```sql
SELECT name, salary,
       NTILE(4) OVER (ORDER BY salary) as quartile
FROM employees;
-- Divides all employees into 4 salary quartiles
```

**When to use which**:
| Function | Ties | Gaps | Use Case |
|----------|------|------|----------|
| ROW_NUMBER | No ties (arbitrary) | N/A | Deduplication, pagination, top-N per group |
| RANK | Same rank for ties | Yes | Competition ranking (1st, 2nd, 2nd, 4th) |
| DENSE_RANK | Same rank for ties | No | Category ranking without gaps |
| NTILE | N/A | N/A | Percentile buckets, quartiles |

### 3.4 Value Functions

**LAG(col, offset, default)**: Access a row before the current row.

```sql
SELECT date, revenue,
       LAG(revenue, 1, 0) OVER (ORDER BY date) as prev_day_revenue,
       revenue - LAG(revenue, 1, 0) OVER (ORDER BY date) as daily_change
FROM daily_sales;

-- Result:
-- 2024-01-01 | 1000 |    0 | 1000
-- 2024-01-02 | 1200 | 1000 |  200
-- 2024-01-03 | 1100 | 1200 | -100
```

**LEAD(col, offset, default)**: Access a row after the current row.

```sql
SELECT date, revenue,
       LEAD(revenue, 1) OVER (ORDER BY date) as next_day_revenue
FROM daily_sales;

-- 2024-01-01 | 1000 | 1200
-- 2024-01-02 | 1200 | 1100
-- 2024-01-03 | 1100 | NULL  ← no next row
```

**FIRST_VALUE / LAST_VALUE / NTH_VALUE**:

```sql
SELECT name, dept, salary,
       FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary DESC) as highest_paid,
       LAST_VALUE(name) OVER (
           PARTITION BY dept ORDER BY salary DESC
           ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
       ) as lowest_paid
FROM employees;
```

**LAST_VALUE gotcha**: By default, the window frame is `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, so LAST_VALUE returns the current row (not the actual last). Always specify `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` for LAST_VALUE.

### 3.5 Aggregate Window Functions

Any aggregate function can be used as a window function:

```sql
SELECT name, dept, salary,
       SUM(salary) OVER (PARTITION BY dept) as dept_total,
       COUNT(*) OVER (PARTITION BY dept) as dept_count,
       AVG(salary) OVER (PARTITION BY dept) as dept_avg,
       MAX(salary) OVER (PARTITION BY dept) as dept_max,
       MIN(salary) OVER (PARTITION BY dept) as dept_min
FROM employees;
```

### 3.6 Window Frames

The frame defines which rows relative to the current row are included in the computation.

```sql
ROWS BETWEEN <start> AND <end>

-- Start/end can be:
UNBOUNDED PRECEDING    -- from the very first row in the partition
N PRECEDING            -- N rows before current
CURRENT ROW            -- the current row
N FOLLOWING            -- N rows after current
UNBOUNDED FOLLOWING    -- to the very last row in the partition
```

**ROWS vs RANGE**:
- `ROWS`: Physical rows. `3 PRECEDING` means exactly 3 rows before.
- `RANGE`: Logical range. `3 PRECEDING` means all rows within value range of 3 from current row's ORDER BY value.

```sql
-- Running total (cumulative sum)
SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- 3-day moving average
AVG(amount) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)

-- Total across entire partition
SUM(amount) OVER (PARTITION BY dept ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

**Default frame**:
- With `ORDER BY`: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` (running total behavior)
- Without `ORDER BY`: `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` (entire partition)

### 3.7 Practical Window Function Patterns

**Top N per group**:
```sql
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn
    FROM employees
)
SELECT * FROM ranked WHERE rn <= 3;
```

**Running total**:
```sql
SELECT date, amount,
       SUM(amount) OVER (ORDER BY date) as running_total
FROM transactions;
```

**Percentage of total**:
```sql
SELECT name, dept, salary,
       ROUND(salary * 100.0 / SUM(salary) OVER (PARTITION BY dept), 2) as pct_of_dept
FROM employees;
```

**Year-over-year growth**:
```sql
SELECT year, revenue,
       LAG(revenue) OVER (ORDER BY year) as prev_year,
       ROUND((revenue - LAG(revenue) OVER (ORDER BY year)) * 100.0 
             / LAG(revenue) OVER (ORDER BY year), 2) as yoy_growth_pct
FROM annual_revenue;
```

---

## 4. CTEs and Subqueries

### 4.1 Common Table Expressions (CTEs)

A CTE is a named temporary result set that exists for the duration of a single query:

```sql
WITH active_users AS (
    SELECT user_id, name, last_login
    FROM users
    WHERE last_login > DATE_SUB(CURRENT_DATE, 30)
),
user_orders AS (
    SELECT user_id, COUNT(*) as order_count, SUM(amount) as total_spent
    FROM orders
    WHERE order_date > DATE_SUB(CURRENT_DATE, 30)
    GROUP BY user_id
)
SELECT a.name, a.last_login, u.order_count, u.total_spent
FROM active_users a
JOIN user_orders u ON a.user_id = u.user_id
ORDER BY u.total_spent DESC;
```

**Advantages of CTEs**:
- Readability: break complex queries into named steps
- Reusability: reference the same CTE multiple times
- Maintainability: easier to debug individual CTEs

### 4.2 Recursive CTEs

Recursive CTEs reference themselves, useful for hierarchical data:

```sql
-- Organization hierarchy: find all reports (direct + indirect) of a manager
WITH RECURSIVE reports AS (
    -- Base case: direct reports
    SELECT id, name, manager_id, 1 as level
    FROM employees
    WHERE manager_id = 100  -- Start from manager with id=100
    
    UNION ALL
    
    -- Recursive step: reports of reports
    SELECT e.id, e.name, e.manager_id, r.level + 1
    FROM employees e
    JOIN reports r ON e.manager_id = r.id
)
SELECT * FROM reports ORDER BY level, name;
```

**How it works**:
1. Execute the base case (non-recursive part): direct reports of manager 100
2. Execute the recursive part using the previous result as input
3. Repeat step 2 until no new rows are produced
4. UNION ALL combines all iterations

**Note**: Spark SQL supports recursive CTEs starting from Spark 3.4. Before that, you would use iterative DataFrame operations.

### 4.3 Subqueries

**Scalar subquery**: Returns a single value.
```sql
SELECT name, salary,
       (SELECT AVG(salary) FROM employees) as company_avg
FROM employees;
```

**Correlated subquery**: References the outer query. Executes once per outer row (can be slow).
```sql
SELECT name, salary, dept
FROM employees e1
WHERE salary > (
    SELECT AVG(salary)
    FROM employees e2
    WHERE e2.dept = e1.dept  -- references outer query's dept
);
```

**EXISTS / NOT EXISTS**:
```sql
-- Customers who have placed at least one order
SELECT c.name
FROM customers c
WHERE EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.id
);

-- Customers who have never ordered
SELECT c.name
FROM customers c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o WHERE o.customer_id = c.id
);
```

### 4.4 EXISTS vs IN

```sql
-- These are logically equivalent:
WHERE customer_id IN (SELECT id FROM customers WHERE active = true)
WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = orders.customer_id AND c.active = true)
```

**Performance differences**:
- `IN` with a subquery: the subquery result is materialized, then used for lookup. If the subquery returns many rows, this can be expensive.
- `EXISTS`: can short-circuit (stops as soon as one match is found). Better for large subquery results.
- `IN` with `NULL`s: `NOT IN` returns empty if the subquery contains any NULL. `NOT EXISTS` handles NULLs correctly.

```sql
-- DANGEROUS: If orders.amount has any NULLs, NOT IN returns NO rows
SELECT * FROM products WHERE id NOT IN (SELECT product_id FROM orders);

-- SAFE: NOT EXISTS handles NULLs correctly
SELECT * FROM products p WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.product_id = p.id);
```

---

## 5. Aggregation Patterns

### 5.1 GROUP BY Basics

```sql
SELECT dept, COUNT(*) as cnt, AVG(salary) as avg_sal, MAX(salary) as max_sal
FROM employees
GROUP BY dept;
```

Every column in SELECT must be either in GROUP BY or an aggregate function. This is a common error:
```sql
-- WRONG: 'name' is not in GROUP BY and not aggregated
SELECT dept, name, COUNT(*) FROM employees GROUP BY dept;

-- RIGHT: aggregate name or add to GROUP BY
SELECT dept, COLLECT_LIST(name) as names, COUNT(*) FROM employees GROUP BY dept;
```

### 5.2 HAVING

HAVING filters groups (after GROUP BY), while WHERE filters rows (before GROUP BY):

```sql
-- Departments with more than 5 employees and average salary > 50000
SELECT dept, COUNT(*) as cnt, AVG(salary) as avg_sal
FROM employees
WHERE active = true          -- filters rows before grouping
GROUP BY dept
HAVING COUNT(*) > 5          -- filters groups after grouping
   AND AVG(salary) > 50000;
```

### 5.3 GROUPING SETS

GROUPING SETS allow multiple grouping levels in a single query:

```sql
-- Instead of UNION ALL of three separate GROUP BY queries:
SELECT region, product, SUM(sales) as total
FROM sales_data
GROUP BY GROUPING SETS (
    (region, product),    -- sales by region AND product
    (region),             -- sales by region only
    ()                    -- grand total
);
```

This is equivalent to but more efficient than:
```sql
SELECT region, product, SUM(sales) FROM sales_data GROUP BY region, product
UNION ALL
SELECT region, NULL, SUM(sales) FROM sales_data GROUP BY region
UNION ALL
SELECT NULL, NULL, SUM(sales) FROM sales_data;
```

### 5.4 ROLLUP and CUBE

**ROLLUP**: Hierarchical subtotals (right-to-left removal of columns):
```sql
SELECT region, city, product, SUM(sales)
FROM sales_data
GROUP BY ROLLUP(region, city, product);
-- Groups: (region, city, product), (region, city), (region), ()
```

**CUBE**: All possible combinations of subtotals:
```sql
SELECT region, product, SUM(sales)
FROM sales_data
GROUP BY CUBE(region, product);
-- Groups: (region, product), (region), (product), ()
```

**GROUPING() function**: Indicates whether a column is aggregated (NULL because of grouping):
```sql
SELECT 
    CASE WHEN GROUPING(region) = 1 THEN 'ALL' ELSE region END as region,
    CASE WHEN GROUPING(product) = 1 THEN 'ALL' ELSE product END as product,
    SUM(sales) as total
FROM sales_data
GROUP BY CUBE(region, product);
```

---

## 6. Set Operations

### 6.1 UNION vs UNION ALL

```sql
-- UNION: combines and removes duplicates (slower -- requires sort/hash)
SELECT name FROM employees_us
UNION
SELECT name FROM employees_uk;

-- UNION ALL: combines without deduplication (faster)
SELECT name FROM employees_us
UNION ALL
SELECT name FROM employees_uk;
```

**Rule**: Always use UNION ALL unless you specifically need deduplication. UNION requires a full sort/hash to deduplicate, which is expensive for large datasets.

### 6.2 INTERSECT and EXCEPT

```sql
-- INTERSECT: rows in BOTH queries
SELECT product_id FROM orders_2023
INTERSECT
SELECT product_id FROM orders_2024;
-- Products ordered in both years

-- EXCEPT (MINUS in Oracle): rows in first but NOT in second
SELECT product_id FROM orders_2023
EXCEPT
SELECT product_id FROM orders_2024;
-- Products ordered in 2023 but not in 2024
```

### 6.3 NULL Handling in Set Operations

Set operations treat NULLs as equal (unlike joins):
```sql
SELECT NULL
UNION
SELECT NULL;
-- Result: one row with NULL (deduplicated)
```

---

## 7. NULL Semantics

### 7.1 Three-Valued Logic

SQL uses three-valued logic: TRUE, FALSE, and **UNKNOWN**. Any comparison involving NULL yields UNKNOWN:

```sql
NULL = NULL     → UNKNOWN (not TRUE!)
NULL != NULL    → UNKNOWN (not TRUE!)
NULL > 5        → UNKNOWN
NULL AND TRUE   → UNKNOWN
NULL OR TRUE    → TRUE (TRUE dominates OR)
NULL AND FALSE  → FALSE (FALSE dominates AND)
```

### 7.2 IS NULL / IS NOT NULL

The only way to check for NULL:

```sql
-- WRONG: never matches (NULL = NULL is UNKNOWN, not TRUE)
SELECT * FROM employees WHERE manager_id = NULL;

-- RIGHT:
SELECT * FROM employees WHERE manager_id IS NULL;
SELECT * FROM employees WHERE manager_id IS NOT NULL;
```

### 7.3 COALESCE and IFNULL

```sql
-- COALESCE: returns first non-NULL argument
SELECT COALESCE(phone, email, 'N/A') as contact FROM customers;

-- IFNULL (Spark/MySQL): two-argument version of COALESCE
SELECT IFNULL(phone, 'Unknown') as phone FROM customers;

-- NULLIF: returns NULL if arguments are equal
SELECT NULLIF(quantity, 0) as safe_quantity FROM orders;
-- Useful to avoid division by zero: revenue / NULLIF(quantity, 0)
```

### 7.4 NULL in Aggregations

Aggregate functions **ignore NULLs** (except COUNT(*)):

```sql
-- Given: salaries = [50000, NULL, 60000, NULL, 70000]
SELECT 
    COUNT(*) as total_rows,        -- 5 (counts all rows including NULL)
    COUNT(salary) as non_null,     -- 3 (counts only non-NULL values)
    SUM(salary) as total,          -- 180000 (ignores NULLs)
    AVG(salary) as average         -- 60000 (180000/3, not 180000/5)
FROM employees;
```

### 7.5 NULL-Safe Comparison (Spark SQL)

```sql
-- Standard: NULL = NULL → UNKNOWN
-- Spark null-safe: NULL <=> NULL → TRUE
SELECT * FROM a JOIN b ON a.key <=> b.key;

-- IS NOT DISTINCT FROM (standard SQL equivalent):
SELECT * FROM a JOIN b ON a.key IS NOT DISTINCT FROM b.key;
```

---

## 8. Advanced Analytical Patterns

### 8.1 Gap-and-Island Problem

Find groups of consecutive values (islands) separated by gaps:

```sql
-- Given: active dates = {1, 2, 3, 5, 6, 8, 9, 10}
-- Islands: {1-3}, {5-6}, {8-10}

WITH numbered AS (
    SELECT date_val,
           date_val - ROW_NUMBER() OVER (ORDER BY date_val) as grp
    FROM active_dates
)
SELECT MIN(date_val) as island_start, MAX(date_val) as island_end
FROM numbered
GROUP BY grp
ORDER BY island_start;

-- How it works:
-- date_val: 1  2  3  5  6  8  9  10
-- row_num:  1  2  3  4  5  6  7  8
-- grp:      0  0  0  1  1  2  2  2
-- Same 'grp' = same island
```

### 8.2 Consecutive Days / Streaks

```sql
-- Find users with 3+ consecutive login days
WITH login_groups AS (
    SELECT user_id, login_date,
           login_date - ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY login_date) * INTERVAL '1 day' as grp
    FROM (SELECT DISTINCT user_id, login_date FROM logins) t
)
SELECT user_id, MIN(login_date) as streak_start, MAX(login_date) as streak_end,
       COUNT(*) as streak_length
FROM login_groups
GROUP BY user_id, grp
HAVING COUNT(*) >= 3;
```

### 8.3 Sessionization

Group events into sessions (a session ends after N minutes of inactivity):

```sql
WITH time_gaps AS (
    SELECT user_id, event_time,
           CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) > INTERVAL '30 minutes'
                THEN 1 ELSE 0
           END as new_session
    FROM events
),
sessions AS (
    SELECT user_id, event_time,
           SUM(new_session) OVER (PARTITION BY user_id ORDER BY event_time) as session_id
    FROM time_gaps
)
SELECT user_id, session_id,
       MIN(event_time) as session_start,
       MAX(event_time) as session_end,
       COUNT(*) as events_in_session
FROM sessions
GROUP BY user_id, session_id;
```

### 8.4 Running Total and Cumulative Sum

```sql
-- Running total of sales
SELECT date, amount,
       SUM(amount) OVER (ORDER BY date) as cumulative
FROM daily_sales;

-- Running total reset each month
SELECT date, amount,
       SUM(amount) OVER (PARTITION BY YEAR(date), MONTH(date) ORDER BY date) as monthly_cumulative
FROM daily_sales;

-- Cumulative percentage
SELECT date, amount,
       SUM(amount) OVER (ORDER BY date) * 100.0 / SUM(amount) OVER () as cumulative_pct
FROM daily_sales;
```

### 8.5 Moving Average

```sql
-- 7-day moving average
SELECT date, amount,
       AVG(amount) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as ma_7d
FROM daily_sales;

-- 30-day moving average (requires at least 30 rows)
SELECT date, amount,
       CASE WHEN ROW_NUMBER() OVER (ORDER BY date) >= 30
            THEN AVG(amount) OVER (ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
       END as ma_30d
FROM daily_sales;
```

### 8.6 Pivot and Unpivot

**Pivot** (rows to columns):
```sql
-- Convert monthly revenue rows into columns
SELECT product,
       SUM(CASE WHEN month = 'Jan' THEN revenue ELSE 0 END) as Jan,
       SUM(CASE WHEN month = 'Feb' THEN revenue ELSE 0 END) as Feb,
       SUM(CASE WHEN month = 'Mar' THEN revenue ELSE 0 END) as Mar
FROM monthly_revenue
GROUP BY product;

-- Spark SQL has built-in PIVOT:
SELECT * FROM monthly_revenue
PIVOT (SUM(revenue) FOR month IN ('Jan', 'Feb', 'Mar'));
```

**Unpivot** (columns to rows):
```sql
-- Spark SQL UNPIVOT (3.4+):
SELECT * FROM quarterly_report
UNPIVOT (revenue FOR quarter IN (Q1, Q2, Q3, Q4));

-- Before Spark 3.4, use UNION ALL or stack():
SELECT product, 'Q1' as quarter, Q1 as revenue FROM quarterly_report
UNION ALL
SELECT product, 'Q2', Q2 FROM quarterly_report
UNION ALL
SELECT product, 'Q3', Q3 FROM quarterly_report
UNION ALL
SELECT product, 'Q4', Q4 FROM quarterly_report;

-- Or with stack():
SELECT product, quarter, revenue
FROM quarterly_report
LATERAL VIEW stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) t AS quarter, revenue;
```

### 8.7 Self-Join Patterns

```sql
-- Employees who earn more than their manager
SELECT e.name as employee, e.salary as emp_salary,
       m.name as manager, m.salary as mgr_salary
FROM employees e
JOIN employees m ON e.manager_id = m.id
WHERE e.salary > m.salary;

-- Find pairs of products bought together (co-occurrence)
SELECT a.product_id as prod_a, b.product_id as prod_b, COUNT(*) as co_count
FROM order_items a
JOIN order_items b ON a.order_id = b.order_id AND a.product_id < b.product_id
GROUP BY a.product_id, b.product_id
ORDER BY co_count DESC;
```

### 8.8 Deduplication

```sql
-- Keep the most recent record per user
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) as rn
    FROM user_profiles
)
SELECT * FROM ranked WHERE rn = 1;

-- Alternative: QUALIFY (Spark SQL, Snowflake, BigQuery)
SELECT *
FROM user_profiles
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) = 1;
```

---

## 9. Date and Time Manipulation

### 9.1 Common Functions

```sql
-- Current date/time
SELECT CURRENT_DATE, CURRENT_TIMESTAMP;

-- Date extraction
SELECT 
    YEAR(order_date) as yr,
    MONTH(order_date) as mo,
    DAY(order_date) as dy,
    DAYOFWEEK(order_date) as dow,    -- 1=Sunday, 7=Saturday
    QUARTER(order_date) as qtr,
    WEEKOFYEAR(order_date) as wk
FROM orders;

-- Date truncation
SELECT DATE_TRUNC('month', order_date) as month_start  -- first day of month
FROM orders;

-- Date arithmetic
SELECT 
    DATE_ADD(order_date, 7) as plus_7_days,
    DATE_SUB(order_date, 30) as minus_30_days,
    DATEDIFF(ship_date, order_date) as days_to_ship,
    MONTHS_BETWEEN(end_date, start_date) as month_diff
FROM orders;

-- Date formatting
SELECT DATE_FORMAT(order_date, 'yyyy-MM-dd HH:mm:ss') as formatted
FROM orders;

-- String to date
SELECT TO_DATE('2024-01-15', 'yyyy-MM-dd') as parsed_date;
SELECT TO_TIMESTAMP('2024-01-15 14:30:00', 'yyyy-MM-dd HH:mm:ss') as parsed_ts;
```

### 9.2 Time Zone Handling

```sql
-- Convert timestamp between time zones
SELECT FROM_UTC_TIMESTAMP(event_time, 'Asia/Kolkata') as ist_time
FROM events;

SELECT TO_UTC_TIMESTAMP(local_time, 'Asia/Kolkata') as utc_time
FROM events;
```

### 9.3 Date-Based Patterns

```sql
-- Group by month
SELECT DATE_TRUNC('month', order_date) as month, SUM(amount)
FROM orders GROUP BY DATE_TRUNC('month', order_date);

-- Last 30 days
SELECT * FROM orders WHERE order_date >= DATE_SUB(CURRENT_DATE, 30);

-- Same day last year
SELECT * FROM orders 
WHERE order_date = DATE_SUB(CURRENT_DATE, 365);

-- Business days (exclude weekends)
SELECT COUNT(*) as business_days
FROM (
    SELECT EXPLODE(SEQUENCE(start_date, end_date)) as d
) t
WHERE DAYOFWEEK(d) NOT IN (1, 7);
```

---

## 10. String Functions and Regex

### 10.1 Common String Functions

```sql
SELECT 
    LENGTH('hello') as len,                    -- 5
    UPPER('hello') as upper_case,              -- HELLO
    LOWER('HELLO') as lower_case,              -- hello
    TRIM('  hello  ') as trimmed,              -- hello
    LTRIM('  hello') as left_trimmed,          -- hello
    RTRIM('hello  ') as right_trimmed,         -- hello
    SUBSTRING('hello world', 1, 5) as sub,     -- hello
    CONCAT('hello', ' ', 'world') as joined,   -- hello world
    CONCAT_WS(', ', 'a', 'b', 'c') as csv,    -- a, b, c
    REPLACE('hello world', 'world', 'sql') as replaced,  -- hello sql
    SPLIT('a,b,c', ',') as arr,                -- ['a', 'b', 'c']
    INSTR('hello', 'll') as position,          -- 3
    LPAD('42', 5, '0') as padded,              -- 00042
    REVERSE('hello') as reversed               -- olleh
FROM dual;
```

### 10.2 Regex Functions

```sql
-- REGEXP_EXTRACT: extract matching group
SELECT REGEXP_EXTRACT('email: user@domain.com', '([\\w.]+)@([\\w.]+)', 1) as username;
-- Result: user

-- REGEXP_REPLACE: replace matches
SELECT REGEXP_REPLACE('Phone: 123-456-7890', '[^0-9]', '') as digits_only;
-- Result: 1234567890

-- RLIKE / REGEXP: boolean match
SELECT * FROM logs WHERE message RLIKE 'ERROR|FATAL';

-- Extract all numbers from a string
SELECT REGEXP_EXTRACT_ALL('abc123def456', '[0-9]+') as numbers;
-- Result: ['123', '456']
```

---

## 11. Query Optimization

### 11.1 Reading Execution Plans

```sql
EXPLAIN SELECT * FROM orders WHERE amount > 100;
EXPLAIN EXTENDED SELECT * FROM orders JOIN customers ON orders.cust_id = customers.id;
```

In Spark:
```python
df.explain()           # Physical plan only
df.explain(True)       # All plans (parsed, analyzed, optimized, physical)
df.explain("formatted") # Human-readable formatted plan
```

Key things to look for in an execution plan:
- **Scan type**: FileScan (full scan) vs FilteredScan (with pushdown)
- **Join type**: BroadcastHashJoin vs SortMergeJoin vs ShuffledHashJoin
- **Exchange**: Indicates a shuffle (data movement across executors)
- **Sort**: Sorting operations (expensive for large data)
- **Filter pushdown**: Filters appear at the scan level, not after

### 11.2 Predicate Pushdown

Push filters as close to the data source as possible:

```sql
-- GOOD: filter pushed to Parquet scan (reads less data)
SELECT * FROM parquet.`/data/table/` WHERE year = 2024 AND amount > 100;
-- Spark pushes year (partition pruning) and amount > 100 (Parquet row group stats) to the scan

-- BAD: UDF prevents pushdown
SELECT * FROM parquet.`/data/table/` WHERE my_udf(amount) > 100;
-- Spark cannot push my_udf to Parquet -- must read all data, then filter
```

### 11.3 Column Pruning

Only select columns you need:

```sql
-- BAD: reads ALL columns from Parquet
SELECT * FROM large_table WHERE id = 123;

-- GOOD: reads only 2 columns
SELECT name, email FROM large_table WHERE id = 123;
```

For a 100-column Parquet table, selecting 2 columns reads ~2% of the data.

### 11.4 Partition Pruning

```sql
-- Table partitioned by (year, month)
-- GOOD: only reads year=2024/month=01/ partition
SELECT * FROM events WHERE year = 2024 AND month = 1;

-- BAD: expression on partition column prevents pruning
SELECT * FROM events WHERE year * 100 + month = 202401;
-- Spark cannot simplify this expression, reads all partitions
```

### 11.5 Join Optimization Tips

```sql
-- 1. Use broadcast hint for small tables
SELECT /*+ BROADCAST(small_table) */ *
FROM large_table l JOIN small_table s ON l.key = s.key;

-- 2. Filter before joining
SELECT * 
FROM orders o 
JOIN customers c ON o.cust_id = c.id
WHERE o.order_date > '2024-01-01';
-- Spark automatically pushes the date filter before the join

-- 3. Avoid cartesian joins
-- BAD: missing join condition
SELECT * FROM a, b WHERE a.x > b.y;  -- cartesian with filter

-- 4. Use appropriate join type
-- Instead of LEFT JOIN + WHERE IS NULL for anti-join:
SELECT * FROM a LEFT ANTI JOIN b ON a.id = b.id;
```

### 11.6 Avoiding Common Performance Pitfalls

```sql
-- 1. DISTINCT is expensive (requires hash/sort)
-- Use GROUP BY if you also need aggregation
SELECT DISTINCT dept FROM employees;           -- OK for small result
SELECT dept, COUNT(*) FROM employees GROUP BY dept;  -- Better if you need counts too

-- 2. ORDER BY is expensive (global sort)
-- Only use when you truly need ordered output
SELECT * FROM events ORDER BY event_time;      -- full sort of entire dataset
SELECT * FROM events ORDER BY event_time LIMIT 100;  -- much cheaper (top-K)

-- 3. Use UNION ALL instead of UNION
-- UNION deduplicates (expensive), UNION ALL does not

-- 4. Avoid functions on indexed/partitioned columns in WHERE
-- BAD: function prevents partition pruning / index usage
WHERE YEAR(order_date) = 2024
-- GOOD: compare directly
WHERE order_date >= '2024-01-01' AND order_date < '2025-01-01'
```

---

## 12. Spark SQL Specifics

### 12.1 Spark SQL vs DataFrame API

Both compile to the same execution plan:

```python
# DataFrame API
df.filter(col("age") > 30).select("name", "age").groupBy("name").count()

# Equivalent Spark SQL
spark.sql("SELECT name, COUNT(*) FROM people WHERE age > 30 GROUP BY name")
```

Use Spark SQL when:
- Complex queries are easier to read in SQL
- You are prototyping in notebooks
- You want to use SQL-specific features (PIVOT, QUALIFY)

Use DataFrame API when:
- You need programmatic query construction
- You want compile-time column name checking (with typed datasets)
- You are building reusable functions/modules

### 12.2 Spark SQL Hints

```sql
-- Broadcast join hint
SELECT /*+ BROADCAST(small) */ * FROM large JOIN small ON large.id = small.id;

-- Shuffle hash join hint
SELECT /*+ SHUFFLE_HASH(b) */ * FROM a JOIN b ON a.id = b.id;

-- Sort merge join hint
SELECT /*+ SORT_MERGE(a, b) */ * FROM a JOIN b ON a.id = b.id;

-- Coalesce hint (reduce partitions)
SELECT /*+ COALESCE(10) */ * FROM large_table;

-- Repartition hint
SELECT /*+ REPARTITION(100, id) */ * FROM large_table;
```

### 12.3 Spark-Specific Functions

```sql
-- EXPLODE: flatten array/map into rows
SELECT id, EXPLODE(tags) as tag FROM items;
-- If tags = ['a', 'b', 'c'], produces 3 rows

-- POSEXPLODE: explode with position index
SELECT id, pos, tag FROM items LATERAL VIEW POSEXPLODE(tags) t AS pos, tag;

-- COLLECT_LIST / COLLECT_SET: aggregate into array
SELECT dept, COLLECT_LIST(name) as names FROM employees GROUP BY dept;
SELECT dept, COLLECT_SET(name) as unique_names FROM employees GROUP BY dept;

-- TRANSFORM: apply expression to each array element
SELECT TRANSFORM(scores, x -> x * 1.1) as curved_scores FROM students;

-- FILTER: filter array elements
SELECT FILTER(scores, x -> x >= 60) as passing_scores FROM students;

-- AGGREGATE: fold/reduce an array
SELECT AGGREGATE(scores, 0, (acc, x) -> acc + x) as total FROM students;

-- SEQUENCE: generate array of values
SELECT SEQUENCE(1, 10) as nums;
SELECT SEQUENCE(DATE'2024-01-01', DATE'2024-01-31', INTERVAL 1 DAY) as dates;

-- STRUCT: create struct column
SELECT STRUCT(name, age, dept) as employee_info FROM employees;

-- MAP: create map from key-value pairs
SELECT MAP('name', name, 'dept', dept) as info FROM employees;
```

### 12.4 Temporary Views

```sql
-- Create a temporary view (session-scoped)
CREATE OR REPLACE TEMP VIEW active_users AS
SELECT * FROM users WHERE active = true;

-- Create a global temporary view (application-scoped)
CREATE OR REPLACE GLOBAL TEMP VIEW active_users AS
SELECT * FROM users WHERE active = true;
-- Access as: SELECT * FROM global_temp.active_users;
```

---

## 13. 50+ Practice Problems

### Easy

**P1: Second Highest Salary**
```sql
-- Find the second highest salary (return NULL if doesn't exist)
SELECT MAX(salary) as second_highest
FROM employees
WHERE salary < (SELECT MAX(salary) FROM employees);

-- Or with DENSE_RANK:
WITH ranked AS (
    SELECT salary, DENSE_RANK() OVER (ORDER BY salary DESC) as rnk
    FROM employees
)
SELECT DISTINCT salary FROM ranked WHERE rnk = 2;
```

**P2: Duplicate Emails**
```sql
SELECT email, COUNT(*) as cnt
FROM users
GROUP BY email
HAVING COUNT(*) > 1;
```

**P3: Employees Earning More Than Manager**
```sql
SELECT e.name as Employee
FROM employees e
JOIN employees m ON e.manager_id = m.id
WHERE e.salary > m.salary;
```

**P4: Customers Who Never Ordered**
```sql
SELECT c.name
FROM customers c
LEFT ANTI JOIN orders o ON c.id = o.customer_id;

-- Standard SQL:
SELECT c.name FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);
```

**P5: Rising Temperature**
```sql
-- Days where temperature is higher than the previous day
SELECT w1.id
FROM weather w1
JOIN weather w2 ON DATEDIFF(w1.record_date, w2.record_date) = 1
WHERE w1.temperature > w2.temperature;

-- With LAG:
SELECT id FROM (
    SELECT id, temperature,
           LAG(temperature) OVER (ORDER BY record_date) as prev_temp
    FROM weather
) t WHERE temperature > prev_temp;
```

### Medium

**P6: Department Top 3 Salaries**
```sql
WITH ranked AS (
    SELECT name, dept, salary,
           DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary DESC) as rnk
    FROM employees
)
SELECT dept, name, salary FROM ranked WHERE rnk <= 3;
```

**P7: Consecutive Numbers**
```sql
-- Find numbers that appear at least 3 times consecutively
SELECT DISTINCT l1.num as ConsecutiveNums
FROM logs l1
JOIN logs l2 ON l1.id = l2.id - 1
JOIN logs l3 ON l1.id = l3.id - 2
WHERE l1.num = l2.num AND l2.num = l3.num;

-- With LAG/LEAD:
SELECT DISTINCT num FROM (
    SELECT num,
           LAG(num) OVER (ORDER BY id) as prev_num,
           LEAD(num) OVER (ORDER BY id) as next_num
    FROM logs
) t WHERE num = prev_num AND num = next_num;
```

**P8: Year-over-Year Revenue Growth**
```sql
SELECT year, revenue,
       LAG(revenue) OVER (ORDER BY year) as prev_year_revenue,
       ROUND((revenue - LAG(revenue) OVER (ORDER BY year)) * 100.0 
             / LAG(revenue) OVER (ORDER BY year), 2) as yoy_growth
FROM annual_revenue;
```

**P9: Median Salary**
```sql
-- Approximate median using PERCENTILE_APPROX
SELECT dept, PERCENTILE_APPROX(salary, 0.5) as median_salary
FROM employees GROUP BY dept;

-- Exact median using window functions:
WITH ranked AS (
    SELECT dept, salary,
           ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) as rn,
           COUNT(*) OVER (PARTITION BY dept) as cnt
    FROM employees
)
SELECT dept, AVG(salary) as median
FROM ranked
WHERE rn IN (FLOOR((cnt + 1) / 2.0), CEIL((cnt + 1) / 2.0))
GROUP BY dept;
```

**P10: Cumulative Distinct Count**
```sql
-- Number of distinct users per day, cumulative
WITH daily AS (
    SELECT date, user_id,
           MIN(date) OVER (PARTITION BY user_id ORDER BY date) as first_date
    FROM events
)
SELECT date, COUNT(DISTINCT CASE WHEN first_date = date THEN user_id END) as new_users,
       SUM(COUNT(DISTINCT CASE WHEN first_date = date THEN user_id END)) 
           OVER (ORDER BY date) as cumulative_users
FROM daily GROUP BY date;
```

**P11: Detect Gaps in Sequence**
```sql
SELECT prev_id + 1 as gap_start, id - 1 as gap_end
FROM (
    SELECT id, LAG(id) OVER (ORDER BY id) as prev_id
    FROM records
) t
WHERE id - prev_id > 1;
```

**P12: Running Balance**
```sql
SELECT transaction_date, type, amount,
       SUM(CASE WHEN type = 'credit' THEN amount ELSE -amount END) 
           OVER (ORDER BY transaction_date) as running_balance
FROM transactions;
```

**P13: Products Purchased Together**
```sql
SELECT a.product_id as product_a, b.product_id as product_b, COUNT(*) as times_together
FROM order_items a
JOIN order_items b ON a.order_id = b.order_id AND a.product_id < b.product_id
GROUP BY a.product_id, b.product_id
ORDER BY times_together DESC
LIMIT 10;
```

### Hard

**P14: Median Using Window Functions (General)**
```sql
WITH ordered AS (
    SELECT value,
           ROW_NUMBER() OVER (ORDER BY value) as rn,
           COUNT(*) OVER () as total
    FROM numbers
)
SELECT AVG(value) as median
FROM ordered
WHERE rn BETWEEN total / 2.0 AND total / 2.0 + 1;
```

**P15: Trips and Users (Cancellation Rate)**
```sql
SELECT request_date,
       ROUND(SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 2) as cancel_rate
FROM trips
WHERE client_id NOT IN (SELECT user_id FROM banned_users)
  AND driver_id NOT IN (SELECT user_id FROM banned_users)
  AND request_date BETWEEN '2024-01-01' AND '2024-01-31'
GROUP BY request_date;
```

**P16: Department Budget Utilization**
```sql
WITH dept_spend AS (
    SELECT dept, SUM(salary) as total_salary
    FROM employees GROUP BY dept
),
dept_budget AS (
    SELECT dept, budget FROM department_budgets
)
SELECT d.dept, d.budget, s.total_salary,
       ROUND(s.total_salary * 100.0 / d.budget, 2) as utilization_pct,
       d.budget - s.total_salary as remaining
FROM dept_budget d
JOIN dept_spend s ON d.dept = s.dept
ORDER BY utilization_pct DESC;
```

**P17: Sessionization**
```sql
WITH gaps AS (
    SELECT user_id, event_time,
           CASE WHEN UNIX_TIMESTAMP(event_time) - 
                     UNIX_TIMESTAMP(LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time)) > 1800
                THEN 1 ELSE 0 END as new_session
    FROM clickstream
),
sessions AS (
    SELECT *, SUM(new_session) OVER (PARTITION BY user_id ORDER BY event_time) as session_id
    FROM gaps
)
SELECT user_id, session_id,
       MIN(event_time) as session_start, MAX(event_time) as session_end,
       COUNT(*) as clicks, 
       UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time)) as duration_sec
FROM sessions GROUP BY user_id, session_id;
```

**P18: SCD Type 2 Query (Current State)**
```sql
-- Find the current record for each entity in an SCD Type 2 table
SELECT * FROM dimension_table
WHERE is_current = true;

-- Or using effective dates:
SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY effective_date DESC) as rn
    FROM dimension_table
    WHERE effective_date <= CURRENT_DATE
) t WHERE rn = 1;
```

**P19: Funnel Analysis**
```sql
WITH step1 AS (
    SELECT DISTINCT user_id FROM events WHERE event_type = 'page_view'
),
step2 AS (
    SELECT DISTINCT user_id FROM events WHERE event_type = 'add_to_cart'
    AND user_id IN (SELECT user_id FROM step1)
),
step3 AS (
    SELECT DISTINCT user_id FROM events WHERE event_type = 'purchase'
    AND user_id IN (SELECT user_id FROM step2)
)
SELECT 
    (SELECT COUNT(*) FROM step1) as page_views,
    (SELECT COUNT(*) FROM step2) as add_to_cart,
    (SELECT COUNT(*) FROM step3) as purchases,
    ROUND((SELECT COUNT(*) FROM step2) * 100.0 / (SELECT COUNT(*) FROM step1), 2) as view_to_cart_pct,
    ROUND((SELECT COUNT(*) FROM step3) * 100.0 / (SELECT COUNT(*) FROM step2), 2) as cart_to_purchase_pct;
```

**P20: Retention Cohort Analysis**
```sql
WITH first_activity AS (
    SELECT user_id, DATE_TRUNC('month', MIN(event_date)) as cohort_month
    FROM events GROUP BY user_id
),
monthly_activity AS (
    SELECT DISTINCT user_id, DATE_TRUNC('month', event_date) as activity_month
    FROM events
)
SELECT f.cohort_month,
       MONTHS_BETWEEN(m.activity_month, f.cohort_month) as months_since_join,
       COUNT(DISTINCT m.user_id) as retained_users,
       COUNT(DISTINCT m.user_id) * 100.0 / 
           COUNT(DISTINCT CASE WHEN m.activity_month = f.cohort_month THEN m.user_id END) 
           OVER (PARTITION BY f.cohort_month) as retention_pct
FROM first_activity f
JOIN monthly_activity m ON f.user_id = m.user_id
GROUP BY f.cohort_month, MONTHS_BETWEEN(m.activity_month, f.cohort_month)
ORDER BY f.cohort_month, months_since_join;
```

**P21: Time-Weighted Average**
```sql
WITH intervals AS (
    SELECT sensor_id, value, timestamp,
           LEAD(timestamp) OVER (PARTITION BY sensor_id ORDER BY timestamp) as next_ts,
           UNIX_TIMESTAMP(LEAD(timestamp) OVER (PARTITION BY sensor_id ORDER BY timestamp)) -
           UNIX_TIMESTAMP(timestamp) as duration_sec
    FROM sensor_readings
)
SELECT sensor_id,
       SUM(value * duration_sec) / SUM(duration_sec) as time_weighted_avg
FROM intervals
WHERE duration_sec IS NOT NULL
GROUP BY sensor_id;
```

**P22: Finding Overlapping Intervals**
```sql
SELECT a.id as meeting_a, b.id as meeting_b
FROM meetings a
JOIN meetings b ON a.id < b.id
    AND a.room = b.room
    AND a.start_time < b.end_time
    AND b.start_time < a.end_time;
```

**P23: Dense Ranking with Ties and Percentiles**
```sql
SELECT name, score,
       DENSE_RANK() OVER (ORDER BY score DESC) as rnk,
       PERCENT_RANK() OVER (ORDER BY score) as percentile,
       CUME_DIST() OVER (ORDER BY score) as cumulative_dist,
       NTILE(10) OVER (ORDER BY score) as decile
FROM exam_results;
```

**P24: Recursive Hierarchy Flattening**
```sql
WITH RECURSIVE hierarchy AS (
    SELECT id, name, parent_id, name as path, 0 as depth
    FROM categories WHERE parent_id IS NULL
    
    UNION ALL
    
    SELECT c.id, c.name, c.parent_id, 
           CONCAT(h.path, ' > ', c.name), h.depth + 1
    FROM categories c
    JOIN hierarchy h ON c.parent_id = h.id
)
SELECT * FROM hierarchy ORDER BY path;
```

**P25: Ratio Between Consecutive Rows**
```sql
SELECT date, metric_value,
       LAG(metric_value) OVER (ORDER BY date) as prev_value,
       ROUND(metric_value * 1.0 / NULLIF(LAG(metric_value) OVER (ORDER BY date), 0), 4) as ratio
FROM daily_metrics;
```

---

## 14. Quick-Reference Cheat Sheet

### Execution Order
```
FROM → WHERE → GROUP BY → HAVING → SELECT → DISTINCT → ORDER BY → LIMIT
```

### Join Quick Reference
| Type | Returns | NULL Handling |
|------|---------|--------------|
| INNER | Matching rows from both | NULLs never match |
| LEFT | All left + matching right | NULLs on right if no match |
| RIGHT | All right + matching left | NULLs on left if no match |
| FULL OUTER | All rows from both | NULLs where no match |
| CROSS | Cartesian product | N/A |
| SEMI | Left rows with match in right | No right columns |
| ANTI | Left rows without match | No right columns |

### Window Function Quick Reference
| Function | Purpose |
|----------|---------|
| ROW_NUMBER() | Unique sequential number (no ties) |
| RANK() | Same rank for ties, gaps after |
| DENSE_RANK() | Same rank for ties, no gaps |
| NTILE(n) | Divide into n buckets |
| LAG(col, n) | Value n rows before |
| LEAD(col, n) | Value n rows after |
| FIRST_VALUE(col) | First value in window |
| LAST_VALUE(col) | Last value (use UNBOUNDED FOLLOWING) |
| SUM/AVG/COUNT/MIN/MAX OVER() | Running or partitioned aggregates |

### NULL Behavior
| Context | Behavior |
|---------|----------|
| `=`, `!=`, `<`, `>` | UNKNOWN if either side is NULL |
| `IS NULL` / `IS NOT NULL` | Only way to test for NULL |
| JOIN ON `a = b` | NULLs don't match |
| GROUP BY | All NULLs in one group |
| DISTINCT | All NULLs considered equal |
| ORDER BY | NULLs first (ASC) or last (DESC) in Spark |
| COUNT(*) | Counts NULLs |
| COUNT(col) | Ignores NULLs |
| SUM, AVG, MIN, MAX | Ignore NULLs |

### Performance Tips
```
1. SELECT only needed columns (column pruning)
2. Filter early (WHERE before JOIN where possible)
3. Use UNION ALL over UNION
4. Avoid functions on partition/indexed columns in WHERE
5. Use broadcast hints for small tables
6. Prefer EXISTS over IN for large subqueries
7. Use LIMIT with ORDER BY (top-K is cheaper than full sort)
8. Collect statistics: ANALYZE TABLE ... COMPUTE STATISTICS
```
