# Data Modeling for Large-Scale Systems -- A Complete Deep Dive

**From Normalization Theory to Lakehouse Architecture**

---

## Table of Contents

1. [Why Data Modeling Matters](#1-why-data-modeling-matters)
2. [Normalization](#2-normalization)
3. [Denormalization](#3-denormalization)
4. [Star Schema](#4-star-schema)
5. [Snowflake Schema](#5-snowflake-schema)
6. [Data Vault](#6-data-vault)
7. [Medallion Architecture](#7-medallion-architecture)
8. [Partition Key Design](#8-partition-key-design)
9. [Slowly Changing Dimensions](#9-slowly-changing-dimensions)
10. [Kimball vs Inmon](#10-kimball-vs-inmon)
11. [Data Modeling for NoSQL](#11-data-modeling-for-nosql)
12. [Wide vs Narrow Tables](#12-wide-vs-narrow-tables)
13. [Interview Walkthrough: Model from Scratch](#13-interview-walkthrough-model-from-scratch)
14. [Quick-Reference Cheat Sheet](#14-quick-reference-cheat-sheet)

---

## 1. Why Data Modeling Matters

### 1.1 The Impact of Good Modeling

A well-designed data model:
- **Query performance**: Proper partitioning and indexing reduces scan time from hours to seconds
- **Storage efficiency**: Normalized structures avoid data duplication; denormalized structures avoid expensive joins
- **Maintainability**: Clear relationships make the data self-documenting
- **Scalability**: Partition strategies determine how the system handles growth
- **Data quality**: Constraints and relationships enforce business rules

### 1.2 The Cost of Bad Modeling

- Queries that join 15 tables to answer a simple question
- Duplicate data that becomes inconsistent across tables
- Partitions with extreme skew (one partition has 99% of the data)
- Schema changes that break dozens of downstream pipelines
- Analysts who can't find or understand the data they need

---

## 2. Normalization

### 2.1 What Is Normalization

Normalization organizes data to **reduce redundancy** and **prevent anomalies** (insert, update, delete anomalies). Each normal form adds a stricter rule.

### 2.2 First Normal Form (1NF)

**Rule**: All columns must contain atomic (indivisible) values. No repeating groups.

```
VIOLATES 1NF:
| student | courses          |
|---------|-----------------|
| Alice   | Math, Physics   |  ← multi-valued
| Bob     | CS              |

SATISFIES 1NF:
| student | course   |
|---------|----------|
| Alice   | Math     |
| Alice   | Physics  |
| Bob     | CS       |
```

### 2.3 Second Normal Form (2NF)

**Rule**: Must be 1NF + every non-key column must depend on the **entire** primary key (not just part of it).

```
VIOLATES 2NF (composite key: student_id + course_id):
| student_id | course_id | student_name | grade |
|-----------|----------|-------------|-------|
| 1         | CS101    | Alice       | A     |
| 1         | PH201    | Alice       | B     |

student_name depends only on student_id (partial dependency)

SATISFIES 2NF: Split into two tables:
Students: | student_id | student_name |
Grades:   | student_id | course_id | grade |
```

### 2.4 Third Normal Form (3NF)

**Rule**: Must be 2NF + no **transitive dependencies** (non-key column depending on another non-key column).

```
VIOLATES 3NF:
| employee_id | department_id | department_name |

department_name depends on department_id, not directly on employee_id

SATISFIES 3NF:
Employees:   | employee_id | department_id |
Departments: | department_id | department_name |
```

### 2.5 BCNF (Boyce-Codd Normal Form)

**Rule**: For every functional dependency X → Y, X must be a superkey.

A stricter version of 3NF that handles cases where 3NF doesn't fully eliminate redundancy.

### 2.6 When to Stop Normalizing

| Normal Form | Eliminates | Practical Use |
|------------|-----------|--------------|
| 1NF | Repeating groups | Always apply |
| 2NF | Partial dependencies | OLTP systems |
| 3NF | Transitive dependencies | OLTP systems (most stop here) |
| BCNF | All redundancy from FDs | Theoretical, rarely needed |
| 4NF | Multi-valued dependencies | Very rare |
| 5NF | Join dependencies | Almost never |

**For analytics**: 3NF is the maximum normalization level. Beyond that, the join cost outweighs the storage savings. Most analytical models are **denormalized** (star schema).

---

## 3. Denormalization

### 3.1 What Is Denormalization

Denormalization intentionally adds redundancy to improve read performance:

```
Normalized (3NF):
Orders:      | order_id | customer_id | product_id | quantity |
Customers:   | customer_id | name | city |
Products:    | product_id | name | price |

Query: SELECT c.name, p.name, o.quantity * p.price
       FROM orders o JOIN customers c ... JOIN products p ...
       3 tables, 2 joins

Denormalized:
Orders_flat: | order_id | customer_name | customer_city | product_name | price | quantity | total |

Query: SELECT customer_name, product_name, total FROM orders_flat
       1 table, 0 joins
```

### 3.2 Denormalization Techniques

**Pre-join**: Combine related tables into a single wide table.
**Pre-aggregate**: Store aggregated values (totals, counts, averages).
**Redundant columns**: Copy frequently-accessed columns from related tables.
**Materialized views**: Precompute and store query results.

### 3.3 Trade-offs

| Aspect | Normalized | Denormalized |
|--------|-----------|-------------|
| **Read performance** | Slower (many joins) | Faster (fewer/no joins) |
| **Write performance** | Faster (update one place) | Slower (update many places) |
| **Storage** | Less (no redundancy) | More (duplicated data) |
| **Consistency** | Easy (single source of truth) | Hard (must update all copies) |
| **Flexibility** | High (compose queries) | Low (schema change = rewrite) |
| **Best for** | OLTP (transactional) | OLAP (analytical) |

---

## 4. Star Schema

### 4.1 Structure

The star schema is the foundation of dimensional modeling for analytics:

```
                    +------------------+
                    |  dim_customer    |
                    | customer_id (PK) |
                    | name             |
                    | city             |
                    | segment          |
                    +--------+---------+
                             |
+------------------+    +----v-----------+    +------------------+
|   dim_product    |    | fact_sales     |    |   dim_date       |
| product_id (PK)  |--->| sale_id (PK)   |<---| date_id (PK)     |
| name             |    | customer_id(FK)|    | date             |
| category         |    | product_id(FK) |    | day_of_week      |
| price            |    | date_id (FK)   |    | month            |
+------------------+    | quantity       |    | quarter          |
                        | amount         |    | year             |
                        | discount       |    | is_holiday       |
                        +----------------+    +------------------+
```

**Fact table**: The center table. Contains the **measurements** (metrics, quantities, amounts) and **foreign keys** to dimension tables. Each row is a business event (a sale, a reading, a click).

**Dimension tables**: Surround the fact table. Contain the **descriptive attributes** (who, what, when, where). Used for filtering, grouping, and labeling.

### 4.2 Fact Table Types

| Type | Contains | Example |
|------|---------|---------|
| **Transaction fact** | One row per event | Each individual sale |
| **Periodic snapshot** | One row per period | Daily inventory levels |
| **Accumulating snapshot** | One row per lifecycle | Order: created → shipped → delivered |

### 4.3 Dimension Table Concepts

**Surrogate key**: An auto-generated integer key (1, 2, 3, ...) instead of the natural business key. Why: natural keys can change, are sometimes composite, and may not be integer (slower joins).

**Degenerate dimension**: A dimension that lives in the fact table (no separate dim table). Example: `order_number` in fact_sales -- it's a dimension but has no other attributes worth a separate table.

**Junk dimension**: A dimension table that combines multiple low-cardinality flags/indicators into one table:

```
dim_flags: | flag_id | is_returned | is_gift | payment_method |
           | 1       | false       | false   | credit         |
           | 2       | false       | true    | credit         |
           | 3       | true        | false   | cash           |
```

Instead of 3 foreign keys in the fact table, you have 1.

**Role-playing dimension**: Same dimension table used in multiple roles. Example: `dim_date` might be referenced as `order_date`, `ship_date`, and `delivery_date` in the same fact table.

### 4.4 Star Schema Advantages

- **Simple queries**: Analysts understand the star shape intuitively
- **Fast aggregations**: Fact tables are pre-joined with dimension keys
- **BI tool friendly**: Most BI tools (Tableau, Power BI) are designed for star schemas
- **Predictable performance**: At most one level of joins (fact → dim)

---

## 5. Snowflake Schema

### 5.1 Structure

A snowflake schema normalizes dimension tables into sub-dimensions:

```
fact_sales → dim_product → dim_category → dim_department
                         → dim_brand
           → dim_customer → dim_city → dim_state → dim_country
           → dim_date
```

### 5.2 Star vs Snowflake

| Aspect | Star | Snowflake |
|--------|------|-----------|
| **Dimension normalization** | Denormalized (flat) | Normalized (multi-level) |
| **Number of tables** | Few (1 fact + N dims) | Many (dims split into sub-dims) |
| **Query complexity** | Simple (1-level joins) | Complex (multi-level joins) |
| **Query performance** | Faster (fewer joins) | Slower (more joins) |
| **Storage** | More (redundancy in dims) | Less (no redundancy) |
| **Maintenance** | Easier | Harder (more tables to manage) |

### 5.3 When to Use Snowflake

- When dimension tables are very large and have significant redundancy
- When storage cost is a primary concern
- When data integrity is critical (transactional systems)
- When the BI tool handles snowflake schemas well

**In practice**: Star schema is preferred for analytics. Snowflake schema is used when dimensions are genuinely large and the redundancy matters.

---

## 6. Data Vault

### 6.1 What Is Data Vault

Data Vault is a modeling technique designed for **enterprise data warehouses** that need to handle large volumes, frequent schema changes, and full audit history.

### 6.2 Components

**Hub**: Business key and metadata. The core entity.
```
hub_customer: | hub_customer_id | customer_bk (business key) | load_date | source |
```

**Link**: Relationships between hubs.
```
link_order: | link_order_id | hub_customer_id | hub_product_id | load_date | source |
```

**Satellite**: Descriptive attributes and history.
```
sat_customer: | hub_customer_id | load_date | name | city | segment | end_date |
```

### 6.3 Data Vault Advantages

- **Agile**: Adding new sources doesn't require redesigning existing structures
- **Auditable**: Full history of every change (who, when, what, from where)
- **Scalable**: Parallel loading (hubs, links, satellites load independently)
- **Source-independent**: Integrates data from many sources without transformation

### 6.4 When to Use

Data Vault is for **large enterprise environments** with:
- Many source systems
- Frequent schema changes
- Strict audit requirements
- Large data engineering teams

For smaller teams, star schema or medallion architecture is simpler and sufficient.

---

## 7. Medallion Architecture

### 7.1 Three-Layer Design

```
Bronze (Raw)  →  Silver (Cleaned)  →  Gold (Aggregated)
```

### 7.2 Schema Design per Layer

**Bronze**: Match the source schema. Add metadata columns.
```
| _ingestion_time | _source | _batch_id | raw_device_id | raw_timestamp | raw_value | ... |
```

**Silver**: Standardized, validated, enriched schema.
```
| device_id (STRING) | timestamp (TIMESTAMP) | value (DOUBLE) | device_type (STRING) | region (STRING) |
```

**Gold**: Business-specific, pre-aggregated.
```
| date (DATE) | region (STRING) | device_type (STRING) | avg_value | max_value | count | total_energy |
```

### 7.3 Relationships Between Layers

```
Bronze:  1 table per source system (append-only)
Silver:  1 table per business entity (cleaned, joined with reference data)
Gold:    1 table per business question / dashboard
```

### 7.4 Medallion vs Star Schema

They are complementary:
- Medallion is a **data flow architecture** (how data progresses through stages)
- Star schema is a **data model** (how tables relate to each other)

Typically, **Gold layer tables use star schema** or denormalized wide tables.

---

## 8. Partition Key Design

### 8.1 Why Partition

Partitioning splits a table into physical segments based on column values. Queries that filter on the partition column skip irrelevant segments entirely (partition pruning).

### 8.2 Choosing Partition Keys

**Good partition keys**:
- Frequently used in WHERE clauses (date, region, status)
- Low-to-medium cardinality (365 dates/year, 50 regions, 5 statuses)
- Relatively even distribution across values

**Bad partition keys**:
- High cardinality (user_id with 10M values → 10M directories)
- Skewed distribution (90% of data in one partition)
- Rarely used in filters (won't benefit from pruning)

### 8.3 Common Partition Strategies

**Time-based partitioning** (most common):
```python
df.write.partitionBy("year", "month", "day").parquet("/data/events/")
# Results in: /data/events/year=2024/month=01/day=15/part-xxx.parquet
```

Granularity decision:
| Data Volume per Day | Partition By | Reason |
|-------------------|-------------|--------|
| < 1 GB | month | Avoid too many tiny partitions |
| 1-100 GB | day | Standard daily partitioning |
| > 100 GB | day + hour | Finer granularity for large data |

**Entity-based partitioning**:
```python
df.write.partitionBy("region").parquet("/data/sales/")
```

**Composite partitioning**:
```python
df.write.partitionBy("date", "device_type").parquet("/data/readings/")
```

### 8.4 Avoiding Partition Skew

```python
# Check partition sizes
df.groupBy("date").count().orderBy(col("count").desc()).show(20)

# If one date has 100x more data than others, consider:
# 1. Add a second partition column to split large partitions
# 2. Use bucketing instead of partitioning
# 3. Combine small partitions (weekly instead of daily)
```

### 8.5 Over-Partitioning

```
Too many partitions → small file problem
/data/events/year=2024/month=01/day=01/hour=00/device_type=A/
                                                              → 1 file of 100 KB
                                                              
Fix: Reduce partition granularity or compact files
```

Rule of thumb: Each partition should have files totaling at least **128 MB**.

---

## 9. Slowly Changing Dimensions

### 9.1 Overview of SCD Types

| Type | Strategy | History | Complexity |
|------|---------|---------|-----------|
| 0 | Retain original | None (immutable) | Trivial |
| 1 | Overwrite | None | Low |
| 2 | Add new row | Full | High |
| 3 | Previous value column | One level | Medium |
| 4 | Separate history table | Full (in separate table) | Medium |
| 6 | Hybrid (1+2+3) | Full + current everywhere | High |

### 9.2 SCD Type 2: Full Implementation

The most commonly asked about in interviews:

```
Before update:
| sk | id  | name  | city | eff_from   | eff_to     | is_current |
| 1  | 101 | Alice | NYC  | 2020-01-01 | 9999-12-31 | true       |

After Alice moves to LA:
| sk | id  | name  | city | eff_from   | eff_to     | is_current |
| 1  | 101 | Alice | NYC  | 2020-01-01 | 2024-07-01 | false      |
| 2  | 101 | Alice | LA   | 2024-07-01 | 9999-12-31 | true       |
```

Implementation with Delta Lake MERGE:
```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "/dim/customers/")
source = spark.read.parquet("/staging/customer_updates/")

# Step 1: Find records that have actually changed
changes = source.alias("s").join(
    target.toDF().filter(col("is_current") == True).alias("t"),
    col("s.id") == col("t.id")
).filter(
    (col("s.city") != col("t.city")) | (col("s.name") != col("t.name"))
).select("s.*")

# Step 2: Union of records to insert (new current) and updates (expire old)
updates = changes.withColumn("merge_key", col("id"))
expire = changes.withColumn("merge_key", lit(None).cast("string"))  # won't match, forces insert

staged = updates.union(expire)

# Step 3: MERGE
target.alias("t").merge(
    staged.alias("s"),
    "t.id = s.merge_key AND t.is_current = true"
).whenMatchedUpdate(set={
    "is_current": lit(False),
    "eff_to": current_date()
}).whenNotMatchedInsert(values={
    "id": "s.id",
    "name": "s.name",
    "city": "s.city",
    "eff_from": current_date(),
    "eff_to": lit("9999-12-31").cast("date"),
    "is_current": lit(True)
}).execute()
```

### 9.3 Point-in-Time Queries with SCD Type 2

```sql
-- What was Alice's city on March 15, 2023?
SELECT city FROM dim_customers
WHERE id = 101
  AND eff_from <= '2023-03-15'
  AND eff_to > '2023-03-15';
```

---

## 10. Kimball vs Inmon

### 10.1 Kimball (Bottom-Up)

- Build **department-specific data marts** (star schemas) first
- Enterprise warehouse emerges by combining data marts via **conformed dimensions**
- Focus on business processes (sales, inventory, HR)
- Denormalized star schemas for fast queries
- Faster to deliver value (start with one department)

### 10.2 Inmon (Top-Down)

- Build an **enterprise-wide normalized data warehouse** (3NF) first
- Department data marts are derived from the warehouse
- Focus on the enterprise data model
- Normalized to avoid redundancy and ensure consistency
- Higher upfront investment, longer time to first value

### 10.3 Comparison

| Aspect | Kimball | Inmon |
|--------|---------|-------|
| **Approach** | Bottom-up | Top-down |
| **Model** | Star schema (denormalized) | 3NF (normalized) |
| **Time to value** | Fast (start with one mart) | Slow (build enterprise model first) |
| **Integration** | Via conformed dimensions | Via central warehouse |
| **Query performance** | Optimized (few joins) | Needs further denormalization |
| **Maintenance** | Each mart is independent | Central model, more complex |
| **Team size** | Small teams can start | Requires dedicated modeling team |

### 10.4 Modern Reality

Most modern data platforms use a **hybrid approach**:
- **Lakehouse / Medallion Architecture** for data flow
- **Star schemas in the Gold layer** (Kimball-influenced)
- **Normalized entities in the Silver layer** (Inmon-influenced)
- **Conformed dimensions** shared across Gold tables

---

## 11. Data Modeling for NoSQL

### 11.1 Cassandra Modeling

Cassandra's data model is **query-driven**: you design tables to serve specific queries.

```
Query: Get all readings for device X on date Y, ordered by timestamp

Table design:
CREATE TABLE readings_by_device (
    device_id text,
    date date,
    timestamp timestamp,
    value double,
    PRIMARY KEY ((device_id, date), timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

Partition Key: (device_id, date)  → determines which node stores the data
Clustering Key: timestamp         → determines sort order within the partition
```

**Key principles**:
- One table per query pattern (denormalize aggressively)
- Partition key determines data distribution (avoid hot partitions)
- Clustering key determines sort order
- No joins (all data for a query must be in one table)

### 11.2 MongoDB Document Modeling

**Embedded documents** (denormalized):
```json
{
    "order_id": "ORD001",
    "customer": {
        "name": "Alice",
        "email": "alice@example.com"
    },
    "items": [
        {"product": "Widget", "quantity": 2, "price": 10.0},
        {"product": "Gadget", "quantity": 1, "price": 25.0}
    ]
}
```

**Referenced documents** (normalized):
```json
// orders collection
{"order_id": "ORD001", "customer_id": "CUST001", "item_ids": ["ITEM001", "ITEM002"]}

// customers collection
{"_id": "CUST001", "name": "Alice", "email": "alice@example.com"}
```

**Decision**: Embed when data is frequently read together and the embedded array won't grow unbounded. Reference when data is shared across many documents or grows independently.

---

## 12. Wide vs Narrow Tables

### 12.1 Wide Table (Denormalized Flat)

```
| device_id | date | kwh_reading | voltage_avg | current_max | temp_min | temp_max | humidity | pressure | ... (200 columns) |
```

Pros: One scan for any combination of metrics. No joins.
Cons: Sparse (many NULLs if not all devices report all metrics). Schema changes add columns.

### 12.2 Narrow Table (Entity-Attribute-Value)

```
| device_id | date | metric_name | metric_value |
| D001      | 2024-01-15 | kwh_reading | 42.5 |
| D001      | 2024-01-15 | voltage_avg | 230.5 |
| D001      | 2024-01-15 | temp_min    | 15.2 |
```

Pros: Flexible (new metrics without schema change). No sparse NULLs.
Cons: Queries for multiple metrics require pivot. More rows = more storage.

### 12.3 When to Choose Each

| Criterion | Wide | Narrow |
|----------|------|--------|
| Fixed set of metrics | Yes | No |
| Often query multiple metrics together | Wide | N/A |
| Metrics vary by device type | No | Narrow |
| Schema changes frequently | No | Narrow |
| Reporting and dashboards | Wide | N/A |
| Storage of sparse data | No (wasteful) | Narrow (efficient) |

### 12.4 Hybrid Approach

```
Core wide table (common metrics all devices have):
| device_id | date | kwh | voltage | current |

Extension narrow table (device-specific metrics):
| device_id | date | metric_name | metric_value |
```

---

## 13. Interview Walkthrough: Model from Scratch

### 13.1 The Prompt

"Design a data model for an e-commerce analytics platform."

### 13.2 Step-by-Step Approach

**Step 1: Identify business processes**
- Customer places an order
- Customer browses products
- Product is shipped
- Customer returns a product

**Step 2: Identify fact tables (measurements)**
```
fact_orders:       order_id, customer_id, product_id, date_id, quantity, amount, discount
fact_page_views:   view_id, customer_id, product_id, date_id, session_id, duration_sec
fact_shipments:    shipment_id, order_id, date_id, carrier_id, cost, delivery_days
fact_returns:      return_id, order_id, date_id, reason_id, refund_amount
```

**Step 3: Identify dimension tables (descriptive)**
```
dim_customer:  customer_id, name, email, city, state, country, segment, join_date
dim_product:   product_id, name, category, subcategory, brand, price, weight
dim_date:      date_id, date, day_of_week, month, quarter, year, is_holiday, is_weekend
dim_carrier:   carrier_id, name, type (ground, air, express)
dim_reason:    reason_id, reason_text, category (defective, wrong_item, changed_mind)
```

**Step 4: Partition strategy**
```
fact_orders:     partitioned by date (daily)
fact_page_views: partitioned by date (daily), possibly bucketed by customer_id
fact_shipments:  partitioned by date (daily)
```

**Step 5: Physical storage**
```
Format: Delta Lake (Parquet + transaction log)
Compression: Snappy (default)
Location: /gold/ecommerce/fact_orders/, /gold/ecommerce/dim_customer/, etc.
```

---

## 14. Quick-Reference Cheat Sheet

### Model Selection

```
OLTP (transactional app)? → 3NF (normalized)
OLAP (analytics/reporting)? → Star schema (denormalized)
Enterprise with many sources? → Data Vault + Star schema Gold layer
Data lake / lakehouse? → Medallion (Bronze/Silver/Gold)
NoSQL (Cassandra)? → Query-driven denormalized tables
```

### Partition Strategy

```
Filter on date? → Partition by date (year/month/day)
Filter on category? → Partition by category
Both? → Composite partition (date + category)
High cardinality filter? → Bucket instead of partition
< 128 MB per partition? → Reduce granularity
```

### SCD Type Decision

```
Never changes? → Type 0
Don't need history? → Type 1 (overwrite)
Need full history? → Type 2 (new row)
Need current + previous only? → Type 3 (prev column)
```

### Fact Table Grain

The **grain** is the level of detail in each row:
```
1 row = 1 individual sale transaction (finest)
1 row = 1 daily summary per product (aggregated)
1 row = 1 monthly report per region (highly aggregated)

Rule: Start with the finest grain. Aggregate in Gold layer views.
```

### Naming Conventions

```
Fact tables:       fact_<business_process> (fact_orders, fact_page_views)
Dimension tables:  dim_<entity> (dim_customer, dim_product, dim_date)
Surrogate keys:    <table>_sk or <table>_id (customer_sk)
Business keys:     <entity>_bk (customer_bk)
Foreign keys:      <referenced_table>_id (customer_id in fact_orders)
```
