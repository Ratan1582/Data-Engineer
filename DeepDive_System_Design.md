# System Design for Data Engineering -- A Complete Deep Dive

**From Requirements Gathering to Production-Scale Architecture**

---

## Table of Contents

1. [System Design Framework](#1-system-design-framework)
2. [Back-of-Envelope Estimation](#2-back-of-envelope-estimation)
3. [Design 1: Large-Scale IoT Ingestion Pipeline](#3-design-1-large-scale-iot-ingestion-pipeline)
4. [Design 2: Real-Time + Batch Analytics Platform](#4-design-2-real-time--batch-analytics-platform)
5. [Design 3: Data Lake / Lakehouse Architecture](#5-design-3-data-lake--lakehouse-architecture)
6. [Design 4: KPI Dashboard System](#6-design-4-kpi-dashboard-system)
7. [Design 5: SLA and Data Quality Monitoring](#7-design-5-sla-and-data-quality-monitoring)
8. [Design 6: Event-Driven Data Pipeline](#8-design-6-event-driven-data-pipeline)
9. [Scaling Patterns](#9-scaling-patterns)
10. [Message Brokers -- Kafka Deep Dive](#10-message-brokers----kafka-deep-dive)
11. [Orchestration at Scale](#11-orchestration-at-scale)
12. [Cost Optimization](#12-cost-optimization)
13. [Monitoring and Observability](#13-monitoring-and-observability)
14. [Trade-Off Analysis Framework](#14-trade-off-analysis-framework)
15. [Quick-Reference Cheat Sheet](#15-quick-reference-cheat-sheet)

---

## 1. System Design Framework

### 1.1 The Interview Framework

Use this structured approach for any data engineering system design interview:

```
Step 1: REQUIREMENTS (5 minutes)
  - Functional: What does the system do? What data flows?
  - Non-functional: Scale, latency, availability, consistency, cost
  - Clarify ambiguities: ask questions!

Step 2: ESTIMATION (3 minutes)
  - Data volume (GB/day, records/sec)
  - Storage requirements (1 year, 5 years)
  - Throughput (read/write QPS)
  - Network bandwidth

Step 3: HIGH-LEVEL DESIGN (10 minutes)
  - Draw the architecture: sources → ingestion → processing → storage → serving
  - Name specific technologies for each component
  - Show data flow direction

Step 4: DEEP DIVE (15 minutes)
  - Interviewer picks components to explore
  - Discuss specific design decisions
  - Handle failure scenarios
  - Address scalability

Step 5: TRADE-OFFS (5 minutes)
  - What are the alternatives?
  - What are the weaknesses of your design?
  - How would you change it under different constraints?
```

### 1.2 Key Questions to Ask

Before designing, always clarify:
- What is the data volume? (GB/day? TB/day?)
- What is the latency requirement? (real-time? minutes? hours?)
- How many concurrent users/queries?
- What is the retention period? (days? years?)
- What is the read vs write ratio?
- What consistency model is needed? (strong? eventual?)
- What is the budget/cost constraint?
- What existing infrastructure is in place?

---

## 2. Back-of-Envelope Estimation

### 2.1 Data Volume

```
IoT scenario: 4 million smart meters, reading every 15 minutes
  Records/day: 4M × (24h × 4/h) = 384 million records/day
  Record size: ~200 bytes (JSON with device_id, timestamp, 5-6 metrics)
  Raw data/day: 384M × 200B = ~76 GB/day (raw JSON)
  Parquet (5x compression): ~15 GB/day
  Per year: ~5.5 TB/year (Parquet)
```

### 2.2 Throughput

```
Writes: 384M records / 86,400 seconds = ~4,400 records/second average
Peak (2x average): ~8,800 records/second
Reads: 100 analysts, 10 queries/hour, each scanning ~1 GB = 1 TB/day read
```

### 2.3 Storage

```
1 year of data: 5.5 TB (Parquet with Snappy)
5 years with tiering:
  Hot (current year): 5.5 TB on SSD ($0.10/GB/mo) = $550/mo
  Warm (years 2-3): 11 TB on S3-IA ($0.0125/GB/mo) = $137/mo
  Cold (years 4-5): 11 TB on Glacier ($0.004/GB/mo) = $44/mo
  Total: ~$731/mo for 27.5 TB over 5 years
```

### 2.4 Common Numbers to Know

| Resource | Capacity |
|----------|---------|
| HDD sequential read | 150 MB/s |
| SSD sequential read | 500 MB/s - 3 GB/s |
| Network (1 Gbps) | 125 MB/s |
| Network (10 Gbps) | 1.25 GB/s |
| Kafka throughput | 100K-1M messages/sec per broker |
| Spark task overhead | ~10ms per task |
| Parquet row group | 128 MB default |
| S3 PUT request | ~5ms latency |
| S3 GET request | ~50-100ms first byte |

---

## 3. Design 1: Large-Scale IoT Ingestion Pipeline

### 3.1 Requirements

- 4 million smart meters sending readings every 15 minutes
- Each reading: device_id, timestamp, 5-6 metrics (kWh, voltage, current, etc.)
- Data must be queryable within 1 hour of generation
- Historical data retention: 5 years
- Support ad-hoc queries and pre-computed KPIs

### 3.2 Architecture

```
Smart Meters (4M)
      │
      ▼
IoT Gateway / MQTT Broker
      │
      ▼
Kafka (message broker)
      │
      ├──────────────────────┐
      ▼                      ▼
Spark Streaming         Spark Batch (daily)
(Bronze: raw ingest)    (Silver → Gold)
      │                      │
      ▼                      ▼
Delta Lake               Delta Lake
(Bronze table)           (Silver/Gold tables)
      │                      │
      └──────────┬───────────┘
                 ▼
          SQL Warehouse / BI Tools
          (dashboards, ad-hoc queries)
```

### 3.3 Component Details

**Kafka**: 
- 3-5 brokers, 20+ partitions on the `meter-readings` topic
- Partition by `device_id` hash for ordering per device
- Retention: 7 days (enough for reprocessing)

**Spark Streaming (Bronze)**:
- Auto Loader or Kafka source
- Micro-batch every 5 minutes
- Writes raw data to Delta Lake Bronze table
- Schema: raw JSON + metadata (ingestion_time, source, batch_id)

**Spark Batch (Silver/Gold)**:
- Daily job at 2 AM
- Silver: clean, validate, deduplicate, enrich with device metadata
- Gold: daily/monthly aggregations per device, region, vendor

**Delta Lake**:
- Bronze: partitioned by `ingestion_date`
- Silver: partitioned by `date`
- Gold: partitioned by `date`, Z-ORDERed by `device_id`

### 3.4 Scaling Considerations

- Kafka scales by adding partitions and brokers
- Spark scales by adding executors
- Delta Lake scales with cloud storage (virtually unlimited)
- Read scaling: SQL warehouses auto-scale based on query load

### 3.5 Failure Handling

- Kafka provides at-least-once delivery
- Spark checkpointing ensures exactly-once processing
- Delta Lake ACID ensures consistent writes
- Dead letter queue for malformed records

---

## 4. Design 2: Real-Time + Batch Analytics Platform

### 4.1 Lambda Architecture

Processes data through two parallel paths:

```
Data Sources
      │
      ▼
Message Broker (Kafka)
      │
      ├────────────────┐
      ▼                ▼
Speed Layer         Batch Layer
(Spark Streaming)   (Spark Batch)
(low latency,       (high accuracy,
 approximate)        complete)
      │                │
      ▼                ▼
Real-Time View     Batch View
(last few hours)   (historical)
      │                │
      └────────┬───────┘
               ▼
        Serving Layer
        (merge real-time + batch)
```

**Speed layer**: Processes data as it arrives (seconds to minutes latency). May produce approximate results (e.g., approximate counts, recent aggregations).

**Batch layer**: Reprocesses the same data in batch (hourly/daily). Produces complete, accurate results that overwrite the speed layer's approximations.

**Serving layer**: Combines both views. For recent data, serve from speed layer. For older data, serve from batch layer.

### 4.2 Kappa Architecture

Simplification: only one processing path (streaming):

```
Data Sources → Kafka → Spark Streaming → Delta Lake → Serving
```

All processing is done through the streaming layer. For reprocessing, replay Kafka from the beginning (or a specific offset).

### 4.3 Lambda vs Kappa

| Aspect | Lambda | Kappa |
|--------|--------|-------|
| **Code maintenance** | Two codebases (batch + stream) | One codebase |
| **Accuracy** | High (batch corrects stream) | Depends on stream processing |
| **Complexity** | High (two paths to maintain) | Lower |
| **Reprocessing** | Re-run batch job | Replay from Kafka |
| **Latency** | Speed layer: seconds; Batch: hours | Seconds to minutes |

**Modern recommendation**: Kappa with Delta Lake. Structured Streaming writes to Delta Lake, which supports both streaming and batch queries. Backfill by replaying from Kafka or reprocessing from Bronze.

---

## 5. Design 3: Data Lake / Lakehouse Architecture

### 5.1 Architecture

```
Data Sources                    Lakehouse (Delta Lake)              Consumers
┌─────────┐                    ┌───────────────────┐              ┌──────────┐
│ RDBMS   │──CDC──┐            │ Bronze            │              │ BI Tools │
│ APIs    │──REST─┤            │ (raw, append-only)│              │ (Tableau)│
│ Files   │──S3───┤───Ingest──▶│         │         │──Query──────▶│          │
│ Kafka   │──Stream┤           │         ▼         │              │ Notebooks│
│ IoT     │──MQTT─┘            │ Silver            │              │          │
└─────────┘                    │ (cleaned, joined) │              │ ML       │
                               │         │         │              │ Pipelines│
                               │         ▼         │              └──────────┘
                               │ Gold              │
                               │ (aggregated, KPIs)│
                               └───────────────────┘
                                       │
                                Unity Catalog
                               (governance, lineage,
                                access control)
```

### 5.2 Governance Layer

**Unity Catalog**:
- Centralized access control (who can read what)
- Data lineage (which tables feed into which)
- Data classification (PII, sensitive, public)
- Audit logging (who accessed what, when)

**Data Quality**:
- DLT expectations at Silver layer
- Data quality dashboard tracking metrics over time
- Alert on quality regression

### 5.3 Cost Optimization

```
Storage tiering:
  Bronze (hot): Standard storage, 30-day retention for raw
  Silver (warm): Standard storage, 1-year retention
  Gold (hot): Standard storage, served to dashboards
  Archive (cold): Glacier/Cool, 5+ year retention

Compute optimization:
  Ingestion: serverless or spot instances
  Batch processing: job clusters (ephemeral)
  Ad-hoc queries: SQL warehouses (auto-scale)
  ML training: GPU clusters (on-demand)
```

---

## 6. Design 4: KPI Dashboard System

### 6.1 Requirements

- 10 KPIs computed daily (revenue, active users, conversion rate, etc.)
- Data volume: 500M events/day from 3 source systems
- Dashboard refresh: daily at 8 AM, some near-real-time
- 200 concurrent dashboard users
- Drill-down from company → region → store → product

### 6.2 Architecture

```
Source Systems
      │
      ▼
Spark ETL (daily at 3 AM)
      │
      ├── Bronze: raw events
      ├── Silver: cleaned, joined
      └── Gold: pre-aggregated KPIs
              │
              ├── gold.daily_kpis (company-level)
              ├── gold.regional_kpis (region-level)
              ├── gold.store_kpis (store-level)
              └── gold.product_kpis (product-level)
                      │
                      ▼
              SQL Warehouse
                      │
                      ▼
              BI Dashboard (Tableau/Power BI)
```

### 6.3 Pre-Aggregation Strategy

Compute KPIs at multiple granularities during batch processing:

```python
# Finest granularity (store × product × date)
kpi_detail = silver.groupBy("date", "region", "store", "product").agg(
    sum("revenue").alias("revenue"),
    countDistinct("user_id").alias("unique_users"),
    count("*").alias("event_count")
)

# Roll up to store level
kpi_store = kpi_detail.groupBy("date", "region", "store").agg(
    sum("revenue").alias("revenue"),
    sum("unique_users").alias("unique_users"),
    sum("event_count").alias("event_count")
)

# Roll up to region level
kpi_region = kpi_store.groupBy("date", "region").agg(...)

# Roll up to company level
kpi_company = kpi_region.groupBy("date").agg(...)
```

### 6.4 Near-Real-Time Component

For KPIs that need real-time updates (e.g., current-hour revenue):

```python
# Streaming aggregation updated every 5 minutes
streaming_kpis = spark.readStream.format("delta").load("/silver/events/") \
    .withWatermark("timestamp", "1 hour") \
    .groupBy(window("timestamp", "1 hour"), "region") \
    .agg(sum("revenue").alias("hourly_revenue"))

streaming_kpis.writeStream \
    .format("delta") \
    .outputMode("update") \
    .option("checkpointLocation", "/checkpoints/rt_kpis/") \
    .trigger(processingTime="5 minutes") \
    .start("/gold/realtime_kpis/")
```

---

## 7. Design 5: SLA and Data Quality Monitoring

### 7.1 Architecture

```
Data Pipelines                  Monitoring System              Alerting
┌───────────┐                  ┌────────────────┐             ┌──────────┐
│Pipeline A │──metrics──┐      │ SLA Tracker    │──breach────▶│ Slack    │
│Pipeline B │──metrics──┤      │ (freshness,    │             │ PagerDuty│
│Pipeline C │──metrics──┤─────▶│  completeness, │──report────▶│ Email    │
│ ...       │──metrics──┘      │  volume)       │             │          │
└───────────┘                  │                │             │Dashboard │
                               │ Quality Scorer │             └──────────┘
                               │ (null rates,   │
                               │  duplicates,   │
                               │  anomalies)    │
                               └────────────────┘
```

### 7.2 SLA Metrics

```python
sla_checks = {
    "bronze_events": {
        "freshness_max_hours": 1,       # data must be < 1 hour old
        "min_daily_records": 300_000_000, # expect 300M+ records/day
        "max_null_rate_device_id": 0.001  # < 0.1% null device IDs
    },
    "silver_events": {
        "freshness_max_hours": 2,
        "min_daily_records": 290_000_000,  # some records dropped in cleaning
        "max_duplicate_rate": 0.0001
    },
    "gold_daily_kpis": {
        "freshness_max_hours": 6,  # must be ready by 6 AM
        "expected_record_count": 365  # one per day per KPI
    }
}
```

### 7.3 Implementation

```python
def check_sla(table_path, sla_config, date):
    results = {}
    df = spark.read.format("delta").load(table_path).filter(col("date") == date)
    
    # Freshness
    latest_ts = df.agg(max("_ingestion_time")).collect()[0][0]
    hours_old = (datetime.now() - latest_ts).total_seconds() / 3600
    results["freshness_hours"] = hours_old
    results["freshness_ok"] = hours_old <= sla_config["freshness_max_hours"]
    
    # Volume
    count = df.count()
    results["record_count"] = count
    results["volume_ok"] = count >= sla_config.get("min_daily_records", 0)
    
    # Quality
    if "max_null_rate_device_id" in sla_config:
        null_rate = df.filter(col("device_id").isNull()).count() / count
        results["null_rate"] = null_rate
        results["quality_ok"] = null_rate <= sla_config["max_null_rate_device_id"]
    
    # Overall
    results["sla_met"] = all(v for k, v in results.items() if k.endswith("_ok"))
    
    return results
```

---

## 8. Design 6: Event-Driven Data Pipeline

### 8.1 Architecture

```
Source DB                  Event Bus              Processing
┌──────┐                  ┌────────┐             ┌───────────┐
│OLTP  │──CDC──▶          │ Kafka  │──consume──▶ │Spark      │
│MySQL │ (Debezium)       │        │             │Streaming  │
└──────┘                  │ Topics:│             └─────┬─────┘
                          │ orders │                   │
┌──────┐                  │ users  │              ┌────▼────┐
│API   │──events──▶       │ events │              │Delta    │
│Server│                  └────────┘              │Lake     │
└──────┘                                          └─────────┘
```

### 8.2 CDC with Debezium

```json
// Debezium CDC event from MySQL
{
    "op": "u",  // u=update, c=create, d=delete, r=read (snapshot)
    "before": {"id": 1, "name": "Alice", "city": "NYC"},
    "after": {"id": 1, "name": "Alice", "city": "LA"},
    "source": {"db": "prod", "table": "customers", "ts_ms": 1705312800000},
    "ts_ms": 1705312800100
}
```

### 8.3 Processing CDC Events

```python
# Read CDC events from Kafka
cdc_stream = spark.readStream \
    .format("kafka") \
    .option("subscribe", "dbserver.prod.customers") \
    .load()

# Parse CDC events
parsed = cdc_stream.select(
    from_json(col("value").cast("string"), cdc_schema).alias("event")
).select("event.*")

# Apply to Delta Lake target
def process_cdc_batch(batch_df, batch_id):
    target = DeltaTable.forPath(spark, "/silver/customers/")
    
    # Get latest event per key (in case of multiple changes in one batch)
    latest = batch_df.withColumn("rn", row_number().over(
        Window.partitionBy("after.id").orderBy(col("ts_ms").desc())
    )).filter(col("rn") == 1)
    
    target.alias("t").merge(
        latest.alias("s"),
        "t.id = s.after.id"
    ).whenMatchedUpdate(
        condition="s.op = 'u'",
        set={"name": "s.after.name", "city": "s.after.city"}
    ).whenMatchedDelete(
        condition="s.op = 'd'"
    ).whenNotMatchedInsert(
        condition="s.op = 'c'",
        values={"id": "s.after.id", "name": "s.after.name", "city": "s.after.city"}
    ).execute()

parsed.writeStream.foreachBatch(process_cdc_batch) \
    .option("checkpointLocation", "/checkpoints/cdc_customers/") \
    .trigger(processingTime="1 minute") \
    .start()
```

---

## 9. Scaling Patterns

### 9.1 Horizontal vs Vertical Scaling

**Vertical scaling** (scale up): Add more CPU/RAM to a single machine.
- Pros: Simple, no distributed coordination
- Cons: Hardware limits, single point of failure
- Use for: Driver node, small-scale processing

**Horizontal scaling** (scale out): Add more machines.
- Pros: Near-unlimited scale, fault tolerance
- Cons: Distributed complexity, network overhead
- Use for: Executors, Kafka brokers, storage nodes

### 9.2 Partitioning Strategies

| Strategy | How | Use When |
|----------|-----|----------|
| **Hash partitioning** | `hash(key) % N` | Even distribution, key-based lookups |
| **Range partitioning** | Key ranges assigned to partitions | Sorted access, range queries |
| **Time-based partitioning** | One partition per time window | Time-series data, lifecycle management |
| **Composite partitioning** | Combine hash + range or time | High cardinality with temporal access |

### 9.3 Replication

```
Replication factor: 3 (standard for production)

Write path: Client → Leader → Follower 1 → Follower 2
Read path: Client → any replica (eventual consistency) or Leader (strong consistency)

Trade-off: Higher replication = more durability, more storage cost, slower writes
```

---

## 10. Message Brokers -- Kafka Deep Dive

### 10.1 Architecture

```
Producers                 Kafka Cluster                    Consumers
┌─────┐                 ┌──────────────┐                 ┌─────────┐
│App 1│──produce──▶     │ Broker 1     │                 │Consumer │
│App 2│──produce──▶     │ Broker 2     │──consume──▶     │Group A  │
│App 3│──produce──▶     │ Broker 3     │──consume──▶     │Consumer │
└─────┘                 └──────────────┘                 │Group B  │
                              │                          └─────────┘
                        ZooKeeper / KRaft
                        (metadata, leader election)
```

### 10.2 Topics and Partitions

```
Topic: "meter-readings" (20 partitions)

Partition 0: [msg0, msg3, msg6, msg9, ...]  → Broker 1
Partition 1: [msg1, msg4, msg7, msg10, ...] → Broker 2
Partition 2: [msg2, msg5, msg8, msg11, ...] → Broker 3
...
Partition 19: [...]                          → Broker 1

Ordering: guaranteed WITHIN a partition, NOT across partitions
```

### 10.3 Consumer Groups

```
Consumer Group "spark-bronze":
  Consumer 1: reads Partition 0, 1, 2, 3, 4
  Consumer 2: reads Partition 5, 6, 7, 8, 9
  Consumer 3: reads Partition 10, 11, 12, 13, 14
  Consumer 4: reads Partition 15, 16, 17, 18, 19

Each partition is assigned to exactly ONE consumer in a group.
Multiple consumer groups can independently read the same topic.
```

### 10.4 Key Concepts

| Concept | Description |
|---------|-----------|
| **Offset** | Position of a message within a partition (monotonically increasing) |
| **Retention** | How long messages are kept (time or size based) |
| **Compaction** | Keep only the latest value per key (for changelogs) |
| **Replication** | Each partition has a leader and N-1 followers |
| **ISR** | In-Sync Replicas: followers that are caught up with the leader |
| **ACKs** | `acks=0` (fire-and-forget), `acks=1` (leader ACK), `acks=all` (all ISR ACK) |

### 10.5 Kafka Configuration for Data Engineering

```
# Producer
acks=all                            # wait for all replicas (durability)
compression.type=snappy             # compress messages
batch.size=65536                    # batch messages for throughput
linger.ms=10                        # wait 10ms to batch more messages

# Consumer (Spark)
auto.offset.reset=earliest          # start from beginning if no offset
enable.auto.commit=false            # Spark manages offsets via checkpoint
max.poll.records=500                # records per poll
```

---

## 11. Orchestration at Scale

### 11.1 Airflow Architecture

```
┌───────────────┐     ┌──────────────┐     ┌──────────────┐
│  Web Server   │     │  Scheduler   │     │  Workers      │
│  (UI + API)   │     │  (trigger    │     │  (execute     │
│               │     │   tasks)     │     │   tasks)      │
└───────┬───────┘     └──────┬───────┘     └──────┬───────┘
        │                    │                    │
        └────────────────────┼────────────────────┘
                             │
                    ┌────────▼────────┐
                    │  Metadata DB    │
                    │  (PostgreSQL)   │
                    │  DAG state,     │
                    │  task history   │
                    └─────────────────┘
```

### 11.2 Airflow vs Databricks Workflows vs Dagster

| Feature | Airflow | Databricks Workflows | Dagster |
|---------|---------|---------------------|---------|
| **Hosting** | Self-hosted or managed (MWAA, Astronomer) | Fully managed | Self-hosted or Dagster Cloud |
| **Language** | Python | JSON/YAML + UI | Python |
| **Best for** | General orchestration | Databricks-native pipelines | Data-aware orchestration |
| **Learning curve** | Medium | Low | Medium |
| **Monitoring** | Web UI + logs | Built-in | Asset-centric UI |
| **Integration** | Vast operator ecosystem | Databricks only | Growing ecosystem |

### 11.3 Retry and Failure Handling

```python
# Airflow retry configuration
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": True,
    "email": ["oncall@company.com"]
}
```

---

## 12. Cost Optimization

### 12.1 Compute Optimization

| Strategy | Savings | Risk |
|----------|---------|------|
| **Spot/preemptible instances** | 60-90% | Task retry on interruption |
| **Auto-scaling** | Variable | Cold start latency |
| **Right-sizing** | 20-40% | Under-provisioning risk |
| **Serverless** | Variable | Cold start, less control |
| **Job clusters over interactive** | 40-60% | No interactive access |

### 12.2 Storage Optimization

| Strategy | Savings |
|----------|---------|
| **Columnar format (Parquet)** | 5-10x vs CSV |
| **Compression (Zstd over Snappy)** | 20-40% more |
| **Storage tiering (hot/warm/cold)** | 50-80% on old data |
| **VACUUM old Delta versions** | Reclaim 20-50% |
| **OPTIMIZE (compact small files)** | Faster reads, less metadata |
| **Column pruning in queries** | Proportional to columns skipped |

### 12.3 Processing Optimization

| Strategy | Impact |
|----------|--------|
| **Incremental processing** | Process 1% instead of 100% of data |
| **Partition pruning** | Read 1 partition instead of 365 |
| **Broadcast joins** | Eliminate shuffle for small tables |
| **Caching reused DataFrames** | Avoid re-reading from storage |
| **Early filtering** | Reduce data before expensive operations |

---

## 13. Monitoring and Observability

### 13.1 The Three Pillars

| Pillar | What | Tools |
|--------|------|-------|
| **Metrics** | Quantitative measurements over time | Prometheus, Grafana, CloudWatch |
| **Logs** | Discrete events with context | ELK Stack, Splunk, CloudWatch Logs |
| **Traces** | End-to-end request flow | Jaeger, Zipkin, X-Ray |

### 13.2 Key Metrics for Data Pipelines

```
Pipeline Health:
  - Pipeline success/failure rate
  - Pipeline duration (P50, P95, P99)
  - Records processed per run
  - Error/DLQ record count

Data Health:
  - Table freshness (time since last update)
  - Row count trends (daily, weekly)
  - Null rate per column
  - Schema change events

Infrastructure:
  - Cluster utilization (CPU, memory, disk)
  - Executor GC time
  - Shuffle spill
  - Task failure rate
```

### 13.3 Dashboard Design

```
Level 1: Executive dashboard (1 page)
  - All pipelines: green/yellow/red status
  - Key KPIs: data freshness, error rate, processing time
  
Level 2: Pipeline dashboard (per pipeline)
  - Duration trend over 30 days
  - Record count trend
  - Error breakdown
  - Resource utilization
  
Level 3: Debug dashboard (per run)
  - Spark UI metrics
  - Stage-level breakdown
  - Task distribution (skew detection)
  - Log links
```

---

## 14. Trade-Off Analysis Framework

### 14.1 Common Trade-Offs

| Trade-Off | Option A | Option B |
|-----------|----------|----------|
| **Latency vs Throughput** | Streaming (low latency, complex) | Batch (high throughput, simpler) |
| **Consistency vs Availability** | Strong consistency (slower writes) | Eventual consistency (faster, risk of stale reads) |
| **Cost vs Performance** | Fewer resources (slower) | More resources (faster, expensive) |
| **Simplicity vs Flexibility** | Star schema (simple queries) | Data vault (flexible, complex) |
| **Freshness vs Accuracy** | Real-time approximate | Batch exact |
| **Storage vs Compute** | Pre-aggregate (more storage) | Compute on-the-fly (more CPU) |

### 14.2 Decision Framework

```
For each component, ask:
1. What is the MOST IMPORTANT constraint? (latency? cost? simplicity?)
2. What can we SACRIFICE? (some latency? storage cost?)
3. What is the SIMPLEST solution that meets requirements?
4. How will this SCALE in 2-3 years?
5. What is the MIGRATION path if requirements change?
```

### 14.3 Answering Interviewer Follow-Ups

Common follow-ups and how to handle them:

**"What if the data volume grows 10x?"**
- Horizontal scaling: more Kafka partitions, more Spark executors
- Storage tiering: move older data to cheaper storage
- Query optimization: more aggressive pre-aggregation in Gold layer

**"What if you need real-time instead of batch?"**
- Add Spark Structured Streaming for the speed layer
- Use Delta Lake for unified batch+streaming reads
- Consider Kappa architecture if batch can be eliminated

**"What if a single node fails?"**
- Kafka: replicated partitions, leader election
- Spark: task retry on different executor, speculative execution
- Delta Lake: ACID transactions prevent partial writes
- Airflow: automatic task retry with exponential backoff

**"How would you handle data skew?"**
- Identify skewed keys via Spark UI
- Salting for join skew
- AQE for automatic skew handling
- Separate processing for known hot keys

---

## 15. Quick-Reference Cheat Sheet

### Architecture Patterns

```
Simple batch pipeline: Sources → Spark ETL → Delta Lake → BI
Streaming pipeline:    Sources → Kafka → Spark Streaming → Delta Lake → BI
Lambda:                Sources → Kafka → Speed layer + Batch layer → Serving
Kappa:                 Sources → Kafka → Streaming → Delta Lake
Lakehouse:             Sources → Bronze → Silver → Gold → BI/ML
CDC pipeline:          Source DB → Debezium → Kafka → Spark → Delta Lake MERGE
```

### Technology Selection

| Component | Options | Recommendation |
|-----------|---------|---------------|
| **Message broker** | Kafka, Kinesis, Pub/Sub | Kafka (most flexible) |
| **Batch processing** | Spark, Hive, Presto | Spark (most versatile) |
| **Stream processing** | Spark Streaming, Flink, Kafka Streams | Spark Streaming (unified with batch) |
| **Storage** | Delta Lake, Iceberg, Hudi, Parquet | Delta Lake (Databricks) or Iceberg (multi-engine) |
| **Orchestration** | Airflow, Databricks Workflows, Dagster | Airflow (general) or Workflows (Databricks-native) |
| **Serving** | SQL warehouse, Presto, Trino | SQL warehouse (Databricks) or Trino (open source) |

### Interview Checklist

```
□ Gathered requirements (volume, latency, retention, users)
□ Back-of-envelope estimation (storage, throughput, cost)
□ Drew high-level architecture (sources → processing → storage → serving)
□ Named specific technologies for each component
□ Discussed failure handling (retry, DLQ, ACID)
□ Addressed scalability (horizontal, partitioning, caching)
□ Discussed monitoring (metrics, alerting, SLA)
□ Acknowledged trade-offs (latency vs cost, simplicity vs flexibility)
□ Proposed cost optimization strategies
```
