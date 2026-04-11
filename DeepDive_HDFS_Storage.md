# HDFS and Distributed Storage -- A Complete Deep Dive

**From Single-Disk Limits to Cloud-Scale Data Lakes**

---

## Table of Contents

1. [Why Distributed Storage](#1-why-distributed-storage)
2. [HDFS Architecture](#2-hdfs-architecture)
3. [Block Storage](#3-block-storage)
4. [Read and Write Paths](#4-read-and-write-paths)
5. [NameNode High Availability](#5-namenode-high-availability)
6. [HDFS Federation](#6-hdfs-federation)
7. [The Small File Problem](#7-the-small-file-problem)
8. [File Formats Deep Dive](#8-file-formats-deep-dive)
9. [Compression](#9-compression)
10. [Cloud Object Storage](#10-cloud-object-storage)
11. [Table Formats -- Delta Lake, Iceberg, Hudi](#11-table-formats----delta-lake-iceberg-hudi)
12. [Storage Lifecycle and Tiering](#12-storage-lifecycle-and-tiering)
13. [Spark and Storage Interaction](#13-spark-and-storage-interaction)
14. [Quick-Reference Cheat Sheet](#14-quick-reference-cheat-sheet)

---

## 1. Why Distributed Storage

### 1.1 The Single-Machine Problem

A single server has physical limits:
- **Disk capacity**: Even large servers top out at ~100 TB of raw disk
- **I/O bandwidth**: A single HDD reads at ~150 MB/s. A single SSD at ~500 MB/s to 3 GB/s
- **Failure risk**: If the disk dies, all data is lost

If you have 1 PB (1,000 TB) of data and a single HDD, just reading it sequentially would take:

```
1,000,000 GB / 0.15 GB/s = ~6.7 million seconds ≈ 77 days
```

With 1,000 disks in parallel, each holding 1 TB:

```
1,000 GB / 0.15 GB/s = ~6,667 seconds ≈ 1.8 hours
```

That is the core insight: **spread data across many machines, read in parallel**.

### 1.2 CAP Theorem (Simplified)

Any distributed storage system faces a fundamental trade-off:

- **Consistency**: Every read returns the most recent write
- **Availability**: Every request gets a response
- **Partition Tolerance**: The system works despite network splits between nodes

The CAP theorem says you can only guarantee **two of three**:
- **CP (Consistency + Partition Tolerance)**: HDFS, HBase -- reads may fail during partitions, but data is always correct
- **AP (Availability + Partition Tolerance)**: Cassandra (tunable), DynamoDB -- always responds, but may return stale data
- **CA (Consistency + Availability)**: Only possible in a single-node system (no partitions)

HDFS chose **CP**: it prioritizes data consistency and availability over write performance during failures.

### 1.3 Distributed Storage Approaches

| Approach | Examples | Model | Best For |
|----------|----------|-------|----------|
| **Distributed file system** | HDFS, GlusterFS | Files split into blocks across nodes | Large sequential reads, batch processing |
| **Object store** | S3, ADLS, GCS | Flat namespace of objects with keys | Cloud-native, any data type, HTTP API |
| **Distributed database** | Cassandra, HBase, MongoDB | Structured data across nodes | Low-latency reads/writes |
| **Table format** | Delta Lake, Iceberg, Hudi | ACID layer on top of file/object store | Analytics with transactions |

---

## 2. HDFS Architecture

### 2.1 Overview

HDFS (Hadoop Distributed File System) is a distributed file system designed for:
- **Commodity hardware**: designed to run on cheap machines that fail frequently
- **Large files**: optimized for files in the GB-TB range
- **Sequential access**: write once, read many times (batch analytics)
- **High throughput**: maximize data transfer rate, not minimize latency

### 2.2 Components

```
                    +-------------------+
                    |     NameNode      |
                    | (metadata master) |
                    |                   |
                    | - File/dir tree   |
                    | - Block locations |
                    | - Replication     |
                    +--------+----------+
                             |
              +--------------+--------------+
              |              |              |
    +---------v--+  +--------v---+  +------v------+
    | DataNode 1 |  | DataNode 2 |  | DataNode 3  |
    |            |  |            |  |             |
    | Block A-1  |  | Block A-2  |  | Block A-3  |
    | Block B-2  |  | Block B-3  |  | Block B-1  |
    | Block C-1  |  | Block C-3  |  | Block C-2  |
    +------------+  +------------+  +-------------+
```

**NameNode** (master):
- Stores the **metadata**: file/directory tree, permissions, block-to-DataNode mapping
- Does NOT store actual data -- only metadata
- All metadata lives **in RAM** for fast access
- Writes metadata changes to an **edit log** on disk (for durability)
- Periodically merges edit log into a **fsimage** (filesystem snapshot)

**DataNodes** (workers):
- Store actual data as **blocks** on their local disks
- Send **heartbeats** to NameNode every 3 seconds (I'm alive)
- Send **block reports** every 6 hours (here are all blocks I have)
- Execute read/write operations as directed by NameNode

**Secondary NameNode** (not a failover NameNode):
- Periodically merges the edit log with the fsimage to prevent the edit log from growing too large
- This is a maintenance process, **not a backup** for the NameNode

### 2.3 Metadata Storage

The NameNode stores:
```
/user/data/sales.csv  -->  {block_1: [DN1, DN3, DN5], block_2: [DN2, DN4, DN6]}
/user/data/logs.parquet --> {block_1: [DN1, DN2, DN3], block_2: [DN3, DN5, DN6], ...}
```

For each file: name, permissions, owner, modification time, replication factor, block size, list of blocks.
For each block: list of DataNodes that hold replicas.

This metadata is stored entirely in RAM. A NameNode typically needs:
- ~150 bytes per file
- ~150 bytes per block
- For 100 million files with average 2 blocks each: ~45 GB of RAM

This is why the **small file problem** is critical -- each tiny file consumes the same metadata overhead as a large file.

---

## 3. Block Storage

### 3.1 What Is a Block

HDFS splits every file into fixed-size **blocks** (default: 128 MB). A 512 MB file becomes 4 blocks:

```
File: /data/measurements.parquet (512 MB)

Block 1 (128 MB) --> stored on DataNode 1, 3, 5
Block 2 (128 MB) --> stored on DataNode 2, 4, 6
Block 3 (128 MB) --> stored on DataNode 1, 4, 6
Block 4 (128 MB) --> stored on DataNode 2, 3, 5
```

If a file is smaller than the block size (say, 1 MB), it occupies **only 1 MB of disk** (not 128 MB). The block size is a maximum, not a fixed allocation.

### 3.2 Why 128 MB Blocks?

The block size is a trade-off between:
- **Too small** (e.g., 4 KB like a local filesystem): millions of blocks per file, enormous NameNode memory usage, excessive seek overhead
- **Too large** (e.g., 1 GB): fewer parallel tasks (one task per block), slow recovery when a block is lost

128 MB is chosen because:
1. A typical HDD can transfer 128 MB in about 1 second (150 MB/s)
2. The seek time (~10 ms) is negligible compared to transfer time
3. This means >99% of the time is spent transferring data, not seeking -- maximizing throughput

For SSDs or high-throughput workloads, 256 MB or 512 MB blocks may be better.

### 3.3 Replication

Every block is replicated to multiple DataNodes (default replication factor: 3).

```
Block A: DN1 (primary), DN3 (replica), DN5 (replica)
```

**Rack-aware placement** (default policy):
1. First replica: on the same node as the writer (or a random node if the writer is not in the cluster)
2. Second replica: on a **different rack** than the first
3. Third replica: on a **different node in the same rack** as the second

```
Rack 1              Rack 2
+--------+          +--------+
| DN1    |          | DN3    |   <-- 2nd replica
|[Rep 1] |          |[Rep 2] |
+--------+          +--------+
| DN2    |          | DN4    |   <-- 3rd replica
|        |          |[Rep 3] |
+--------+          +--------+
```

This balances:
- **Fault tolerance**: survives both single-node and single-rack failures
- **Write bandwidth**: only sends data across racks once (not twice)
- **Read locality**: there is usually a replica on the local rack

### 3.4 Pipeline Replication

When writing a block, the client does not send 3 copies. Instead, it sets up a **pipeline**:

```
Client --> DN1 --> DN3 --> DN5
           |       |       |
         write   write   write
           |       |       |
          ack <-- ack <-- ack
```

The client sends data to DN1. DN1 forwards to DN3. DN3 forwards to DN5. Acknowledgments flow back through the pipeline. This uses network bandwidth efficiently.

---

## 4. Read and Write Paths

### 4.1 Write Path

```
Client                   NameNode                DataNodes
  |                         |                       |
  |--- create("/data/f") -->|                       |
  |<-- OK, block locations--|                       |
  |                         |                       |
  |--- stream block 1 ----->|-->DN1-->DN3-->DN5      |
  |<-- ack ------------------|                       |
  |                         |                       |
  |--- stream block 2 ----->|-->DN2-->DN4-->DN6      |
  |<-- ack ------------------|                       |
  |                         |                       |
  |--- close() ------------>|                       |
  |<-- OK ------------------|                       |
```

Step by step:
1. Client calls `create()` on the NameNode
2. NameNode checks permissions, creates the file entry, returns the DataNode locations for the first block
3. Client streams data to the first DataNode, which pipelines to replicas
4. When a block is full (128 MB), client requests the next block's locations from NameNode
5. When done, client calls `close()`, NameNode finalizes the file

### 4.2 Read Path

```
Client                   NameNode                DataNodes
  |                         |                       |
  |--- open("/data/f") ---->|                       |
  |<-- block locations ------|                       |
  |                         |                       |
  |--- read block 1 ------->DN1 (closest replica)  |
  |<-- data ------------------|                      |
  |                         |                       |
  |--- read block 2 ------->DN4 (closest replica)  |
  |<-- data ------------------|                      |
```

1. Client asks NameNode for the file's block locations
2. NameNode returns the list of blocks and their DataNode locations, **sorted by proximity** to the client
3. Client reads each block from the closest DataNode
4. If a DataNode fails during read, the client automatically tries the next replica

### 4.3 Data Locality

HDFS leverages the fact that moving computation to the data is cheaper than moving data to the computation:

| Locality Level | Description | Network Cost |
|---------------|-------------|-------------|
| **Data-local** | Task runs on the same node as the data | Zero network |
| **Rack-local** | Task runs on a different node in the same rack | Intra-rack (~10 Gbps) |
| **Off-rack** | Task runs in a different rack | Cross-rack (~1-10 Gbps) |

Spark's scheduler tries to assign tasks to nodes that hold the data (data-local). This is why HDFS and Spark clusters are co-located.

---

## 5. NameNode High Availability

### 5.1 The Single Point of Failure Problem

The original HDFS had a single NameNode. If it crashed:
- No new reads or writes could start
- Running jobs would fail
- The cluster was effectively down

Recovery required restarting the NameNode and replaying the edit log, which could take **30+ minutes** for large clusters.

### 5.2 HA Architecture

HDFS HA uses **two NameNodes** in an active/standby configuration:

```
+------------------+     shared edit log      +------------------+
| Active NameNode  | <----(JournalNodes)----> | Standby NameNode |
| (handles all     |                          | (replays edits,  |
|  client requests)|                          |  ready to take   |
|                  |                          |  over instantly)  |
+------------------+                          +------------------+
        |                                              |
        +---------- ZooKeeper (failover) -------+------+
```

**JournalNodes**: A quorum of 3+ nodes that store the edit log. The active NameNode writes edits to a majority of JournalNodes. The standby NameNode reads and replays these edits, keeping its metadata in sync.

**ZooKeeper**: Monitors both NameNodes via a ZK Failover Controller (ZKFC) on each. If the active NameNode fails, ZooKeeper triggers automatic failover to the standby.

Failover takes **seconds** instead of minutes.

### 5.3 Fencing

When failover occurs, the system must ensure the old active NameNode does not continue serving requests (split-brain problem). **Fencing** mechanisms include:
- SSH fencing: sends a `kill` command to the old active's process
- Shell fencing: runs a custom script (e.g., power off the old node)
- STONITH (Shoot The Other Node In The Head): hardware-level fencing

---

## 6. HDFS Federation

### 6.1 The Metadata Scalability Problem

A single NameNode stores all metadata in RAM. For very large clusters (billions of files), this becomes a bottleneck:
- RAM is finite (even 256 GB NameNode can only handle ~1.5 billion files)
- All metadata operations go through one node (single-threaded for writes)

### 6.2 Federation Architecture

HDFS Federation uses **multiple independent NameNodes**, each managing a portion of the filesystem namespace:

```
NameNode 1: /user/team_a/...
NameNode 2: /user/team_b/...
NameNode 3: /data/warehouse/...
```

Each NameNode:
- Manages its own namespace (directory tree)
- Has its own block pool (the blocks belonging to its files)
- Is independent -- failure of one does not affect others

**ViewFS** provides a client-side **mount table** that merges the namespaces:
```
viewfs://cluster/user/team_a/  --> hdfs://nn1/user/team_a/
viewfs://cluster/user/team_b/  --> hdfs://nn2/user/team_b/
viewfs://cluster/data/          --> hdfs://nn3/data/
```

Clients see a single unified namespace.

---

## 7. The Small File Problem

### 7.1 What It Is

HDFS is designed for large files. Each file, regardless of size, requires:
- ~150 bytes of NameNode memory for the file entry
- ~150 bytes per block entry

A 10 KB file uses the same NameNode overhead as a 10 GB file. If you have **100 million small files** (e.g., one JSON file per IoT reading):
- NameNode memory: ~30 GB just for metadata
- Spark creates one task per file (or per block): 100 million tasks is unmanageable
- File open/close overhead dominates actual processing time

### 7.2 Why It Happens

Common causes:
- **IoT data**: one file per device per reading
- **Streaming ingestion**: small batches landing as individual files
- **Partitioning by high-cardinality keys**: `year/month/day/hour/device_id/` creates millions of directories with tiny files
- **Failed compaction**: incremental writes without periodic merging

### 7.3 Solutions

**1. Compaction / Merging**:
```python
df = spark.read.parquet("/data/small_files/")
df.coalesce(100).write.mode("overwrite").parquet("/data/compacted/")
```

**2. Hadoop Archive (HAR)**:
```bash
hadoop archive -archiveName data.har -p /data/small_files/ /data/archive/
```
Combines many small files into a single archive file. Reduces NameNode overhead but adds read latency (must seek within the archive).

**3. SequenceFile / MapFile**:
Store many small records as key-value pairs in a single large file. Common in Hadoop MapReduce but less used in Spark.

**4. Delta Lake / Auto Loader with compaction**:
```sql
OPTIMIZE delta.`/data/table/`
```
Delta Lake's `OPTIMIZE` command compacts small files into larger ones (target 1 GB per file).

**5. Auto Loader's file notification mode**:
Instead of listing millions of files, Auto Loader uses cloud event notifications to discover new files efficiently.

**6. Hive-style bucketing**:
Pre-partition data into a fixed number of buckets, ensuring each bucket file is a reasonable size.

---

## 8. File Formats Deep Dive

### 8.1 Row vs Column Formats

```
Row format (CSV, JSON, Avro):
Row 1: [id=1, name="Alice", age=30, city="NYC"]
Row 2: [id=2, name="Bob",   age=25, city="LA"]
Row 3: [id=3, name="Carol", age=35, city="NYC"]

Column format (Parquet, ORC):
Column "id":   [1, 2, 3]
Column "name": ["Alice", "Bob", "Carol"]
Column "age":  [30, 25, 35]
Column "city": ["NYC", "LA", "NYC"]
```

**Why columnar is better for analytics**:
- **Column pruning**: If your query only needs `name` and `age`, only those columns are read from disk. In row format, you read entire rows.
- **Better compression**: Values in a column are the same type and often similar (e.g., all city names). This compresses much better than mixed-type rows.
- **Vectorized processing**: CPUs process arrays of the same type much faster (cache-friendly, SIMD instructions).

### 8.2 Apache Parquet

Parquet is the dominant columnar format in the Spark/Hadoop ecosystem.

**Internal structure**:
```
Parquet File
├── Row Group 1 (typically 128 MB - 1 GB)
│   ├── Column Chunk: "id"
│   │   ├── Page 1 (compressed data + statistics)
│   │   ├── Page 2
│   │   └── ...
│   ├── Column Chunk: "name"
│   │   ├── Page 1
│   │   └── ...
│   ├── Column Chunk: "age"
│   └── Column Chunk: "city"
├── Row Group 2
│   ├── Column Chunk: "id"
│   ├── ...
└── Footer (schema + row group metadata + column statistics)
```

**Key concepts**:

| Component | What It Is | Size |
|-----------|-----------|------|
| **Row Group** | Horizontal partition of the file. Contains all columns for a range of rows. | 128 MB - 1 GB |
| **Column Chunk** | All data for one column within one row group. | Varies |
| **Page** | Unit of encoding/compression. Contains a batch of values for a column chunk. | ~1 MB (configurable) |
| **Footer** | File metadata: schema, row group locations, column statistics (min/max/null count per chunk). | Small |

**Why the footer matters**: The footer contains **column statistics** (min value, max value, null count) for each column chunk. When Spark reads a Parquet file with a filter like `WHERE age > 30`, it reads the footer first, checks the `age` column statistics for each row group, and **skips entire row groups** where `max(age) <= 30`. This is **predicate pushdown at the file level**.

**Encoding techniques**:
- **Dictionary encoding**: If a column has few distinct values (e.g., city names), store a dictionary `{0: "NYC", 1: "LA"}` and replace values with indices. Dramatically reduces size.
- **Run-length encoding (RLE)**: If consecutive values repeat (e.g., sorted data), store `(value, count)` instead of repeating the value.
- **Delta encoding**: Store the difference from the previous value. Effective for sorted integers or timestamps.
- **Bit packing**: Use the minimum number of bits per value (e.g., 3 bits for values 0-7 instead of 32 bits).

### 8.3 Apache ORC

ORC (Optimized Row Columnar) is similar to Parquet but originally developed for Hive:

```
ORC File
├── Stripe 1 (equivalent to Row Group, default 250 MB)
│   ├── Index Data (min/max per column, row positions)
│   ├── Row Data (column chunks)
│   └── Stripe Footer (encoding info)
├── Stripe 2
└── File Footer + Postscript (schema, stripe locations, statistics)
```

**Parquet vs ORC**:

| Feature | Parquet | ORC |
|---------|---------|-----|
| **Origin** | Twitter/Cloudera | Facebook/Hortonworks |
| **Ecosystem** | Spark, Arrow, Impala, Drill | Hive, Presto, Spark |
| **Nested types** | Excellent (Dremel model) | Good |
| **Compression** | Snappy, Gzip, Zstd, LZ4 | Zlib, Snappy, Zstd, LZ4 |
| **Predicate pushdown** | Row group level | Stripe level + bloom filters |
| **ACID support** | Via Delta Lake / Iceberg | Built into Hive ACID |
| **Default Spark format** | Yes | No (but fully supported) |

In practice, **Parquet has won** in the Spark ecosystem. Use Parquet unless you have a specific Hive-centric workflow that benefits from ORC.

### 8.4 Apache Avro

Avro is a **row-based** format with a schema stored alongside the data:

```
Avro File
├── File Header (schema in JSON)
├── Data Block 1 (sync marker + compressed rows)
├── Data Block 2
└── ...
```

**When to use Avro**:
- **Streaming/messaging**: Avro is the standard format for Kafka messages
- **Schema evolution**: Avro handles schema changes (add/remove/rename fields) gracefully
- **Write-heavy workloads**: Row format is faster for writing all columns of a record
- **Full-row reads**: When you always read entire records (not column subsets)

**Avro vs Parquet decision**:
```
Need to read specific columns? → Parquet
Need fast writes / full row reads? → Avro
Need schema evolution for streaming? → Avro
Need analytics / aggregations? → Parquet
```

### 8.5 CSV and JSON

**CSV**:
- No schema, no types (everything is a string)
- No compression built-in (but can gzip the file)
- No column pruning (must read entire row)
- Universal support, human-readable
- Use only for: data exchange with non-technical systems, small reference data

**JSON**:
- Self-describing (field names in every record)
- Supports nested structures
- Very verbose (field names repeated per record)
- No columnar access
- Use for: API responses, configuration, semi-structured data landing zone

**Size comparison** for the same 1 million row dataset:

| Format | Size (uncompressed) | Size (compressed) | Read Time (Spark) |
|--------|-------------------|-------------------|-------------------|
| CSV | 500 MB | 120 MB (gzip) | 15s |
| JSON | 800 MB | 150 MB (gzip) | 25s |
| Avro | 200 MB | 150 MB (snappy) | 8s |
| Parquet | 100 MB | 80 MB (snappy) | 3s |
| ORC | 90 MB | 75 MB (zlib) | 3s |

---

## 9. Compression

### 9.1 Why Compress

Compression reduces:
1. **Storage cost**: Store less data on disk/cloud
2. **I/O time**: Read less data from disk (often the bottleneck)
3. **Network transfer**: Move less data during shuffle or replication

The trade-off is **CPU time** for compression/decompression.

### 9.2 Splittable vs Non-Splittable

This is critical for distributed processing:

**Splittable**: The compressed file can be split into independent chunks. Each Spark task can decompress its portion independently.

**Non-splittable**: The entire file must be decompressed by a single task. Only one task can process the file, destroying parallelism.

| Codec | Splittable | Speed | Ratio | Use Case |
|-------|-----------|-------|-------|----------|
| **Snappy** | Yes (in Parquet/ORC) | Very fast | Low-medium | Default for Spark, real-time |
| **LZ4** | Yes (in Parquet/ORC) | Very fast | Low-medium | Alternative to Snappy |
| **Zstd** | Yes (in Parquet/ORC) | Fast | High | Best ratio-speed balance |
| **Gzip** | No (as raw .gz) | Slow | High | Archival, external exchange |
| **Bzip2** | Yes (at block boundaries) | Very slow | Very high | Maximum compression needed |
| **Lzo** | Yes (with index) | Fast | Medium | Legacy Hadoop workloads |

**Key insight**: When using columnar formats (Parquet, ORC), compression happens at the **column chunk/page level**, which is always splittable regardless of the codec. A Snappy-compressed Parquet file and a Gzip-compressed Parquet file are both splittable because Spark splits at the row group level.

The splittability issue applies to **raw compressed files** (e.g., `data.csv.gz`). A single gzipped CSV file will be processed by one Spark task.

### 9.3 Choosing a Compression Codec

```
Decision tree:

Is it a columnar format (Parquet/ORC)?
├── YES:
│   ├── Need best read speed? → Snappy (default) or LZ4
│   ├── Need best compression ratio? → Zstd
│   └── Need compatibility with old systems? → Gzip
└── NO (raw CSV/JSON/text):
    ├── Will Spark read it in parallel?
    │   ├── YES → Use Bzip2 (splittable) or convert to Parquet first
    │   └── NO (single file) → Gzip is fine
    └── Is it archival (rarely read)?
        └── YES → Gzip or Zstd
```

### 9.4 Compression in Spark

```python
# Writing with compression
df.write.option("compression", "snappy").parquet("/output/")  # default
df.write.option("compression", "zstd").parquet("/output/")
df.write.option("compression", "gzip").parquet("/output/")

# Reading: Spark auto-detects compression, no options needed
df = spark.read.parquet("/output/")

# For CSV:
df.write.option("compression", "gzip").csv("/output/")  # creates .csv.gz files
# WARNING: each .csv.gz file is NOT splittable -- ensure reasonable file sizes
```

---

## 10. Cloud Object Storage

### 10.1 Object Store vs File System

HDFS is a **distributed file system** -- it has directories, files, block-level operations, and strong consistency.

Cloud object stores (S3, ADLS, GCS) are **object stores** -- fundamentally different:

| Feature | HDFS (File System) | S3/ADLS/GCS (Object Store) |
|---------|-------------------|---------------------------|
| **Data model** | Hierarchical (directories/files) | Flat (bucket + key) |
| **Rename** | O(1) -- pointer update | O(n) -- copy + delete every object |
| **Append** | Supported | Not supported (must rewrite entire object) |
| **Consistency** | Strong | Eventually consistent (S3 was until 2020; now strong for reads) |
| **List directory** | Fast (NameNode in-memory lookup) | Slow (HTTP LIST API, paginated) |
| **Data locality** | Yes (compute near data) | No (compute and storage are separate) |
| **Scalability** | Limited by NameNode memory | Virtually unlimited |
| **Cost** | High (dedicated cluster always running) | Low (pay per GB stored + requests) |
| **Durability** | Depends on replication factor | 99.999999999% (11 nines, S3) |

### 10.2 Directories Are an Illusion

In S3, there are no directories. The path `s3://bucket/data/year=2024/month=01/file.parquet` is a single key in a flat namespace. The "/" is just part of the key string.

When Spark "lists a directory" on S3, it actually:
1. Sends an HTTP `LIST` request with prefix `data/year=2024/month=01/`
2. S3 returns all keys matching that prefix (paginated, 1000 keys per page)
3. For 100,000 files, this means 100 HTTP round-trips

This is why **listing files on S3 is slow** and why the small file problem is even worse on cloud storage.

### 10.3 Rename Is Expensive

HDFS rename is a metadata operation (O(1) -- just update pointers). S3 rename is:
1. Copy every object to the new key
2. Delete every object at the old key

For a Spark job writing to S3, the default `FileOutputCommitter` algorithm:
1. Writes files to a temporary directory
2. Renames them to the final directory

On HDFS: step 2 is instant. On S3: step 2 copies every single output file. For thousands of output files, this can take **minutes to hours** and is the #1 performance issue with Spark on S3.

**Solutions**:
- **S3A Committers** (Magic Committer, Staging Committer): Write directly to the final location using S3's multipart upload
- **Delta Lake**: Uses a transaction log instead of rename-based commits
- **EMRFS S3-optimized committer**: AWS-specific solution

### 10.4 Amazon S3

```
s3://my-bucket/data/table/year=2024/part-00000.parquet
     |          |                    |
     bucket     prefix (virtual dir) object key (file)
```

Key features:
- **Storage classes**: Standard, Intelligent-Tiering, Infrequent Access, Glacier, Deep Archive
- **Versioning**: Keep multiple versions of each object
- **Lifecycle policies**: Automatically move data between storage classes
- **Server-side encryption**: SSE-S3, SSE-KMS, SSE-C
- **Access control**: IAM policies, bucket policies, ACLs

### 10.5 Azure Data Lake Storage Gen2 (ADLS)

```
abfss://container@account.dfs.core.windows.net/data/table/
```

ADLS Gen2 provides a **hierarchical namespace** on top of Azure Blob Storage:
- True directory operations (rename is O(1), not copy+delete)
- POSIX-like permissions (ACLs)
- Optimized for analytics workloads
- Natively supported by Databricks and HDInsight

### 10.6 Google Cloud Storage (GCS)

```
gs://my-bucket/data/table/
```

Strong consistency for all operations (reads, writes, lists, deletes). No eventual consistency issues.

---

## 11. Table Formats -- Delta Lake, Iceberg, Hudi

### 11.1 The Problem They Solve

Raw Parquet files on a file/object store lack:
- **ACID transactions**: Concurrent writes can corrupt data
- **Schema enforcement**: No guarantee that files match the expected schema
- **Time travel**: Cannot query data as it existed yesterday
- **Efficient upserts**: Cannot update/delete individual rows without rewriting entire files
- **Consistent reads**: A reader might see partially-written data

Table formats add a **metadata layer** on top of Parquet/ORC files to provide these features.

### 11.2 Delta Lake

Delta Lake is an open-source table format created by Databricks.

**Architecture**:
```
/data/my_table/
├── _delta_log/                     <-- Transaction log
│   ├── 00000000000000000000.json   <-- Version 0 (initial write)
│   ├── 00000000000000000001.json   <-- Version 1 (insert)
│   ├── 00000000000000000002.json   <-- Version 2 (update)
│   ├── ...
│   └── 00000000000000000010.checkpoint.parquet  <-- Checkpoint every 10 versions
├── part-00000-xxx.parquet          <-- Actual data files
├── part-00001-xxx.parquet
└── ...
```

**Transaction log**: Each JSON file records what changed in that version (files added, files removed, metadata changes). The log is the **single source of truth** for the table's state.

**How reads work**:
1. Read the latest checkpoint (if exists)
2. Replay all JSON log entries after the checkpoint
3. Build the list of "active" data files
4. Read only those files

**How writes work**:
1. Write new Parquet data files
2. Atomically write a new JSON log entry listing the added/removed files
3. If another writer committed first (conflict), retry with optimistic concurrency

**Key features**:
```sql
-- Time travel: query data as of a specific version or timestamp
SELECT * FROM my_table VERSION AS OF 5
SELECT * FROM my_table TIMESTAMP AS OF '2024-01-15'

-- Schema enforcement
ALTER TABLE my_table ADD COLUMNS (new_col STRING)

-- MERGE (upsert)
MERGE INTO target USING source
  ON target.id = source.id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *

-- OPTIMIZE (compaction)
OPTIMIZE my_table

-- Z-ORDER (colocate related data for faster reads)
OPTIMIZE my_table ZORDER BY (city, date)

-- VACUUM (delete old files no longer referenced)
VACUUM my_table RETAIN 168 HOURS
```

### 11.3 Apache Iceberg

Iceberg is an open table format from Netflix, now an Apache project.

**Key differences from Delta Lake**:
- **Snapshot-based**: Each write creates a new snapshot pointing to a manifest list, which points to manifest files, which point to data files
- **Hidden partitioning**: Partition transforms are defined in the table metadata, not in the directory structure. Queries automatically benefit from partition pruning without users knowing the partitioning scheme
- **Partition evolution**: Can change the partitioning scheme without rewriting data
- **Schema evolution**: Full schema evolution (add, drop, rename, reorder columns) without rewriting data

```
Iceberg Table
├── Metadata file (current-snapshot, schema, partition spec)
├── Snapshot → Manifest List → Manifest Files → Data Files (Parquet)
```

### 11.4 Apache Hudi

Hudi (Hadoop Upserts Deletes and Incrementals) from Uber focuses on **upsert-heavy workloads**:

**Two table types**:
- **Copy-on-Write (CoW)**: Updates rewrite entire Parquet files. Better read performance. Slower writes.
- **Merge-on-Read (MoR)**: Updates go to delta log files. Faster writes. Reads must merge base files + deltas.

Best for: CDC (Change Data Capture) pipelines, streaming upserts, near-real-time analytics.

### 11.5 Comparison

| Feature | Delta Lake | Iceberg | Hudi |
|---------|-----------|---------|------|
| **ACID** | Yes | Yes | Yes |
| **Time travel** | Yes | Yes | Yes |
| **Schema evolution** | Yes | Yes (best) | Yes |
| **Partition evolution** | Requires rewrite | No rewrite needed | Limited |
| **Hidden partitioning** | No | Yes | No |
| **Upserts** | MERGE | MERGE | Native (optimized) |
| **Compaction** | OPTIMIZE | rewrite_data_files | Built-in (CoW/MoR) |
| **Streaming** | Structured Streaming | Flink, Spark | Flink, Spark, native |
| **Best with** | Databricks/Spark | Multi-engine (Spark, Flink, Trino, Presto) | CDC/upsert workloads |

---

## 12. Storage Lifecycle and Tiering

### 12.1 Data Temperature

Not all data is accessed equally:

| Tier | Access Pattern | Storage | Example |
|------|---------------|---------|---------|
| **Hot** | Frequently accessed (daily/hourly) | SSD, Standard S3 | Current week's data, active dashboards |
| **Warm** | Occasionally accessed (weekly/monthly) | HDD, S3 Infrequent Access | Last 3 months, ad-hoc queries |
| **Cold** | Rarely accessed (quarterly/yearly) | S3 Glacier, ADLS Cool | Historical archives, compliance data |
| **Frozen** | Almost never accessed (legal hold) | S3 Deep Archive | 7-year retention, audit logs |

### 12.2 Lifecycle Policies

```
S3 Lifecycle Example:
Day 0-30:    Standard storage ($0.023/GB/month)
Day 31-90:   Infrequent Access ($0.0125/GB/month)
Day 91-365:  Glacier ($0.004/GB/month)
Day 366+:    Deep Archive ($0.00099/GB/month)
```

For 100 TB of data:
- All in Standard: $2,300/month
- With lifecycle: ~$500/month (tiered by access patterns)

### 12.3 Implementing Tiering with Partitions

```python
# Partition by date for easy lifecycle management
df.write.partitionBy("year", "month").parquet("/data/events/")

# Results in:
# /data/events/year=2024/month=01/part-xxx.parquet  (hot)
# /data/events/year=2024/month=02/part-xxx.parquet  (hot)
# /data/events/year=2023/month=06/part-xxx.parquet  (warm)
# /data/events/year=2022/month=01/part-xxx.parquet  (cold)
```

Then apply S3 lifecycle rules that match the partition prefix patterns to move older partitions to cheaper storage classes.

### 12.4 TTL (Time-to-Live) Policies

```sql
-- Delta Lake: remove data files older than 7 days
VACUUM my_table RETAIN 168 HOURS

-- Hive: set table TTL
ALTER TABLE my_table SET TBLPROPERTIES ('transactional.properties.auto.purge'='true');

-- Manual partition deletion
ALTER TABLE my_table DROP PARTITION (year=2020, month=1);
```

---

## 13. Spark and Storage Interaction

### 13.1 How Spark Reads Files

```
1. Driver: List files in the input path
2. Driver: Split files into partitions (one partition per block/file/split)
3. Driver: Send partition info to executors
4. Executors: Each reads its assigned partitions in parallel
```

For Parquet:
```
Executor reads Parquet file:
1. Read footer (last few KB) → get schema, row group metadata, column statistics
2. Apply predicate pushdown → skip row groups where filter cannot match
3. Apply column pruning → read only needed column chunks
4. Decompress and decode → return rows
```

### 13.2 Partition Discovery

When Spark reads a partitioned dataset:
```python
df = spark.read.parquet("/data/events/")
```

Spark:
1. Lists all files under `/data/events/`
2. Discovers partition columns from directory names (e.g., `year=2024/month=01/`)
3. Adds partition columns as virtual columns to the DataFrame
4. Uses partition values for **partition pruning**: `WHERE year = 2024` skips all other year directories entirely

### 13.3 Predicate Pushdown

Three levels of predicate pushdown:

```
Level 1: Partition pruning
WHERE year = 2024  →  Only read files in year=2024/ directory

Level 2: Row group/stripe skipping (Parquet/ORC statistics)
WHERE age > 50  →  Skip row groups where max(age) <= 50

Level 3: Page-level filtering (Parquet page index, ORC bloom filters)
WHERE id = 12345  →  Skip pages that definitely don't contain this value
```

### 13.4 Data Locality in Spark

When Spark runs on a cluster co-located with HDFS:
```
Spark Scheduler:
1. Get block locations from NameNode
2. For each partition, prefer to schedule the task on a node that holds the block
3. Fallback order: data-local → rack-local → any node

Spark UI shows locality levels:
- PROCESS_LOCAL: data is in the same JVM's cache (best)
- NODE_LOCAL: data is on the same machine's disk
- RACK_LOCAL: data is in the same rack
- ANY: data is on a different rack (worst)
```

When Spark runs against cloud storage (S3, ADLS), there is **no data locality** -- all reads are remote. Performance depends entirely on network bandwidth, which is why cloud Spark clusters emphasize:
- Fast network (10+ Gbps)
- Columnar formats (minimize data transferred)
- Aggressive predicate pushdown and column pruning

### 13.5 Speculative Execution

If one task is much slower than others (straggler), Spark can launch a **speculative copy** of that task on another node:

```python
spark.conf.set("spark.speculation", True)
spark.conf.set("spark.speculation.multiplier", 1.5)  # 1.5x slower than median
spark.conf.set("spark.speculation.quantile", 0.75)    # after 75% of tasks complete
```

In HDFS, stragglers often occur due to a slow disk or a degraded DataNode. Speculative execution reads the same block from a different replica on a different node.

### 13.6 Write Patterns

```python
# Overwrite: replace all data
df.write.mode("overwrite").parquet("/output/")

# Append: add new files
df.write.mode("append").parquet("/output/")

# Partitioned overwrite: replace only specific partitions
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.mode("overwrite").partitionBy("date").parquet("/output/")
# Only replaces partitions that appear in df, leaves others untouched

# Bucketed write (for join optimization)
df.write.bucketBy(256, "user_id").sortBy("user_id").saveAsTable("events_bucketed")
```

### 13.7 Output Committers

When Spark writes output files, it must handle the case where a task fails mid-write:

**Algorithm 1 (default for non-S3)**:
1. Each task writes to a temporary file
2. On task success, rename temp file to the task's final path
3. On job success, rename all task outputs to the final directory

**Algorithm 2 (better for S3)**:
1. Each task writes directly to the final directory with a unique name
2. On job completion, the driver lists and validates all output files

**S3A Committers**: Write using S3 multipart upload, committing by completing the upload. No rename needed.

---

## 14. Quick-Reference Cheat Sheet

### HDFS Key Numbers

| Component | Default Value |
|-----------|-------------|
| Block size | 128 MB |
| Replication factor | 3 |
| Heartbeat interval | 3 seconds |
| Block report interval | 6 hours |
| NameNode memory per file | ~150 bytes |
| NameNode memory per block | ~150 bytes |

### File Format Selection

```
Analytics query on specific columns? → Parquet
Full-row streaming / CDC? → Avro
Hive ecosystem? → ORC
Human-readable exchange? → CSV/JSON
Need ACID / time travel? → Delta Lake / Iceberg / Hudi
```

### Compression Selection

```
Spark default / balanced: Snappy
Best compression ratio: Zstd
Legacy compatibility: Gzip (in Parquet, still splittable)
Maximum compression (rare reads): Gzip or Bzip2
```

### Cloud Storage Quick Reference

| Feature | S3 | ADLS Gen2 | GCS |
|---------|-----|-----------|------|
| **Rename** | Copy+delete (slow) | O(1) with HNS | Copy+delete (slow) |
| **Consistency** | Strong (since 2020) | Strong | Strong |
| **Spark connector** | s3a:// | abfss:// | gs:// |
| **Cheapest archive** | Deep Archive | Archive | Archive |

### Table Format Selection

```
Databricks ecosystem? → Delta Lake
Multi-engine (Spark + Flink + Trino)? → Iceberg
Heavy upserts / CDC? → Hudi
General purpose Spark analytics? → Delta Lake or Iceberg
```

### Interview Red Flags to Avoid

| Bad | Good |
|-----|------|
| "HDFS is just like a regular filesystem" | "HDFS splits files into 128MB blocks replicated across nodes for fault tolerance and parallel I/O" |
| "CSV is fine for big data" | "Columnar formats like Parquet reduce I/O by 5-10x through column pruning and compression" |
| "S3 is a filesystem" | "S3 is an object store with flat namespace; rename is O(n) copy+delete, which affects Spark commit protocols" |
| "Just use Parquet for everything" | "Parquet for analytics, Avro for streaming/CDC, Delta Lake when you need ACID and time travel" |
| "Compression doesn't matter" | "Choosing between Snappy (fast) and Zstd (better ratio) can save 30-50% storage and improve read throughput" |
