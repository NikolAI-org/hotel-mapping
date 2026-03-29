# Delta Lake Features - Implementation Reference

## Overview
This document explains how Delta Lake features are implemented in this pipeline.

---

## Core Delta Lake Features

### 1. ACID Transactions

**What It Is:**
- Atomicity: All-or-nothing writes
- Consistency: Schema enforcement
- Isolation: Concurrent operations
- Durability: Reliable storage

**Implementation:**
```python
# In delta_transform.py
df_transformed.write \
    .format("delta") \
    .mode("overwrite") \
    .save(delta_table_path)
```

**Benefits:**
- ✅ No partial writes
- ✅ Data consistency guaranteed
- ✅ Safe concurrent access
- ✅ Recoverable from failures

---

### 2. Time Travel

**What It Is:**
Query historical versions of your data using version numbers or timestamps.

**Implementation:**
```python
# In delta_time_travel.py

# Query specific version
df_v0 = spark.read \
    .format("delta") \
    .option("versionAsOf", 0) \
    .load(delta_table_path)

# Query at timestamp
df_ts = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2024-01-01T10:00:00") \
    .load(delta_table_path)
```

**Use Cases:**
- Audit data changes
- Reproduce ML experiments
- Rollback errors
- Compare versions

---

### 3. Schema Enforcement & Evolution

**What It Is:**
Delta Lake validates schema on write and supports controlled evolution.

**Implementation:**
```python
# Schema enforcement (automatic)
df.write.format("delta").mode("append").save(path)

# Schema evolution (explicit)
df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(path)
```

**Configuration:**
```properties
# In spark-defaults.conf
spark.databricks.delta.schema.autoMerge.enabled=false
```

---

### 4. MERGE Operations (Upsert)

**What It Is:**
Efficiently update existing records and insert new ones in a single operation.

**Implementation:**
```python
# In delta_merge.py
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, delta_table_path)

deltaTable.alias("target").merge(
    df_upsert.alias("source"),
    "target.booking_id = source.booking_id"
).whenMatchedUpdate(set={
    "is_cancelled": col("source.is_cancelled"),
    "total_amount": col("source.total_amount"),
    "revenue": col("source.revenue"),
    "processed_timestamp": col("source.processed_timestamp")
}).whenNotMatchedInsertAll().execute()
```

**Benefits:**
- Single atomic operation
- Efficient CDC (Change Data Capture)
- Idempotent operations
- Handles late-arriving data

---

### 5. Data Versioning

**What It Is:**
Every write creates a new version tracked in `_delta_log/`.

**Implementation:**
```python
# Get version history
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, path)
history_df = deltaTable.history()
history_df.show()
```

**Version Metadata:**
- Version number
- Timestamp
- Operation (WRITE, MERGE, DELETE)
- User/job information
- Metrics (rows added/removed)

---

### 6. Optimize & Z-Ordering

**What It Is:**
Compact small files and cluster data for better performance.

**Implementation:**
```sql
-- Compact files
OPTIMIZE delta.`s3a://delta-lake/hotel_bookings/`

-- Z-order clustering
OPTIMIZE delta.`s3a://delta-lake/hotel_bookings/`
ZORDER BY (city, booking_date)
```

**Python API:**
```python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, path)
deltaTable.optimize().executeCompaction()
deltaTable.optimize().executeZOrderBy("city", "booking_date")
```

---

### 7. VACUUM

**What It Is:**
Remove old data files no longer needed (for data older than retention period).

**Implementation:**
```python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, path)

# Remove files older than 7 days
deltaTable.vacuum(7)
```

**Configuration:**
```properties
# In spark-defaults.conf
spark.databricks.delta.retentionDurationCheck.enabled=false
```

⚠️ **Warning:** Only run VACUUM if you don't need time-travel to old versions!

---

### 8. Change Data Feed

**What It Is:**
Track row-level changes (inserts, updates, deletes) between versions.

**Implementation:**
```python
# Enable during table creation
df.write \
    .format("delta") \
    .option("delta.enableChangeDataFeed", "true") \
    .save(path)

# Read changes between versions
changes_df = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .option("endingVersion", 2) \
    .load(path)
```

**Configuration:**
```properties
# In spark-defaults.conf
spark.databricks.delta.properties.defaults.enableChangeDataFeed=true
```

---

## S3A Configuration for MinIO

### Required Settings

```properties
# In spark-defaults.conf

# S3A Implementation
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem

# MinIO Endpoint
spark.hadoop.fs.s3a.endpoint=http://minio:9000

# Credentials
spark.hadoop.fs.s3a.access.key=minioadmin
spark.hadoop.fs.s3a.secret.key=minioadmin

# Path Style (required for MinIO)
spark.hadoop.fs.s3a.path.style.access=true

# Disable SSL
spark.hadoop.fs.s3a.connection.ssl.enabled=false

# AWS Credentials Provider
spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
```

### Performance Tuning

```properties
# Fast upload
spark.hadoop.fs.s3a.fast.upload=true
spark.hadoop.fs.s3a.fast.upload.buffer=disk

# Block size
spark.hadoop.fs.s3a.block.size=128M

# Multipart upload
spark.hadoop.fs.s3a.multipart.size=104857600
spark.hadoop.fs.s3a.multipart.threshold=2147483647
```

---

## Spark-Delta Integration

### Required Packages

```
io.delta:delta-core_2.12:2.4.0
org.apache.hadoop:hadoop-aws:3.3.4
```

### Spark Configuration

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Delta Lake") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

### In Airflow SparkSubmitOperator

```python
SparkSubmitOperator(
    task_id='delta_job',
    application='/opt/spark-jobs/delta_transform.py',
    conf={
        'spark.master': 'spark://spark-master:7077',
        'spark.jars.packages': 'io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4',
        'spark.sql.extensions': 'io.delta.sql.DeltaSparkSessionExtension',
        'spark.sql.catalog.spark_catalog': 'org.apache.spark.sql.delta.catalog.DeltaCatalog',
        # ... S3A configs
    }
)
```

---

## Common Operations

### Create Delta Table

```python
df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://delta-lake/my_table/")
```

### Read Delta Table

```python
df = spark.read \
    .format("delta") \
    .load("s3a://delta-lake/my_table/")
```

### Append Data

```python
df_new.write \
    .format("delta") \
    .mode("append") \
    .save("s3a://delta-lake/my_table/")
```

### Update Data

```python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "s3a://delta-lake/my_table/")
deltaTable.update(
    condition = "status = 'pending'",
    set = {"status": "'completed'"}
)
```

### Delete Data

```python
deltaTable.delete("booking_date < '2023-01-01'")
```

### Get Table History

```python
deltaTable.history().show()
```

### Describe Table

```python
deltaTable.detail().show()
```

---

## Delta Lake vs Parquet

| Feature | Delta Lake | Parquet |
|---------|-----------|---------|
| Format | Open source lakehouse format | Column storage format |
| ACID | ✅ Yes | ❌ No |
| Time Travel | ✅ Yes | ❌ No |
| Schema Enforcement | ✅ Yes | ❌ No |
| Upserts (MERGE) | ✅ Efficient | ⚠️ Read all + rewrite |
| Concurrent Writes | ✅ Safe | ❌ Not safe |
| Streaming | ✅ Exactly-once | ⚠️ At-least-once |
| Performance | ✅ Optimized | ✅ Fast reads |
| Metadata | ✅ Rich (_delta_log) | ⚠️ Limited |
| Updates | ✅ Row-level | ❌ Rewrite partition |

---

## Best Practices

### 1. Partitioning
```python
df.write \
    .format("delta") \
    .partitionBy("booking_date") \
    .save(path)
```

### 2. Optimize Regularly
```python
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, path)
deltaTable.optimize().executeCompaction()
```

### 3. Z-Order for Queries
```python
# For queries filtering on city and date
deltaTable.optimize().executeZOrderBy("city", "booking_date")
```

### 4. Vacuum Old Files
```python
# Keep 30 days of history
deltaTable.vacuum(30)
```

### 5. Enable Statistics
```python
spark.conf.set("spark.databricks.delta.stats.collect", "true")
```

### 6. Monitor Table Size
```python
deltaTable.detail().select("numFiles", "sizeInBytes").show()
```

---

## Troubleshooting

### Issue: Protocol Version Error
```
ProtocolChangedException: The protocol version of the Delta table has been changed
```

**Solution:**
```python
# Restore to previous version
deltaTable.restoreToVersion(0)
```

### Issue: Concurrent Write Conflict
```
ConcurrentAppendException: Files were added to the table
```

**Solution:**
```python
# Configure retry
spark.conf.set("spark.databricks.delta.write.txnAppId", "unique-id")
spark.conf.set("spark.databricks.delta.write.txnVersion", "1")
```

### Issue: Schema Mismatch
```
AnalysisException: A schema mismatch detected when writing to the Delta table
```

**Solution:**
```python
# Enable schema evolution
df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save(path)
```

---

## References

- **Delta Lake Docs**: https://docs.delta.io/
- **Delta Lake GitHub**: https://github.com/delta-io/delta
- **API Docs**: https://docs.delta.io/latest/api/python/index.html
- **Best Practices**: https://docs.delta.io/latest/best-practices.html

---

*This reference guide covers Delta Lake features implemented in the hotel-mapping-airflow project.*
