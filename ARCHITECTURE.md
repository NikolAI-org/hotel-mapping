# Delta Lake Data Pipeline - Architecture & Technical Specifications

## System Architecture

```
┌───────────────────────────────────────────────────────────────────────────┐
│                              CLIENT LAYER                                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          │
│  │  Airflow UI     │  │  Spark UI       │  │  MinIO Console  │          │
│  │  :8080          │  │  :8081          │  │  :9001          │          │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘          │
└───────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                         ORCHESTRATION LAYER                                │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │                      Apache Airflow (v2.8.0)                        │ │
│  │  ┌──────────────┐  ┌───────────────┐  ┌──────────────────────────┐ │ │
│  │  │  Webserver   │  │   Scheduler   │  │  Worker (CeleryExecutor) │ │ │
│  │  └──────────────┘  └───────────────┘  └──────────────────────────┘ │ │
│  │                                                                      │ │
│  │  Providers:                                                          │ │
│  │  • apache-airflow-providers-apache-spark (v4.6.0)                   │ │
│  │  • SparkSubmitOperator for job submission                           │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
│  ┌─────────────────┐              ┌─────────────────┐                     │
│  │  PostgreSQL     │              │     Redis       │                     │
│  │  (Metadata DB)  │              │  (Celery Broker)│                     │
│  └─────────────────┘              └─────────────────┘                     │
└───────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                         PROCESSING LAYER                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │              Apache Spark Cluster (v3.5.0)                          │ │
│  │                                                                      │ │
│  │  ┌──────────────────────┐         ┌────────────────────────────┐   │ │
│  │  │   Spark Master       │◄───────►│    Spark Worker            │   │ │
│  │  │   :7077 (RPC)        │         │    Memory: 2G              │   │ │
│  │  │   :8080 (Web UI)     │         │    Cores: 2                │   │ │
│  │  └──────────────────────┘         └────────────────────────────┘   │ │
│  │                                                                      │ │
│  │  Extensions & Dependencies:                                          │ │
│  │  • io.delta:delta-core_2.12:2.4.0                                   │ │
│  │  • org.apache.hadoop:hadoop-aws:3.3.4                               │ │
│  │  • Delta Lake SQL Extensions                                        │ │
│  │  • S3A FileSystem Support                                            │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌───────────────────────────────────────────────────────────────────────────┐
│                          STORAGE LAYER                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐ │
│  │              MinIO (S3-Compatible Object Storage)                   │ │
│  │                                                                      │ │
│  │  ┌────────────────────────┐    ┌──────────────────────────────┐    │ │
│  │  │   Bucket: data-lake    │    │   Bucket: delta-lake         │    │ │
│  │  │   ├── raw/             │    │   ├── hotel_bookings/        │    │ │
│  │  │   │   └── hotel_       │    │   │   ├── _delta_log/        │    │ │
│  │  │   │       bookings/    │    │   │   └── *.parquet          │    │ │
│  │  │   │       ├── *.csv    │    │   └── hotel_bookings_       │    │ │
│  │  │   │       └── *.json   │    │       aggregated/            │    │ │
│  │  └────────────────────────┘    └──────────────────────────────┘    │ │
│  │                                                                      │ │
│  │  Access: S3A Protocol (s3a://)                                       │ │
│  │  Endpoint: http://minio:9000                                         │ │
│  └─────────────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

```
1. INGESTION PHASE
   ┌─────────────┐
   │ Python Task │ → Generate sample hotel booking data
   └─────────────┘
         │
         ▼
   ┌─────────────┐
   │ Python Task │ → Upload CSV/JSON to MinIO (s3a://data-lake/raw/)
   └─────────────┘

2. TRANSFORMATION PHASE
   ┌──────────────────┐
   │ SparkSubmitOp    │ → Read raw CSV from MinIO
   │ delta_transform  │ → Apply transformations (dates, calculations)
   └──────────────────┘ → Write to Delta Lake (s3a://delta-lake/)
         │
         ▼
   ┌──────────────────┐
   │ SparkSubmitOp    │ → Read Delta table
   │ delta_merge      │ → Perform MERGE/Upsert operations
   └──────────────────┘ → Update records, insert new ones
         │
         ▼
   ┌──────────────────┐
   │ SparkSubmitOp    │ → Query Delta table history
   │ delta_timetravel │ → Compare versions
   └──────────────────┘ → Demonstrate time-travel queries

3. ANALYTICS PHASE
   ┌─────────────┐
   │ Delta Table │ → Ready for BI tools, queries, ML
   └─────────────┘
```

## Technical Specifications

### Delta Lake Configuration

**Core Extensions:**
```properties
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

**S3A Configuration:**
```properties
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.access.key=minioadmin
spark.hadoop.fs.s3a.secret.key=minioadmin
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.connection.ssl.enabled=false
```

**Performance Settings:**
```properties
spark.hadoop.fs.s3a.fast.upload=true
spark.hadoop.fs.s3a.fast.upload.buffer=disk
spark.hadoop.fs.s3a.block.size=128M
spark.databricks.delta.retentionDurationCheck.enabled=false
spark.sql.adaptive.enabled=true
```

### Container Resources

| Service | CPU | Memory | Ports |
|---------|-----|--------|-------|
| airflow-webserver | 1 | 1GB | 8080 |
| airflow-scheduler | 1 | 1GB | - |
| airflow-worker | 1 | 1GB | - |
| postgres | 0.5 | 512MB | 5432 |
| redis | 0.5 | 256MB | 6379 |
| spark-master | 1 | 1GB | 7077, 8081 |
| spark-worker | 2 | 2GB | - |
| minio | 1 | 512MB | 9000, 9001 |

### Network Configuration

**Network:** delta-lake-network (bridge)

**Service Discovery:**
- Services communicate via Docker DNS
- Hostnames: minio, spark-master, postgres, redis
- Internal communication only (except exposed ports)

## Delta Lake Features Implemented

### 1. ACID Transactions
- Atomicity: All-or-nothing writes
- Consistency: Schema enforcement
- Isolation: Concurrent reads/writes
- Durability: Persistent storage in MinIO

### 2. Time Travel
```python
# Query specific version
df = spark.read.format("delta").option("versionAsOf", 0).load(path)

# Query at timestamp
df = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(path)
```

### 3. Schema Evolution
- Automatic schema detection
- Schema merging on write
- Schema validation

### 4. MERGE Operations (Upsert)
```python
deltaTable.merge(
    source,
    "target.id = source.id"
).whenMatchedUpdate(
    set = {...}
).whenNotMatchedInsert(
    values = {...}
).execute()
```

### 5. Data Versioning
- Full history tracking
- Version metadata in _delta_log
- Rollback capabilities

## DAG Execution Flow

### DAG 1: hotel_data_ingestion
```
generate_data (PythonOperator)
    ↓
    • Creates 1000 sample hotel booking records
    • Saves to /opt/data/hotel_bookings_raw.csv
    ↓
upload_to_minio (PythonOperator)
    ↓
    • Uploads CSV to s3a://data-lake/raw/hotel_bookings/
    • Uploads JSON version for variety
    • Returns S3 path for next DAG
```

### DAG 2: hotel_delta_transformation
```
transform_to_delta (SparkSubmitOperator)
    ↓
    • Read CSV from MinIO
    • Transform: date conversions, calculations
    • Write to Delta: s3a://delta-lake/hotel_bookings/
    ↓
delta_merge_operations (SparkSubmitOperator)
    ↓
    • Read existing Delta table
    • Create updates (simulated cancellations, price changes)
    • MERGE: Update existing + Insert new records
    ↓
delta_time_travel (SparkSubmitOperator)
    ↓
    • Query Delta table history
    • Compare versions (v0 vs latest)
    • Demonstrate versionAsOf and timestampAsOf
    • Analyze changes over time
    ↓
log_completion (PythonOperator)
    ↓
    • Log success metrics
    • Display Delta features used
```

## Data Schema

### Raw Data (CSV/JSON)
```
booking_id: string
hotel_name: string
city: string
room_type: string
booking_date: date
checkin_date: date
checkout_date: date
num_guests: integer
total_amount: double
is_cancelled: boolean
customer_id: string
```

### Transformed Delta Table
```
booking_id: string
hotel_name: string
city: string
room_type: string
booking_date: date
checkin_date: date
checkout_date: date
num_guests: integer
total_amount: double
is_cancelled: boolean
customer_id: string
nights_stayed: integer          (calculated)
booking_lead_time: integer      (calculated)
revenue: double                 (calculated)
processed_timestamp: timestamp  (added)
```

## Security Considerations

### Current (Development) Setup
- Default credentials in use
- No SSL/TLS encryption
- Open network access within Docker
- No authentication beyond basic

### Production Recommendations
1. **Credentials Management**
   - Use AWS Secrets Manager / HashiCorp Vault
   - Rotate credentials regularly
   - Strong passwords (min 16 chars)

2. **Network Security**
   - Private subnets for services
   - VPC peering for cross-service communication
   - Firewall rules
   - VPN access for management

3. **Encryption**
   - SSL/TLS for all communication
   - Encryption at rest (MinIO SSE-S3)
   - Encryption in transit (HTTPS, TLS)

4. **Access Control**
   - Airflow RBAC enabled
   - IAM roles for services
   - Least privilege principle
   - Audit logging

5. **Data Governance**
   - Column-level encryption for PII
   - Data masking
   - Retention policies
   - Compliance (GDPR, CCPA)

## Monitoring & Observability

### Built-in UIs
- **Airflow**: Task execution, DAG visualization, logs
- **Spark**: Job progress, executor metrics, stage details
- **MinIO**: Bucket metrics, access logs, storage usage

### Recommended Additions
- Prometheus + Grafana for metrics
- ELK Stack for log aggregation
- Alertmanager for notifications
- DataDog / New Relic for APM

## Scalability

### Horizontal Scaling
```yaml
# Add more Spark workers
docker-compose up -d --scale spark-worker=3

# Add more Airflow workers
docker-compose up -d --scale airflow-worker=3
```

### Vertical Scaling
- Increase worker memory (SPARK_WORKER_MEMORY)
- Increase executor cores (spark.executor.cores)
- Allocate more Docker resources

### Production Scaling
- Kubernetes deployment (Helm charts)
- Auto-scaling based on metrics
- Separate compute and storage
- Distributed MinIO cluster

## Cost Optimization

1. **Compute**: Spot/preemptible instances for workers
2. **Storage**: Lifecycle policies for old versions
3. **Network**: VPC endpoints to avoid data transfer costs
4. **Compression**: Snappy/Zstd for Delta tables

## Disaster Recovery

### Backup Strategy
```bash
# MinIO data backup
mc mirror minio/delta-lake s3/backup-bucket/

# Airflow metadata backup
docker-compose exec postgres pg_dump -U airflow > backup.sql
```

### Recovery
- Delta Lake: Native versioning provides recovery
- Point-in-time restore from backups
- Multi-region replication for HA

## Performance Benchmarks

### Expected Performance (1000 records)
- Ingestion: < 30 seconds
- Delta Transform: < 60 seconds
- Merge Operation: < 45 seconds
- Time Travel Query: < 10 seconds

### Optimization Tips
- Partition large tables by date
- Z-order clustering for common queries
- Optimize writes (bin-packing)
- Regular VACUUM operations

---

**Version:** 1.0.0  
**Last Updated:** January 2026  
**Maintained By:** Data Engineering Team
