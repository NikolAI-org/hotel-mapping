# Delta Lake Data Pipeline with Airflow & Spark

A production-grade data pipeline leveraging **Apache Airflow**, **Apache Spark**, **Delta Lake**, and **MinIO** for robust data processing and storage.

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Apache Airflow                                │
│  ┌──────────────┐  ┌───────────┐  ┌────────────┐               │
│  │  Webserver   │  │ Scheduler │  │   Worker   │               │
│  └──────────────┘  └───────────┘  └────────────┘               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Apache Spark Cluster                          │
│  ┌──────────────┐              ┌────────────────┐              │
│  │    Master    │◄────────────►│     Worker     │              │
│  └──────────────┘              └────────────────┘              │
│        (Delta Lake Configured)                                   │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    MinIO (S3-Compatible)                         │
│  Buckets: data-lake, delta-lake                                 │
└─────────────────────────────────────────────────────────────────┘
```

## 📋 Components

### Scoring Logic Doc
- `docs/SCORING_LOGIC.md` - concise reference for pair-scoring formulas, mismatch rules, and postal-code handling in `address_unit_score`

### Core Services
- **Airflow**: Orchestration (Webserver, Scheduler, Worker, Postgres, Redis)
- **Spark**: Processing (1 Master, 1 Worker) using Bitnami images
- **MinIO**: S3-compatible object storage
- **PostgreSQL**: Airflow metadata database
- **Redis**: Celery message broker

### Key Features
- ✅ Delta Lake integration with Spark
- ✅ S3A filesystem for MinIO access
- ✅ ACID transactions
- ✅ Time-travel queries
- ✅ MERGE/Upsert operations
- ✅ Schema evolution
- ✅ Data versioning

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- At least 4GB RAM available
- 10GB free disk space

### Setup Instructions

1. **Clone/Navigate to the project directory**
```bash
cd hotel-mapping-airflow
```

2. **Configure environment variables**
```bash
# The .env file is already created with default values
# Modify if needed:
# - AWS_ACCESS_KEY (default: minioadmin)
# - AWS_SECRET_KEY (default: minioadmin)
# - MINIO_ENDPOINT (default: http://minio:9000)
```

3. **Set proper permissions (Linux/Mac)**
```bash
mkdir -p ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" >> .env
```

4. **Start all services**
```bash
docker-compose up -d
```

5. **Wait for services to be healthy** (takes ~2-3 minutes)
```bash
docker-compose ps
```

6. **Access the services**
- **Airflow UI**: http://localhost:8080 (Username: `airflow`, Password: `airflow`)
- **Spark Master UI**: http://localhost:8081
- **MinIO Console**: http://localhost:9001 (Username: `minioadmin`, Password: `minioadmin`)

## 📂 Project Structure

```
hotel-mapping-airflow/
├── docker-compose.yaml          # Main orchestration file
├── Dockerfile                   # Custom Airflow image with dependencies
├── .env                         # Environment configuration
├── .env.example                 # Example environment file
├── dags/                        # Airflow DAGs
│   ├── 01_data_ingestion_dag.py          # Data ingestion to MinIO
│   └── 02_delta_transformation_dag.py    # Delta Lake transformations
├── spark/
│   ├── conf/
│   │   └── spark-defaults.conf  # Spark configuration for Delta Lake
│   └── jobs/                    # Spark job scripts
│       ├── delta_transform.py   # Initial Delta transformation
│       ├── delta_merge.py       # MERGE/Upsert operations
│       └── delta_time_travel.py # Time-travel queries
├── config/                      # Additional configurations
├── logs/                        # Airflow logs
├── plugins/                     # Airflow plugins
└── data/                        # Local data directory
```

## 🔧 Configuration Details

### Spark Configuration (spark-defaults.conf)

The Spark cluster is pre-configured with:

```properties
# Delta Lake Extensions
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# S3A Configuration for MinIO
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.access.key=minioadmin
spark.hadoop.fs.s3a.secret.key=minioadmin
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.connection.ssl.enabled=false
```

### Delta Lake Packages

```
io.delta:delta-core_2.12:2.4.0
org.apache.hadoop:hadoop-aws:3.3.4
```

## 📊 Sample DAGs

### DAG 1: Data Ingestion (`hotel_data_ingestion`)

**Purpose**: Generate sample hotel booking data and upload to MinIO

**Tasks**:
1. `generate_data`: Creates 1000 sample hotel booking records
2. `upload_to_minio`: Uploads CSV and JSON to MinIO bucket

**Schedule**: Daily (`@daily`)

**Output**: `s3a://data-lake/raw/hotel_bookings/`

### DAG 2: Delta Transformation (`hotel_delta_transformation`)

**Purpose**: Transform raw data to Delta Lake format and demonstrate Delta capabilities

**Tasks**:
1. `transform_to_delta`: Read CSV, apply transformations, write to Delta format
2. `delta_merge_operations`: Perform MERGE/Upsert operations
3. `delta_time_travel`: Execute time-travel queries across versions
4. `log_completion`: Log completion status

**Schedule**: Daily (`@daily`)

**Output**: `s3a://delta-lake/hotel_bookings/`

**Delta Features Demonstrated**:
- Schema enforcement
- ACID transactions
- Version control
- Time-travel queries (versionAsOf, timestampAsOf)
- MERGE INTO operations
- Aggregations and analytics

## 🎯 Usage Examples

### Running the Pipeline

1. **Trigger Data Ingestion DAG**
   - Navigate to Airflow UI → DAGs → `hotel_data_ingestion`
   - Click "Trigger DAG" button
   - Wait for completion (~1-2 minutes)

2. **Trigger Delta Transformation DAG**
   - Navigate to Airflow UI → DAGs → `hotel_delta_transformation`
   - Click "Trigger DAG" button
   - Monitor execution in Spark UI (http://localhost:8081)

### Accessing Data in MinIO

```python
# Using boto3/minio client
from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# List objects in data-lake bucket
objects = client.list_objects("data-lake", recursive=True)
for obj in objects:
    print(obj.object_name)
```

### Querying Delta Tables

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Query Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Read latest version
df = spark.read.format("delta").load("s3a://delta-lake/hotel_bookings/")
df.show()

# Time-travel to version 0
df_v0 = spark.read.format("delta").option("versionAsOf", 0).load("s3a://delta-lake/hotel_bookings/")
df_v0.show()
```

## 🔍 Monitoring & Troubleshooting

### View Service Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f airflow-webserver
docker-compose logs -f spark-master
docker-compose logs -f minio
```

### Check Service Health

```bash
docker-compose ps
```

### Access Airflow CLI

```bash
docker-compose run --rm airflow-cli bash
```

### Spark Job Monitoring
- Spark Master UI: http://localhost:8081
- View running/completed applications
- Check executor logs and metrics

### Common Issues

1. **Permission Denied Errors**
   ```bash
   # Fix ownership
   sudo chown -R $(id -u):$(id -g) ./logs ./plugins
   ```

2. **MinIO Connection Issues**
   - Ensure MinIO is healthy: `docker-compose ps minio`
   - Check endpoint configuration in .env file
   - Verify bucket exists: Access MinIO Console

3. **Spark Job Failures**
   - Check Spark Master logs: `docker-compose logs spark-master`
   - Verify Delta Lake packages are loaded
   - Ensure S3A configuration is correct

## 🛠️ Customization

### Adding New DAGs

1. Create a new Python file in `dags/` directory
2. Define your DAG with proper operators
3. Airflow will auto-detect it within 30 seconds

### Modifying Spark Configuration

Edit `spark/conf/spark-defaults.conf` and restart Spark services:

```bash
docker-compose restart spark-master spark-worker
```

### Scaling Workers

Modify `docker-compose.yaml` to add more Spark workers:

```yaml
spark-worker-2:
  image: bitnami/spark:3.5.0
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
    # ... same config as spark-worker
```

## 📈 Performance Tuning

### Spark Memory Configuration

Adjust in `spark/conf/spark-defaults.conf`:

```properties
spark.driver.memory=2g
spark.executor.memory=4g
spark.executor.cores=4
```

### Airflow Concurrency

Modify `docker-compose.yaml`:

```yaml
AIRFLOW__CORE__PARALLELISM: 32
AIRFLOW__CORE__DAG_CONCURRENCY: 16
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 16
```

## 🔒 Security Considerations

**⚠️ This setup is for development/testing. For production:**

1. Change default credentials in `.env`
2. Enable SSL/TLS for all services
3. Use proper authentication mechanisms
4. Implement network isolation
5. Enable Airflow RBAC
6. Use secrets management (Vault, AWS Secrets Manager)

## 📚 Additional Resources

- [Delta Lake Documentation](https://docs.delta.io/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [MinIO Documentation](https://min.io/docs/)

## 🧹 Cleanup

```bash
# Stop all services
docker-compose down

# Remove volumes (deletes all data)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

## 📝 License

This project is provided as-is for educational and development purposes.

## 🤝 Contributing

Feel free to submit issues, fork the repository, and create pull requests for any improvements.

---

**Built with ❤️ for Data Engineering**
