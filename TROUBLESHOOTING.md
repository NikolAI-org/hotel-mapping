# Delta Lake Airflow Pipeline - Troubleshooting Guide

## Common Issues and Solutions

### 1. Permission Denied Errors

**Symptom:**
```
PermissionError: [Errno 13] Permission denied: '/opt/airflow/logs'
```

**Solution:**
```bash
# On Linux/Mac
sudo chown -R $(id -u):$(id -g) ./logs ./plugins ./data
chmod -R 755 ./logs ./plugins ./data

# Or set AIRFLOW_UID in .env
echo "AIRFLOW_UID=$(id -u)" >> .env
docker-compose down -v
docker-compose up -d
```

---

### 2. MinIO Connection Refused

**Symptom:**
```
ConnectionRefusedError: [Errno 111] Connection refused
```

**Solution:**
```bash
# Check if MinIO is running
docker-compose ps minio

# Restart MinIO
docker-compose restart minio

# Check MinIO logs
docker-compose logs minio

# Test connection
docker-compose exec airflow-webserver python -c "from minio import Minio; client = Minio('minio:9000', 'minioadmin', 'minioadmin', secure=False); print(list(client.list_buckets()))"
```

---

### 3. Spark Job Fails with ClassNotFoundException

**Symptom:**
```
java.lang.ClassNotFoundException: org.apache.spark.sql.delta.catalog.DeltaCatalog
```

**Solution:**
```bash
# Ensure packages are loaded correctly in DAG
# Check the SparkSubmitOperator configuration includes:
'spark.jars.packages': 'io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4'

# Restart Spark services
docker-compose restart spark-master spark-worker

# Check Spark logs
docker-compose logs spark-master spark-worker
```

---

### 4. Airflow DAG Not Appearing

**Symptom:**
DAG files are in `dags/` folder but don't appear in Airflow UI

**Solution:**
```bash
# Check DAG folder is mounted
docker-compose exec airflow-webserver ls -la /opt/airflow/dags

# Check for Python syntax errors
docker-compose exec airflow-webserver python -m py_compile /opt/airflow/dags/01_data_ingestion_dag.py

# Check Airflow scheduler logs
docker-compose logs airflow-scheduler

# Refresh DAGs
docker-compose restart airflow-scheduler
```

---

### 5. S3A FileSystem Not Found

**Symptom:**
```
java.lang.RuntimeException: java.lang.ClassNotFoundException: Class org.apache.hadoop.fs.s3a.S3AFileSystem not found
```

**Solution:**
```bash
# Ensure hadoop-aws package is included
# In SparkSubmitOperator, add:
'spark.jars.packages': 'io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4'

# Verify spark-defaults.conf has S3A settings
cat spark/conf/spark-defaults.conf | grep s3a
```

---

### 6. Out of Memory Errors

**Symptom:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Solution:**
```bash
# Increase Spark memory in docker-compose.yaml
# For spark-worker:
environment:
  - SPARK_WORKER_MEMORY=4G  # Increase from 2G

# Or in spark-defaults.conf:
spark.driver.memory=2g
spark.executor.memory=4g

# Restart Spark
docker-compose restart spark-master spark-worker
```

---

### 7. Airflow Database Connection Error

**Symptom:**
```
sqlalchemy.exc.OperationalError: (psycopg2.OperationalError) could not connect to server
```

**Solution:**
```bash
# Check if PostgreSQL is healthy
docker-compose ps postgres

# Wait for PostgreSQL to be ready
docker-compose up -d postgres
sleep 10

# Reinitialize Airflow
docker-compose up airflow-init

# Restart Airflow services
docker-compose restart airflow-webserver airflow-scheduler
```

---

### 8. Docker Compose Version Issues

**Symptom:**
```
ERROR: Version in "./docker-compose.yaml" is unsupported
```

**Solution:**
```bash
# Update Docker Compose
# On Mac with Homebrew:
brew upgrade docker-compose

# On Linux:
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verify version (should be >= 1.27.0)
docker-compose --version
```

---

### 9. Delta Table Corruption

**Symptom:**
```
ProtocolChangedException: The protocol version of the Delta table has been changed by a concurrent update
```

**Solution:**
```bash
# This usually happens with concurrent writes
# Retry the operation

# If persists, you can restore to a previous version
# In your Spark job:
from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "s3a://delta-lake/hotel_bookings/")
deltaTable.restoreToVersion(0)  # Restore to version 0
```

---

### 10. Insufficient Resources

**Symptom:**
Services keep restarting or are unhealthy

**Solution:**
```bash
# Check resource usage
docker stats

# Increase Docker resources in Docker Desktop:
# Settings → Resources → Advanced
# - CPUs: 4+
# - Memory: 8GB+
# - Disk: 20GB+

# Scale down workers if needed
docker-compose up -d --scale spark-worker=1
```

---

## Diagnostic Commands

### Check All Service Health
```bash
docker-compose ps
```

### View All Logs
```bash
docker-compose logs -f
```

### View Specific Service Logs
```bash
docker-compose logs -f airflow-webserver
docker-compose logs -f spark-master
docker-compose logs -f minio
```

### Access Container Shell
```bash
# Airflow
docker-compose exec airflow-webserver bash

# Spark Master
docker-compose exec spark-master bash

# Run Airflow CLI commands
docker-compose exec airflow-webserver airflow dags list
docker-compose exec airflow-webserver airflow tasks list hotel_data_ingestion
```

### Check Network Connectivity
```bash
# From Airflow to MinIO
docker-compose exec airflow-webserver ping minio

# From Airflow to Spark
docker-compose exec airflow-webserver ping spark-master

# From Spark to MinIO
docker-compose exec spark-master ping minio
```

### Test MinIO Access
```bash
docker-compose exec airflow-webserver python << EOF
from minio import Minio

client = Minio('minio:9000', 'minioadmin', 'minioadmin', secure=False)
buckets = client.list_buckets()
for bucket in buckets:
    print(f"Bucket: {bucket.name}")
    objects = client.list_objects(bucket.name, recursive=True)
    for obj in objects:
        print(f"  - {obj.object_name}")
EOF
```

### Verify Spark Connection
```bash
docker-compose exec spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  --class org.apache.spark.examples.SparkPi \
  /opt/bitnami/spark/examples/jars/spark-examples*.jar 10
```

---

## Performance Optimization

### 1. Tune Spark Configuration
Edit `spark/conf/spark-defaults.conf`:
```properties
spark.executor.memory=4g
spark.executor.cores=4
spark.driver.memory=2g
spark.sql.shuffle.partitions=200
```

### 2. Optimize Delta Lake
```properties
spark.databricks.delta.optimizeWrite.enabled=true
spark.databricks.delta.autoCompact.enabled=true
```

### 3. Tune Airflow
In `docker-compose.yaml`:
```yaml
AIRFLOW__CORE__PARALLELISM: 32
AIRFLOW__CORE__DAG_CONCURRENCY: 16
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 8
```

---

## Getting Help

1. **Check Logs**: Always start with logs to identify the root cause
2. **Verify Configuration**: Ensure all config files are properly set
3. **Test Components**: Test each component individually
4. **Resource Check**: Ensure sufficient resources are allocated
5. **Restart Services**: Often resolves transient issues

For more help:
- Delta Lake: https://docs.delta.io/
- Apache Airflow: https://airflow.apache.org/docs/
- Apache Spark: https://spark.apache.org/docs/latest/
- MinIO: https://min.io/docs/
