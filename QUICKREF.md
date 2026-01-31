# Delta Lake Pipeline - Quick Reference

## 🚀 Quick Start Commands

```bash
# Initial setup
./setup.sh

# Or using Make
make setup

# Start services
docker-compose up -d

# Validate setup
python3 validate.py

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

## 📊 Service Endpoints

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| Spark Master UI | http://localhost:8081 | - |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |

## 🔧 Common Commands

### Docker Compose

```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# View service status
docker-compose ps

# View logs (all services)
docker-compose logs -f

# View logs (specific service)
docker-compose logs -f airflow-webserver
docker-compose logs -f spark-master
docker-compose logs -f minio

# Restart a service
docker-compose restart airflow-scheduler

# Scale workers
docker-compose up -d --scale spark-worker=3

# Clean everything (WARNING: deletes all data)
docker-compose down -v --rmi all
```

### Airflow CLI

```bash
# Access Airflow CLI
docker-compose exec airflow-webserver bash

# List DAGs
docker-compose exec airflow-webserver airflow dags list

# Trigger a DAG
docker-compose exec airflow-webserver airflow dags trigger hotel_data_ingestion

# List tasks in a DAG
docker-compose exec airflow-webserver airflow tasks list hotel_data_ingestion

# Test a specific task
docker-compose exec airflow-webserver airflow tasks test hotel_data_ingestion generate_data 2024-01-01
```

### Spark

```bash
# Access Spark Master
docker-compose exec spark-master bash

# Submit a Spark job manually
docker-compose exec spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark-jobs/delta_transform.py

# View Spark configuration
docker-compose exec spark-master cat /opt/bitnami/spark/conf/spark-defaults.conf
```

### MinIO

```bash
# Access MinIO CLI (mc)
docker-compose exec minio-init mc alias set myminio http://minio:9000 minioadmin minioadmin

# List buckets
docker-compose exec minio-init mc ls myminio

# List objects in bucket
docker-compose exec minio-init mc ls myminio/data-lake --recursive

# Download file
docker-compose exec minio-init mc cp myminio/data-lake/raw/hotel_bookings/hotel_bookings_raw.csv /tmp/
```

## 📂 File Locations

```
Inside Containers:
├── /opt/airflow/dags/              # Airflow DAGs
├── /opt/airflow/logs/              # Airflow logs
├── /opt/spark-jobs/                # Spark job scripts
├── /opt/bitnami/spark/conf/        # Spark configuration
└── /opt/data/                      # Local data directory

Host Machine:
├── ./dags/                         # Sync with container
├── ./logs/                         # Sync with container
├── ./spark/jobs/                   # Sync with container
├── ./spark/conf/                   # Sync with container
└── ./data/                         # Sync with container
```

## 🎯 DAG Execution Order

**Step 1:** Run `hotel_data_ingestion`
```
Generates sample data → Uploads to MinIO (s3a://data-lake/raw/)
```

**Step 2:** Run `hotel_delta_transformation`
```
Read CSV → Transform → Write Delta → MERGE → Time Travel Queries
```

## 🔍 Debugging

### Check if services are running
```bash
docker-compose ps
```

### View service health
```bash
# Airflow
curl http://localhost:8080/health

# Spark
curl http://localhost:8081

# MinIO
curl http://localhost:9000/minio/health/live
```

### Test network connectivity
```bash
# From Airflow to Spark
docker-compose exec airflow-webserver ping spark-master

# From Airflow to MinIO
docker-compose exec airflow-webserver ping minio

# From Spark to MinIO
docker-compose exec spark-master ping minio
```

### Test MinIO access from Airflow
```bash
docker-compose exec airflow-webserver python << 'EOF'
from minio import Minio
client = Minio('minio:9000', 'minioadmin', 'minioadmin', secure=False)
print("Buckets:", [b.name for b in client.list_buckets()])
EOF
```

### View Delta table files
```bash
docker-compose exec minio-init mc ls myminio/delta-lake/hotel_bookings/ --recursive
```

## 📈 Monitoring

### Resource usage
```bash
# View container resource usage
docker stats

# View specific container
docker stats airflow-webserver
```

### Disk usage
```bash
# View Docker disk usage
docker system df

# View volume usage
docker volume ls
```

## 🛠️ Troubleshooting

### Services won't start
```bash
# Check logs
docker-compose logs

# Remove and recreate
docker-compose down -v
docker-compose up -d
```

### Permission errors
```bash
# Fix ownership (Linux/Mac)
sudo chown -R $(id -u):$(id -g) ./logs ./plugins ./data
chmod -R 755 ./logs ./plugins ./data

# Set AIRFLOW_UID
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### DAG not appearing
```bash
# Check DAG file syntax
docker-compose exec airflow-webserver python -m py_compile /opt/airflow/dags/01_data_ingestion_dag.py

# Restart scheduler
docker-compose restart airflow-scheduler
```

### Spark job fails
```bash
# Check Spark logs
docker-compose logs spark-master spark-worker

# Verify packages
docker-compose exec spark-master ls -la /opt/bitnami/spark/jars/ | grep delta
```

## 📚 Data Paths

### MinIO Buckets

**data-lake** (raw data):
- `s3a://data-lake/raw/hotel_bookings/hotel_bookings_raw.csv`
- `s3a://data-lake/raw/hotel_bookings/hotel_bookings_raw.json`

**delta-lake** (Delta tables):
- `s3a://delta-lake/hotel_bookings/` (main table)
- `s3a://delta-lake/hotel_bookings_aggregated/` (aggregated metrics)

## 🔐 Default Credentials

```bash
# Airflow
Username: airflow
Password: airflow

# MinIO
Access Key: minioadmin
Secret Key: minioadmin

# PostgreSQL (internal)
User: airflow
Password: airflow
Database: airflow
```

## 💡 Tips

1. **Wait for initialization**: Services need 2-3 minutes to fully start
2. **Check health first**: Run `python3 validate.py` before using
3. **Run DAGs in order**: Ingestion → Transformation
4. **Monitor Spark UI**: Watch job progress at http://localhost:8081
5. **Check logs often**: Use `docker-compose logs -f` for debugging

## 🆘 Getting Help

1. **Check validation**: `python3 validate.py`
2. **View logs**: `docker-compose logs [service]`
3. **Read docs**: See README.md, ARCHITECTURE.md, TROUBLESHOOTING.md
4. **Service status**: `docker-compose ps`
5. **Resource check**: `docker stats`

## 🧹 Cleanup

```bash
# Stop services (keep data)
docker-compose down

# Stop and remove data
docker-compose down -v

# Complete cleanup
docker-compose down -v --rmi all
rm -rf logs/* data/*
```

---

**For detailed information, see:**
- [README.md](README.md) - Full documentation
- [ARCHITECTURE.md](ARCHITECTURE.md) - Technical specifications
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Problem resolution
