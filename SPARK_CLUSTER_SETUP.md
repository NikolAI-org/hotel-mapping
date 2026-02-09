# Spark Cluster Setup - Required Changes Checklist

This document tracks all the critical changes needed for the Spark cluster to work properly with the hotel-mapping DAGs.

## ✅ Completed Code Changes

### 1. Spark Configuration (spark-defaults.conf)
**Status:** ✅ Complete
**File:** `spark/conf/spark-defaults.conf`
**Changes:**
- Added Java 21 compatibility JVM flags for driver and executor
- Flags: `--add-exports=java.base/sun.nio.ch=ALL-UNNAMED` and multiple `--add-opens` flags
- Reduced executor memory from 7g to 4g to fit within worker capacity
- Reduced executor cores from 6 to 4

### 2. Docker Compose Configuration
**Status:** ✅ Complete  
**File:** `docker-compose.yaml`
**Changes:**
- Added `SPARK_WORKER_DIR=/tmp/spark-work` environment variable to spark-worker
- Added `spark-work` persistent volume for worker executor directories
- Volume mounts for `/tmp/spark-tmp` and `/tmp/spark-work`

### 3. Spark Docker Image
**Status:** ✅ Complete
**File:** `Dockerfile.spark`
**Changes:**
- Added volume directory creation with proper spark user ownership:
  ```dockerfile
  RUN mkdir -p /tmp/spark-tmp /tmp/spark-work && \
      chown -R spark:spark /tmp/spark-tmp /tmp/spark-work && \
      chmod -R 775 /tmp/spark-tmp /tmp/spark-work
  ```

### 4. Python Dependencies
**Status:** ✅ Complete
**File:** `requirements.txt`
**Changes:**
- Updated PySpark from 3.5.0 to 3.5.3 to match Spark cluster version
- Ensures serialVersionUID compatibility between driver and executor

### 5. Airflow Dockerfile
**Status:** ✅ Complete
**File:** `Dockerfile`
**Changes:**
- Removed `PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH` to avoid using old PySpark from /opt/spark
- Added comment explaining we use pip-installed PySpark for version consistency
- Installs PySpark 3.5.3 via pip

### 6. DAG Configuration
**Status:** ✅ Complete
**File:** `dags/mapper/map_raw_json_country_and_supplier.py`
**Changes:**
- Reduced executor memory: `7g` → `4g`
- Reduced executor cores: `6` → `4`  
- Reduced total executor cores: `6` → `4`
- Reduced parallelism: `12` → `8`
- Reduced shuffle partitions: `12` → `8`
- Uses client deploy mode (cluster mode not supported for PySpark on standalone)

### 7. Setup Script
**Status:** ✅ Complete
**File:** `setup.sh`
**Changes:**
- Added automatic Spark binary copy from spark-master to all Airflow containers
- Removes `/opt/spark/python` directory after copy to use pip-installed PySpark
- Runs after services start to ensure spark-master is available

## 🔧 Runtime Steps (Automated in setup.sh)

These steps are now automated in `setup.sh` but are documented here for reference:

1. **Copy Spark binaries:** Copies /opt/spark from spark-master to all Airflow containers
2. **Remove Python directory:** Deletes /opt/spark/python to avoid version conflicts
3. **Fix permissions:** Ensures spark user owns /tmp/spark-tmp and /tmp/spark-work

## 📋 Pre-Flight Checklist (Before First Run)

1. ✅ Docker has sufficient resources:
   - CPUs: 6 cores
   - Memory: 31GB RAM
   - Disk: 60GB+ available

2. ✅ All configuration files are in place:
   - spark/conf/spark-defaults.conf (with Java 21 flags)
   - docker-compose.yaml (with volumes and env vars)
   - Dockerfile.spark (with volume setup)
   - requirements.txt (PySpark 3.5.3)

3. ✅ Run setup: `./setup.sh`
   - Builds images
   - Starts services
   - Copies Spark binaries
   - Validates setup

## 🎯 Validation Steps

After running `setup.sh`, verify:

```bash
# 1. Check PySpark versions match
docker exec hotel-mapping-airflow-worker-1 python -c "import pyspark; print('Airflow:', pyspark.__version__)"
docker exec hotel-mapping-spark-worker-1 python -c "import pyspark; print('Spark:', pyspark.__version__)"
# Both should show: 3.5.3

# 2. Check Spark binaries exist
docker exec hotel-mapping-airflow-worker-1 ls -la /opt/spark/bin/spark-submit
# Should exist

# 3. Check volume permissions
docker exec hotel-mapping-spark-worker-1 ls -la /tmp/spark-work
# Should be owned by spark user

# 4. Check cluster is running
docker logs hotel-mapping-spark-master-1 --tail 5
# Should show "Master" running

# 5. Trigger test DAG
docker exec hotel-mapping-airflow-worker-1 airflow dags trigger map_raw_json_country_and_supplier
```

## 🐛 Troubleshooting

### Issue: serialVersionUID mismatch
**Solution:** PySpark versions must match exactly (3.5.3). Run setup.sh to copy fresh Spark binaries.

### Issue: "No subfolder can be created"
**Solution:** Volume permissions issue. setup.sh should fix this, or manually:
```bash
docker exec -u root hotel-mapping-spark-worker-1 chown -R spark:spark /tmp/spark-tmp /tmp/spark-work
```

### Issue: "Cluster deploy mode not supported for python"
**Solution:** PySpark on Spark Standalone only supports client mode. DAG already uses client mode.

### Issue: "App requires more resource than any Workers could have"
**Solution:** Executor memory + driver memory > worker memory. DAG now uses 4g executor + 2g driver = 6g total (fits in 8g worker).

## 📦 Summary

**All changes are now in code files.** Running `./setup.sh` will:
1. Build images with all necessary configurations
2. Start services  
3. Copy Spark binaries and fix permissions automatically
4. Validate the setup

No manual steps required after running setup.sh!
