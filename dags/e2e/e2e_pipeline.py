from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import subprocess

# 1. Define your exact sequence here! EAN must go first to build the base.
SUPPLIERS = ["ean", "bookingcom",
             # "ratehawk",
             # "grnconnect",
             # "hobse",
             ]
COUNTRY = 'uae'

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
}


def run_spark_job_direct(job_type, supplier, **kwargs):
    """
    Run Spark job using spark-submit with cluster deployment
    """
    print(f"Running Spark job: {job_type} for {supplier}")

    # Determine script and job name based on the job_type
    if job_type == "ingestion":
        script_path = '/opt/airflow/spark/jobs/ingestion/run_ingestion_job.py'
        job_name = f'ingest-{supplier.lower()}'
        source_path = f"s3a://data-lake/raw_input/{COUNTRY}/{supplier}/"
        param_key = '--source'
        param_value = source_path
    else:
        script_path = '/opt/airflow/spark/jobs/ingestion/run_scoring_job.py'
        job_name = f'score-{supplier.lower()}'
        param_key = '--supplier'
        param_value = supplier

    # Build spark-submit command mimicking the working scripts
    spark_submit_cmd = [
        '/opt/spark/bin/spark-submit',
        '--master', 'spark://spark-master:7077',
        '--deploy-mode', 'client',
        '--name', job_name,
        '--executor-memory', '16g',
        '--executor-cores', '5',
        '--num-executors', '1',
        '--driver-memory', '3g',
        '--packages',
        'io.delta:delta-spark_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
        '--conf', 'spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension',
        '--conf', 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog',
        '--conf', 'spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem',
        '--conf', 'spark.hadoop.fs.s3a.endpoint=http://minio:9000',
        '--conf', 'spark.hadoop.fs.s3a.access.key=minioadmin',
        '--conf', 'spark.hadoop.fs.s3a.secret.key=minioadmin',
        '--conf', 'spark.hadoop.fs.s3a.path.style.access=true',
        '--conf', 'spark.hadoop.fs.s3a.connection.ssl.enabled=false',
        '--conf', 'spark.executorEnv.PYTHONPATH=/opt/airflow',
        '--conf', 'spark.local.dir=/tmp/spark-tmp',
        '--conf', 'spark.sql.shuffle.partitions=20',
        '--conf', 'spark.sql.adaptive.enabled=true',
        '--conf', 'spark.sql.adaptive.coalescePartitions.enabled=true',
        '--conf', 'spark.memory.fraction=0.65',
        '--conf', 'spark.memory.storageFraction=0.3',
        '--conf', 'spark.memory.offHeap.enabled=true',
        '--conf', 'spark.memory.offHeap.size=4g',
        '--conf', 'spark.speculation=false',
        '--conf', 'spark.task.maxFailures=1',
        '--conf', 'spark.excludeOnFailure.enabled=false',
        '--conf', 'spark.dynamicAllocation.enabled=false',
        '--conf', 'spark.shuffle.io.maxRetries=10',
        '--conf', 'spark.shuffle.io.retryWait=30s',
        '--conf', 'spark.network.timeout=1200s',
        '--conf', 'spark.executor.heartbeatInterval=60s',
        '--conf', 'spark.storage.blockManagerSlaveTimeoutMs=1200000',
        '--conf', 'spark.rpc.askTimeout=1200s',
        '--conf', 'spark.rpc.lookupTimeout=1200s',
        '--conf', 'spark.worker.cleanup.enabled=false',
        '--conf', 'spark.shuffle.service.enabled=false',
        '--conf', 'spark.shuffle.spill.compress=true',
        '--conf', 'spark.shuffle.compress=true',
        '--conf', 'spark.sql.files.maxPartitionBytes=128m',
        '--conf', 'spark.sql.files.maxRecordsPerFile=100000',
        '--conf', 'spark.default.parallelism=20',
        '--conf', 'spark.sql.adaptive.advisoryPartitionSizeInBytes=128m',
        '--conf', 'spark.databricks.delta.optimizeWrite.enabled=false',
        '--conf', 'spark.databricks.delta.retentionDurationCheck.enabled=false',
        # Spark UI Configuration
        '--conf', 'spark.ui.enabled=true',
        '--conf', 'spark.ui.port=4040',
        '--conf', 'spark.ui.reverseProxy=false',
        '--conf', 'spark.ui.reverseProxyUrl=',
        '--conf', 'spark.ui.showConsoleProgress=true',
        '--conf', 'spark.eventLog.enabled=true',
        '--conf', 'spark.eventLog.dir=/tmp/spark-events',
        '--conf', 'spark.eventLog.compress=true',
        '--conf', 'spark.history.ui.port=18080',
        '--conf', 'spark.driver.host=airflow-worker',
        '--conf', 'spark.driver.bindAddress=0.0.0.0',
        '--conf', 'spark.ui.killEnabled=true',
        script_path,
        param_key, param_value
    ]

    print(f"Executing command: {' '.join(spark_submit_cmd)}")

    # Clean up Spark temporary directories in spark-worker container BEFORE job
    import os
    import shutil
    import subprocess as sp

    print("\n" + "="*60)
    print("PRE-JOB CLEANUP")
    print("="*60)
    try:
        # Kill all executors and clean everything
        sp.run(
            ['docker', 'exec', '-u', 'root', 'hotel-mapping-spark-worker-1',
             'sh', '-c', 'pkill -9 -f CoarseGrainedExecutorBackend; sleep 1; rm -rf /tmp/spark-work/* /tmp/spark-tmp/*'],
            capture_output=True,
            text=True,
            timeout=10
        )
        print("✓ Killed old executors and cleaned temp directories")
    except Exception as e:
        print(f"⚠ Cleanup warning: {e}")

    # Check and log disk space before job execution
    import os
    import shutil

    print("\n" + "="*60)
    print("DISK SPACE CHECK - BEFORE JOB")
    print("="*60)

    try:
        stat_result = os.statvfs('/tmp')
        total_space = (stat_result.f_blocks *
                       stat_result.f_frsize) / (1024**3)  # GB
        free_space = (stat_result.f_bavail *
                      stat_result.f_frsize) / (1024**3)   # GB
        used_space = total_space - free_space
        used_percent = (used_space / total_space) * 100

        print(f"Container /tmp filesystem:")
        print(f"  Total: {total_space:.2f} GB")
        print(f"  Used:  {used_space:.2f} GB ({used_percent:.1f}%)")
        print(f"  Free:  {free_space:.2f} GB")
    except Exception as e:
        print(f"Could not check /tmp disk space: {e}")

    # Check spark-tmp directory specifically
    spark_tmp_dirs = ['/tmp/spark-tmp', '/tmp/spark-work']
    for tmp_dir in spark_tmp_dirs:
        if os.path.exists(tmp_dir):
            try:
                # Calculate directory size before cleanup
                total_size = 0
                for dirpath, dirnames, filenames in os.walk(tmp_dir):
                    for f in filenames:
                        fp = os.path.join(dirpath, f)
                        if os.path.exists(fp):
                            total_size += os.path.getsize(fp)

                size_mb = total_size / (1024**2)
                print(f"\n{tmp_dir}:")
                print(f"  Size before cleanup: {size_mb:.2f} MB")

                # Clean up old files
                for item in os.listdir(tmp_dir):
                    item_path = os.path.join(tmp_dir, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path, ignore_errors=True)
                    else:
                        os.remove(item_path)
                print(f"  Status: Cleaned up successfully")
            except Exception as e:
                print(f"  Warning: Could not clean {tmp_dir}: {e}")

    # Check actual shared memory allocation
    try:
        shm_stat = os.statvfs('/dev/shm')
        shm_total = (shm_stat.f_blocks * shm_stat.f_frsize) / (1024**3)  # GB
        shm_free = (shm_stat.f_bavail * shm_stat.f_frsize) / (1024**3)   # GB
        print(f"\nShared memory (/dev/shm):")
        print(f"  Total: {shm_total:.2f} GB")
        print(f"  Free:  {shm_free:.2f} GB")
    except Exception as e:
        print(f"  Could not check /dev/shm: {e}")

    # Check tmpfs
    try:
        tmpfs_stat = os.statvfs('/dev')
        tmpfs_total = (tmpfs_stat.f_blocks *
                       tmpfs_stat.f_frsize) / (1024**2)  # MB
        print(f"\nTmpfs (/dev): {tmpfs_total:.0f} MB")
    except Exception as e:
        print(f"  Could not check /dev: {e}")

    print("="*60 + "\n")

    # Set PYTHONPATH for the driver (client mode)
    env = os.environ.copy()
    env['PYTHONPATH'] = '/opt/airflow'

    result = subprocess.run(
        spark_submit_cmd,
        capture_output=True,
        text=True,
        env=env
    )

    print("STDOUT:")
    print(result.stdout)

    if result.stderr:
        print("STDERR:")
        print(result.stderr)

    if result.returncode != 0:
        raise Exception(
            f"Spark job failed with return code {result.returncode}")

    print(f"\nSpark job {job_name} completed successfully")
    return 0


with DAG(
        'hotel_master_sequential_pipeline',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
) as dag:
    start_pipeline = EmptyOperator(task_id="start_pipeline")
    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Set our tracker to the start
    previous_task = start_pipeline

    # Dynamically chain them SEQUENTIALLY
    for supplier in SUPPLIERS:
        # Ingestion Task
        ingest_task = PythonOperator(
            task_id=f"ingest_flatten_{supplier.lower()}",
            python_callable=run_spark_job_direct,
            # We use op_kwargs to safely pass parameters into the Python function
            op_kwargs={'job_type': 'ingestion', 'supplier': supplier}
        )

        # Scoring Task
        scoring_task = PythonOperator(
            task_id=f"score_against_base_{supplier.lower()}",
            python_callable=run_spark_job_direct,
            op_kwargs={'job_type': 'scoring', 'supplier': supplier}
        )

        # 1. Connect the previous supplier's finish line to this supplier's start line
        previous_task >> ingest_task

        # 2. Connect the current supplier's ingestion to its scoring
        ingest_task >> scoring_task

        # 3. Update the pointer so the next loop waits for this scoring_task to finish
        previous_task = scoring_task

    # Connect the very last scoring task to the end
    previous_task >> end_pipeline
