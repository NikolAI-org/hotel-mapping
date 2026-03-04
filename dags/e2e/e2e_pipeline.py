from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import subprocess

# 1. Define your exact sequence here! EAN must go first to build the base.
SUPPLIERS = ["ean", "bookingcom",
             "grnconnect",
             "hobse",
             "ratehawk"
             ]
# SUPPLIERS = ["hobse", "grnconnect", "expedia" ]
COUNTRY = 'india'

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
        '--executor-memory', '2g',  # Give the worker nodes 2GB of RAM
        '--executor-cores', '1',  # STRICTLY 1 core so it only loads 1 PyTorch model!
        '--driver-memory', '2g',
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
        script_path,
        param_key, param_value
    ]

    print(f"Executing command: {' '.join(spark_submit_cmd)}")

    result = subprocess.run(
        spark_submit_cmd,
        capture_output=True,
        text=True
    )

    print("STDOUT:")
    print(result.stdout)

    if result.stderr:
        print("STDERR:")
        print(result.stderr)

    if result.returncode != 0:
        raise Exception(f"Spark job failed with return code {result.returncode}")

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