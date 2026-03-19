"""
DAG: Map Raw JSON Country and Supplier Data
This DAG reads raw JSON hotel data from MinIO based on country and supplier parameters,
applies schema mapping, and writes to Parquet format.

Parameters:
- country: Country name (e.g., 'india')
- supplier_name: Supplier name (e.g., 'expedia', 'hobse')
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Param
import os

# Default arguments for the DAG
default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_spark_job_direct(job_name, **kwargs):
    """
    Run Spark job using spark-submit with cluster deployment
    """
    import subprocess

    print(f"Running Spark job: {job_name}")

    # Get DAG parameters
    params = kwargs["params"]
    supplier_name = params.get("supplier_name", "expedia")

    print(f"Parameters - Supplier: {supplier_name}")

    # Build spark-submit command
    spark_submit_cmd = [
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--deploy-mode",
        "client",
        "--name",
        f"Pair-Scorer-{supplier_name}",
        "--jars",
        "/opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark-jars/delta-spark_2.12-3.1.0.jar,/opt/spark-jars/delta-storage-3.1.0.jar",
        "--conf",
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf",
        "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
        "--conf",
        "spark.hadoop.fs.s3a.access.key=minioadmin",
        "--conf",
        "spark.hadoop.fs.s3a.secret.key=minioadmin",
        "--conf",
        "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf",
        "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--conf",
        "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
        "--executor-memory",
        "4g",
        "--executor-cores",
        "4",
        "--driver-memory",
        "2g",
        "--total-executor-cores",
        "4",
        "--conf",
        "spark.default.parallelism=8",
        "--conf",
        "spark.sql.shuffle.partitions=8",
        "--conf",
        "spark.cores.max=4",
        "/opt/airflow/spark/jobs/ingestion/run_scoring_job.py",
        "--supplier",
        supplier_name,
    ]

    # Set environment variables
    env = os.environ.copy()
    env["SUPPLIER_NAME"] = supplier_name
    env["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
    env["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3"

    print(f"\nExecuting spark-submit...")
    result = subprocess.run(spark_submit_cmd, env=env, capture_output=True, text=True)

    # Print output
    print("\n--- Spark Job Output ---")
    print(result.stdout)

    if result.stderr:
        print("\n--- Spark Job Errors/Warnings ---")
        print(result.stderr)

    # Check return code
    if result.returncode != 0:
        raise Exception(f"Spark job failed with return code {result.returncode}")

    print(f"\nSpark job {job_name} completed successfully")
    return 0


def supplier_scorer(**context):
    """Run JSON to Parquet mapping job"""
    return run_spark_job_direct("supplier-scorer-job", **context)


def log_completion(**context):
    """
    Log information about the mapping operation
    """
    params = context["params"]
    supplier_name = params.get("supplier_name", "expedia")

    print("=" * 80)
    print("Scoring Completed Successfully!")
    print("=" * 80)
    print(f"\nParameters:")
    print(f"  Supplier: {supplier_name}")


# Define the DAG
with DAG(
    "supplier_scorer",
    default_args=default_args,
    description="Create Pairs and score them",
    schedule_interval=None,
    catchup=False,
    tags=["blocking", "pais", "scoring", "hotel-data"],
    params={
        "supplier_name": Param(
            default="expedia",
            type="string",
            description="Supplier name (e.g., expedia, hobse, booking)",
        ),
    },
) as dag:
    # Task 1: Create hotel pairs and scores
    map_task = PythonOperator(
        task_id="supplier_scorer",
        python_callable=supplier_scorer,
        provide_context=True,
    )

    # Task 2: Log completion
    log_task = PythonOperator(
        task_id="log_completion",
        python_callable=log_completion,
        provide_context=True,
    )

    # Set task dependencies
    map_task >> log_task
