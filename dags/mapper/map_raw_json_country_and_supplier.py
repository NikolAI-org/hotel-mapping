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
    country = params.get("country", "india")
    supplier_name = params.get("supplier_name", "expedia")

    source_path = f"s3a://data-lake/raw_input/{country}/{supplier_name}/"

    print(f"Parameters - Country: {country}, Supplier: {supplier_name}")
    print(f"Ingesting Source: {source_path}")

    # Build spark-submit command
    spark_submit_cmd = [
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--deploy-mode",
        "client",
        "--name",
        f"MapJSON-{country}-{supplier_name}",
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
        "/opt/airflow/spark/jobs/ingestion/run_ingestion_job.py",
        "--source",
        source_path,
    ]

    # Set environment variables
    env = os.environ.copy()
    env["COUNTRY"] = country
    env["SUPPLIER_NAME"] = supplier_name
    env["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
    env["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3"

    print("\nExecuting spark-submit...")
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


def map_json_to_parquet(**context):
    """Run JSON to Parquet mapping job"""
    return run_spark_job_direct("map-json-to-parquet-job", **context)


def log_completion(**context):
    """
    Log information about the mapping operation
    """
    params = context["params"]
    country = params.get("country", "india")
    supplier_name = params.get("supplier_name", "expedia")

    print("=" * 80)
    print("JSON to Parquet Mapping Completed Successfully!")
    print("=" * 80)
    print("\nParameters:")
    print(f"  Country: {country}")
    print(f"  Supplier: {supplier_name}")
    print(f"\nSource Path: s3a://data-lake/raw_input/{country}/{supplier_name}/")
    print(f"Target Path: s3a://data-lake/mapped_input/{country}/{supplier_name}/")
    print("\nSchema Applied:")
    print("  - Hotel ID, Name, Provider Information")
    print("  - Geo Coordinates (lat, long)")
    print("  - Contact Details (address, phones, emails)")
    print("  - Hotel Attributes (type, category, star rating)")
    print("=" * 80)


# Define the DAG
with DAG(
    "map_raw_json_country_and_supplier",
    default_args=default_args,
    description="Map raw JSON hotel data to Parquet format by country and supplier",
    schedule_interval=None,
    catchup=False,
    tags=["mapping", "json", "parquet", "hotel-data"],
    params={
        "country": Param(
            default="india",
            type="string",
            description="Country name (e.g., india, usa, uk)",
        ),
        "supplier_name": Param(
            default="expedia",
            type="string",
            description="Supplier name (e.g., expedia, hobse, booking)",
        ),
    },
) as dag:
    # Task 1: Map JSON to Parquet
    map_task = PythonOperator(
        task_id="map_json_to_parquet",
        python_callable=map_json_to_parquet,
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
