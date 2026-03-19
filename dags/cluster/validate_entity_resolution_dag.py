from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import subprocess

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def run_delta_validation_job(**kwargs):
    audit_s3_path = kwargs["params"]["audit_s3_path"]
    clusters_s3_path = kwargs["params"]["clusters_s3_path"]

    spark_submit_cmd = [
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--deploy-mode",
        "client",
        "--name",
        "Validate-Entity-Resolution",
        "--jars",
        "/opt/spark-jars/hadoop-aws-3.3.4.jar,"
        "/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar,"
        "/opt/spark-jars/delta-spark_2.12-3.1.0.jar,"
        "/opt/spark-jars/delta-storage-3.1.0.jar",
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
        "2g",
        "--executor-cores",
        "2",
        # Path to the new validation script
        "/opt/airflow/spark/jobs/cluster/validate_entity_resolution.py",
        audit_s3_path,  # Arg 1 for the script
        clusters_s3_path,  # Arg 2 for the script
    ]

    env = os.environ.copy()
    env["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
    env["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3"

    result = subprocess.run(spark_submit_cmd, env=env, capture_output=True, text=True)

    # Print STDOUT so we can see the validation logs in Airflow UI
    if result.stdout:
        print(result.stdout)

    if result.returncode != 0:
        print("--- SPARK STDERR ---")
        print(result.stderr)
        raise Exception(
            f"Validation job failed. Check logs for Data Quality errors. Exit code: {result.returncode}"
        )


with DAG(
    "validate_entity_resolution_delta",
    default_args=default_args,
    description="Validates Delta Lake tables after clustering",
    schedule_interval="@daily",
    catchup=False,
    tags=["delta", "minio", "hotels", "dq"],
    params={
        "audit_s3_path": "s3a://delta-lake/bronze/comparison_audit_log",
        "clusters_s3_path": "s3a://delta-lake/bronze/final_clusters",
    },
) as dag:
    validate_delta_task = PythonOperator(
        task_id="run_validation_task", python_callable=run_delta_validation_job
    )
