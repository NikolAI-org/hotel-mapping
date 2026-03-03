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


# def run_delta_read_job(**kwargs):
#     """
#     Executes spark-submit to read the hotel_pairs Delta table.
#     """
#     spark_submit_cmd = [
#         "/opt/spark/bin/spark-submit",
#         "--master",
#         "spark://spark-master:7077",
#         "--deploy-mode",
#         "client",
#         "--name",
#         "Read-Hotel-Pairs-Delta",
#         # Required JARs for S3 and Delta
#         "--jars",
#         "/opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark-jars/delta-spark_2.12-3.1.0.jar,/opt/spark-jars/delta-storage-3.1.0.jar",
#         # Delta Configurations
#         "--conf",
#         "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
#         "--conf",
#         "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
#         # MinIO / S3 Connectivity
#         "--conf",
#         "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
#         "--conf",
#         "spark.hadoop.fs.s3a.access.key=minioadmin",
#         "--conf",
#         "spark.hadoop.fs.s3a.secret.key=minioadmin",
#         "--conf",
#         "spark.hadoop.fs.s3a.path.style.access=true",
#         "--conf",
#         "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
#         "--conf",
#         "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
#         # Resource Allocation
#         "--executor-memory",
#         "2g",
#         "--executor-cores",
#         "2",
#         # Path to the script we created in Step 1
#         "/opt/airflow/spark/jobs/read_hotel_pairs.py",
#     ]

#     env = os.environ.copy()
#     env["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
#     env["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3"

#     result = subprocess.run(spark_submit_cmd, env=env, capture_output=True, text=True)

#     print(result.stdout)
#     if result.returncode != 0:
#         print(result.stderr)
#         raise Exception(f"Spark job failed with exit code {result.returncode}")


# with DAG(
#     "read_hotel_pairs_delta",
#     default_args=default_args,
#     description="Read hotel pairs from Delta Lake in MinIO",
#     schedule_interval="@daily",
#     catchup=False,
#     tags=["delta", "minio", "hotels"],
# ) as dag:

#     read_delta_task = PythonOperator(
#         task_id="read_hotel_pairs_task", python_callable=run_delta_read_job
#     )

# ----

def run_delta_read_job(**kwargs):
    s3_path = kwargs["params"]["s3_path"]

    spark_submit_cmd = [
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--deploy-mode",
        "client",
        "--name",
        "Read-Delta-Table",
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
        "/opt/airflow/spark/jobs/cluster/read_hotel_pairs.py",
        s3_path,   # 👈 fully dynamic
    ]

    env = os.environ.copy()
    env["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
    env["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3"

    result = subprocess.run(spark_submit_cmd, env=env, capture_output=True, text=True)

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise Exception(f"Spark job failed with exit code {result.returncode}")
    
with DAG(
    "read_hotel_pairs_delta",
    default_args=default_args,
    description="Read Delta Lake table dynamically",
    schedule_interval="@daily",
    catchup=False,
    tags=["delta", "minio", "hotels"],
    params={
        "s3_path": "s3a://data-lake/hotel_data/final_clusters"
    },
) as dag:
    read_delta_task = PythonOperator(
        task_id="read_hotel_pairs_task", python_callable=run_delta_read_job
    )