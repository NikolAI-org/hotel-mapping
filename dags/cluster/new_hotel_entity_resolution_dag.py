from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Param
import json
import subprocess
import os

# This configuration allows the user to map ANY schema field to a weight
DEFAULT_CONFIG = {
    "weights": {
        "name_score_jaccard": 0.1,
        "normalized_name_score_jaccard": 0.1,
        "name_score_lcs": 0.1,
        "normalized_name_score_lcs": 0.1,
        "name_score_levenshtein": 0.1,
        "normalized_name_score_levenshtein": 0.1,
        "name_score_sbert": 0.1,
        "normalized_name_score_sbert": 0.1,
        "address_line1_score": 0.1,
        "address_sbert_score": 0.1,
        "star_ratings_score": 0.0,
        "postal_code_match": 0.0,
        "phone_match_score": 0.0,
        "email_match_score": 0.0,
        "fax_match_score": 0.0,
    },
    "threshold_high": 0.85,
    "threshold_low": 0.80,
    "provider_name": "hobse",  # Default provider
}


def run_clustering_step(**context):
    params = context["params"]

    # Building the CLI command
    # cmd = [
    #     "/opt/spark/bin/spark-submit",
    #     "--master",
    #     "spark://spark-master:7077",
    #     "--conf",
    #     "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    #     "--conf",
    #     "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
    #     "/opt/airflow/spark/jobs/cluster/cluster_logic.py",
    # ]

    # cmd = [
    #     "/opt/spark/bin/spark-submit",
    #     "--master",
    #     "spark://spark-master:7077",
    #     "--deploy-mode",
    #     "client",
    #     # Use --packages to ensure all Delta dependencies are pulled correctly
    #     "--packages",
    #     "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4",
    #     # Keep jars for the AWS SDK if it's not bundled in hadoop-aws
    #     "--jars",
    #     "/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar",
    #     "--conf",
    #     "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    #     "--conf",
    #     "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
    #     "/opt/airflow/spark/jobs/cluster/entity_resolution_job.py",
    # ]
    
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--deploy-mode", "client",
        "--packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4",
        "--jars", "/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        
        # --- ADD THESE S3A CONFIGS HERE ---
        "--conf", "spark.hadoop.fs.s3a.access.key=minioadmin",
        "--conf", "spark.hadoop.fs.s3a.secret.key=minioadmin",
        "--conf", "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false", # If MinIO is on HTTP
        
        "/opt/airflow/spark/jobs/cluster/entity_resolution_job.py",
    ]

    # Passing dynamic field names and weights as env vars
    env = os.environ.copy()
    env["WEIGHTS"] = json.dumps(params["weights"])
    env["THRESHOLD_HIGH"] = str(params["threshold_high"])
    env["THRESHOLD_LOW"] = str(params["threshold_low"])
    env["PROVIDER_NAME"] = str(params["provider_name"])

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)

    # Always print output so it appears in the logs regardless of success/fail
    if result.stdout:
        print("--- SPARK STDOUT ---")
        print(result.stdout)

    if result.returncode != 0:
        print("!!! CRITICAL: SPARK JOB FAILED !!!")
        print("--- SPARK STDERR ---")
        print(result.stderr)  # This is where the Python Traceback lives!
        raise Exception(f"Spark Job Failed. Check the STDERR block above for details.")


with DAG(
    "new_hotel_entity_resolution",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    params={
        "weights": Param(DEFAULT_CONFIG["weights"], type="object"),
        "threshold_high": Param(DEFAULT_CONFIG["threshold_high"], type="number"),
        "threshold_low": Param(DEFAULT_CONFIG["threshold_low"], type="number"),
        "provider_name": Param(
            DEFAULT_CONFIG["provider_name"], type="string"
        ),  # Added Param
    },
    render_template_as_native_obj=True,
) as dag:

    execute_clustering = PythonOperator(
        task_id="run_hotel_clustering", python_callable=run_clustering_step
    )
