import json
import os
import select
import subprocess
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# 1. Define your exact sequence here! EAN must go first to build the base.
SUPPLIERS = [
    "ean",
    # "hotelbeds",
    "bookingcom",
    "ratehawk",
    "grnconnect",
    "hobse",
]
# SUPPLIERS = ["hobse", "grnconnect", "expedia" ]
COUNTRY = "india"
# COUNTRY = 'mumbai'

# Keep clustering defaults aligned with cluster DAG behavior.
CLUSTER_CONFIG = {
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
    "transitivity": True,
}

DEFAULT_MATCH_LOGIC = {
    "operator": "AND",
    "rules": [
        {"signal": "geo_distance_km", "threshold": 0.5, "comparator": "lte"},
        {
            "operator": "OR",
            "rules": [
                {"signal": "name_score_jaccard", "threshold": 0.9, "comparator": "gte"},
                {"signal": "name_score_lcs", "threshold": 0.9, "comparator": "gte"},
                {
                    "signal": "name_score_levenshtein",
                    "threshold": 0.9,
                    "comparator": "gte",
                },
                {"signal": "name_score_sbert", "threshold": 0.9, "comparator": "gte"},
                {
                    "operator": "AND",
                    "rules": [
                        {
                            "signal": "name_score_jaccard",
                            "threshold": 0.75,
                            "comparator": "gte",
                        },
                        {
                            "signal": "normalized_name_score_jaccard",
                            "threshold": 0.9,
                            "comparator": "gte",
                        },
                    ],
                },
                {
                    "operator": "AND",
                    "rules": [
                        {
                            "signal": "name_score_lcs",
                            "threshold": 0.75,
                            "comparator": "gte",
                        },
                        {
                            "signal": "normalized_name_score_lcs",
                            "threshold": 0.9,
                            "comparator": "gte",
                        },
                    ],
                },
                {
                    "operator": "AND",
                    "rules": [
                        {
                            "signal": "name_score_levenshtein",
                            "threshold": 0.75,
                            "comparator": "gte",
                        },
                        {
                            "signal": "normalized_name_score_levenshtein",
                            "threshold": 0.9,
                            "comparator": "gte",
                        },
                    ],
                },
                {
                    "operator": "AND",
                    "rules": [
                        {
                            "signal": "name_score_sbert",
                            "threshold": 0.75,
                            "comparator": "gte",
                        },
                        {
                            "signal": "normalized_name_score_sbert",
                            "threshold": 0.9,
                            "comparator": "gte",
                        },
                    ],
                },
            ],
        },
        {
            "operator": "OR",
            "rules": [
                {
                    "signal": "address_line1_score",
                    "threshold": 0.2,
                    "comparator": "gte",
                },
                {
                    "signal": "address_sbert_score",
                    "threshold": 0.2,
                    "comparator": "gte",
                },
            ],
        },
        {"signal": "star_ratings_score", "threshold": 0.0, "comparator": "gte"},
        {
            "operator": "OR",
            "rules": [
                {"signal": "postal_code_match", "threshold": 0.5, "comparator": "gte"},
                {"signal": "geo_distance_km", "threshold": 0.1, "comparator": "lte"},
            ],
        },
        {"signal": "country_match", "threshold": 0.5, "comparator": "gte"},
        {
            "operator": "OR",
            "rules": [
                {"signal": "phone_match_score", "threshold": 0.5, "comparator": "gte"},
                {"signal": "email_match_score", "threshold": 0.5, "comparator": "gte"},
                {"signal": "fax_match_score", "threshold": 0.5, "comparator": "gte"},
            ],
        },
    ],
}

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
}


def run_spark_job_direct(job_type, supplier, **kwargs):
    """
    Run Spark job using spark-submit with cluster deployment
    """
    print(f"Running Spark job: {job_type} for {supplier}")

    # Determine script, job name, and parameters based on job_type
    spark_env = None
    if job_type == "ingestion":
        script_path = "/opt/airflow/spark/jobs/ingestion/run_ingestion_job.py"
        job_name = f"ingest-{supplier.lower()}"
        source_path = f"s3a://data-lake/raw_input/{COUNTRY}/{supplier}/"
        param_key = "--source"
        param_value = source_path
    elif job_type == "scoring":
        script_path = "/opt/airflow/spark/jobs/ingestion/run_scoring_job.py"
        job_name = f"score-{supplier.lower()}"
        param_key = "--supplier"
        param_value = supplier
    elif job_type == "clustering":
        script_path = "/opt/airflow/spark/jobs/cluster/entity_resolution_job.py"
        job_name = f"cluster-{supplier.lower()}"
        param_key = None
        param_value = None
        spark_env = dict(
            os.environ,
            PROVIDER_NAME=supplier,
            WEIGHTS=json.dumps(CLUSTER_CONFIG["weights"]),
            THRESHOLD_HIGH=str(CLUSTER_CONFIG["threshold_high"]),
            THRESHOLD_LOW=str(CLUSTER_CONFIG["threshold_low"]),
            MATCH_LOGIC=json.dumps(DEFAULT_MATCH_LOGIC),
            TRANSITIVITY=json.dumps(CLUSTER_CONFIG["transitivity"]),
        )
    else:
        raise ValueError(f"Unsupported job_type: {job_type}")

    # Ensure hotel_data package is importable by both the driver (running in
    # this airflow-worker process) and the executors (running in spark-worker).
    # The module lives at /opt/airflow/hotel_data in all containers.
    if spark_env is None:
        spark_env = dict(os.environ)
    spark_env.setdefault("PYTHONPATH", "/opt/airflow")

    # Per-job-type resource sizing (worker has 8 cores / 22g):
    #   ingestion : no PyTorch — small executors so all 8 cores can be used
    #               3g + 10% overhead = 3.3g → floor(22/3.3)=6 executors, 6 cores active
    #   scoring   : loads PyTorch/SBERT (~2-3g Python worker) — 1 large executor
    #               12g + 1.2g overhead; file-lock in sbert_vectorizer.py serialises load
    #   clustering: graph algorithm, moderate memory, no heavy Python deps
    #               4g + 10% overhead = 4.4g → floor(22/4.4)=5 executors, 5 cores active
    if job_type == "ingestion":
        executor_memory = os.environ.get("SPARK_INGESTION_MEM", "3g")
        executor_cores = os.environ.get("SPARK_INGESTION_CORES", "1")
    elif job_type == "scoring":
        executor_memory = os.environ.get("SPARK_SCORING_MEM", "12g")
        executor_cores = os.environ.get("SPARK_SCORING_CORES", "1")
    else:  # clustering
        executor_memory = os.environ.get("SPARK_CLUSTERING_MEM", "4g")
        executor_cores = os.environ.get("SPARK_CLUSTERING_CORES", "1")

    # Build spark-submit command mimicking the working scripts
    spark_submit_cmd = [
        "/opt/spark/bin/spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--deploy-mode",
        "client",
        "--name",
        job_name,
        "--executor-memory",
        executor_memory,
        "--executor-cores",
        executor_cores,
        # ROLLBACK: '--driver-memory', '4g',  (unchanged)
        "--driver-memory",
        "4g",
        "--packages",
        "io.delta:delta-spark_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "--conf",
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf",
        "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--conf",
        "spark.hadoop.fs.s3a.endpoint=http://minio:9000",
        "--conf",
        "spark.hadoop.fs.s3a.access.key=minioadmin",
        "--conf",
        "spark.hadoop.fs.s3a.secret.key=minioadmin",
        "--conf",
        "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf",
        "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
        "--conf",
        "spark.executorEnv.PYTHONPATH=/opt/airflow",
        "--conf",
        "spark.local.dir=/tmp/spark-tmp",
        "--conf",
        "spark.sql.shuffle.partitions=12",
        # NOTE: spark.executor.pyspark.memory intentionally omitted — PyTorch needs
        # 2-3g per Python worker; constraining to 1g caused ENOMEM (os error 12)
        # in safetensors safe_open during model load.
        # Force SortShuffleWriter (1 sorted file/mapper) instead of
        # BypassMergeSortShuffleWriter (N files/mapper then merge = 2x peak disk).
        "--conf",
        "spark.shuffle.sort.bypassMergeThreshold=1",
        "--conf",
        "spark.shuffle.compress=true",
        "--conf",
        "spark.shuffle.spill.compress=true",
        # Prevent executors being declared dead during long GC pauses
        "--conf",
        "spark.network.timeout=600s",
        "--conf",
        "spark.executor.heartbeatInterval=60s",
        "--conf",
        "spark.storage.blockManagerSlaveTimeoutMs=600000",
        # Adaptive Query Execution — coalesces shuffle partitions automatically
        "--conf",
        "spark.sql.adaptive.enabled=true",
        "--conf",
        "spark.sql.adaptive.coalescePartitions.enabled=true",
        # G1GC + Java 21 flags are set in spark-defaults.conf so they are not
        # overridden here (passing spark.executor.extraJavaOptions in spark-submit
        # would replace the --add-opens flags, breaking Java 21 compatibility).
        script_path,
    ]

    if param_key and param_value:
        spark_submit_cmd.extend([param_key, param_value])

    print(f"Executing command: {' '.join(spark_submit_cmd)}")

    # Run in a polled loop so this PythonOperator keeps emitting heartbeats/logs
    # and avoids zombie detection for long-running spark-submit tasks.
    proc = subprocess.Popen(
        spark_submit_cmd,
        env=spark_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    heartbeat_every_sec = 60
    while True:
        if proc.stdout is not None:
            ready, _, _ = select.select([proc.stdout], [], [], heartbeat_every_sec)
            if ready:
                line = proc.stdout.readline()
                if line:
                    print(line.rstrip())

        rc = proc.poll()
        if rc is not None:
            if proc.stdout is not None:
                for line in proc.stdout:
                    print(line.rstrip())
            if rc != 0:
                raise Exception(f"Spark job failed with return code {rc}")
            break

        # print(f"Spark job '{job_name}' still running...")

    print(f"\nSpark job {job_name} completed successfully")
    return 0


with DAG(
    "hotel_master_sequential_pipeline",
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
            op_kwargs={"job_type": "ingestion", "supplier": supplier},
        )

        # Scoring Task
        scoring_task = PythonOperator(
            task_id=f"score_against_base_{supplier.lower()}",
            python_callable=run_spark_job_direct,
            op_kwargs={"job_type": "scoring", "supplier": supplier},
        )

        # Clustering Task
        clustering_task = PythonOperator(
            task_id=f"cluster_entities_{supplier.lower()}",
            python_callable=run_spark_job_direct,
            op_kwargs={"job_type": "clustering", "supplier": supplier},
        )

        # 1. Connect the previous supplier's finish line to this supplier's start line
        previous_task >> ingest_task

        # 2. Connect the current supplier's ingestion to its scoring
        ingest_task >> scoring_task

        # 3. Connect scoring to clustering
        scoring_task >> clustering_task

        # 4. Update pointer so next supplier waits for clustering completion
        previous_task = clustering_task

    # Connect the very last clustering task to the end
    previous_task >> end_pipeline
