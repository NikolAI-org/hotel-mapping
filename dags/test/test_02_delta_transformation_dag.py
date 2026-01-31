"""
DAG 2: Delta Lake Transformation Pipeline
This DAG reads raw data from MinIO, transforms it, and writes to Delta Lake format.
It also demonstrates Delta Lake time-travel capabilities.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
import os

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def run_spark_job(script_path, job_name):
    """
    Run Spark job using PySpark's Python API (exec approach)
    """
    print(f"Running Spark job: {job_name}")
    print(f"Script: {script_path}")

    # Read and execute the Spark job script
    # The script creates its own SparkSession with the necessary configurations
    with open(script_path, 'r') as f:
        script_content = f.read()

    # Execute the script in a new namespace
    exec(script_content, {'__name__': '__main__'})

    print(f"Spark job {job_name} completed successfully")
    return 0


def run_delta_transform(**context):
    """Run Delta transformation job"""
    return run_spark_job('/opt/airflow/spark/test/jobs/test_delta_transform.py', 'test-delta-transform-job')


def run_delta_merge(**context):
    """Run Delta merge job"""
    return run_spark_job('/opt/airflow/spark/test/jobs/test_delta_merge.py', 'test-delta-merge-job')


def run_delta_timetravel(**context):
    """Run Delta time-travel job"""
    return run_spark_job('/opt/airflow/spark/test/jobs/test_delta_time_travel.py', 'test-delta-timetravel-job')


def log_delta_info(**context):
    """
    Log information about Delta Lake operations
    """
    print("=" * 80)
    print("Delta Lake Transformation Completed Successfully!")
    print("=" * 80)
    print("\nDelta Lake Features Demonstrated:")
    print("1. ACID Transactions - Reliable data writes")
    print("2. Schema Enforcement - Data quality assured")
    print("3. Time Travel - Query historical versions")
    print("4. Upsert Operations - MERGE INTO support")
    print("\nData Location: s3a://delta-lake/hotel_bookings/")
    print("=" * 80)


# Define the DAG
with DAG(
    'test_hotel_delta_transformation',
    default_args=default_args,
    description='[TEST] Transform hotel data using Spark and Delta Lake',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'transformation', 'spark', 'delta-lake'],
) as dag:

    # Task 1: Initial Delta Lake transformation
    transform_to_delta = PythonOperator(
        task_id='transform_to_delta',
        python_callable=run_delta_transform,
        provide_context=True,
    )

    # Task 2: Perform upsert operations (MERGE)
    delta_merge = PythonOperator(
        task_id='delta_merge_operations',
        python_callable=run_delta_merge,
        provide_context=True,
    )

    # Task 3: Time-travel queries
    time_travel_query = PythonOperator(
        task_id='delta_time_travel',
        python_callable=run_delta_timetravel,
        provide_context=True,
    )

    # Task 4: Log completion
    log_completion = PythonOperator(
        task_id='log_completion',
        python_callable=log_delta_info,
        provide_context=True,
    )

    # Set task dependencies
    transform_to_delta >> delta_merge >> time_travel_query >> log_completion
