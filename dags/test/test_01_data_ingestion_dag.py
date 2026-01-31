"""
DAG 1: Data Ingestion Pipeline
This DAG generates sample hotel booking data and uploads it to MinIO.
"""

from datetime import datetime, timedelta
import json
import io
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio
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


def generate_sample_data(**context):
    """
    Generate sample hotel booking dataset
    """
    import random
    from datetime import datetime, timedelta

    # Generate sample data
    num_records = 1000
    hotels = ['Grand Hotel', 'Ocean View Resort',
              'Mountain Lodge', 'City Center Inn', 'Beach Paradise']
    cities = ['New York', 'Los Angeles', 'Chicago', 'Miami', 'Seattle']
    room_types = ['Standard', 'Deluxe', 'Suite', 'Executive']

    data = []
    base_date = datetime(2024, 1, 1)

    for i in range(num_records):
        booking_date = base_date + timedelta(days=random.randint(0, 365))
        checkin_date = booking_date + timedelta(days=random.randint(1, 30))
        checkout_date = checkin_date + timedelta(days=random.randint(1, 7))

        record = {
            'booking_id': f'BK{i+1:06d}',
            'hotel_name': random.choice(hotels),
            'city': random.choice(cities),
            'room_type': random.choice(room_types),
            'booking_date': booking_date.strftime('%Y-%m-%d'),
            'checkin_date': checkin_date.strftime('%Y-%m-%d'),
            'checkout_date': checkout_date.strftime('%Y-%m-%d'),
            'num_guests': random.randint(1, 4),
            'total_amount': round(random.uniform(100, 1000), 2),
            'is_cancelled': random.choice([True, False]),
            'customer_id': f'CUST{random.randint(1, 500):05d}'
        }
        data.append(record)

    df = pd.DataFrame(data)

    # Save to local file temporarily
    local_file_path = '/opt/data/hotel_bookings_raw.csv'
    df.to_csv(local_file_path, index=False)

    print(f"Generated {len(df)} records")
    print(f"Sample data:\n{df.head()}")

    return local_file_path


def upload_to_minio(**context):
    """
    Upload the generated data to MinIO
    """
    # Get MinIO credentials from environment
    minio_endpoint = os.getenv(
        'MINIO_ENDPOINT', 'minio:9000').replace('http://', '')
    access_key = os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')

    # Initialize MinIO client
    client = Minio(
        minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )

    # Bucket name
    bucket_name = 'test-data-lake'

    # Get the file path from the previous task
    ti = context['ti']
    local_file_path = ti.xcom_pull(task_ids='generate_data')

    # Upload to MinIO
    object_name = 'raw/hotel_bookings/hotel_bookings_raw.csv'

    try:
        client.fput_object(
            bucket_name,
            object_name,
            local_file_path,
            content_type='text/csv'
        )
        print(f"Successfully uploaded {object_name} to {bucket_name}")

        # Also upload as JSON for variety
        df = pd.read_csv(local_file_path)
        json_data = df.to_json(orient='records', lines=True)

        json_bytes = json_data.encode('utf-8')
        json_stream = io.BytesIO(json_bytes)

        json_object_name = 'raw/hotel_bookings/hotel_bookings_raw.json'
        client.put_object(
            bucket_name,
            json_object_name,
            json_stream,
            length=len(json_bytes),
            content_type='application/json'
        )
        print(f"Successfully uploaded {json_object_name} to {bucket_name}")

    except Exception as e:
        print(f"Error uploading to MinIO: {str(e)}")
        raise

    return f"s3a://{bucket_name}/{object_name}"


# Define the DAG
with DAG(
    'test_hotel_data_ingestion',
    default_args=default_args,
    description='[TEST] Ingest hotel booking data to MinIO',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'ingestion', 'minio', 'data-lake'],
) as dag:

    # Task 1: Generate sample data
    generate_task = PythonOperator(
        task_id='generate_data',
        python_callable=generate_sample_data,
        provide_context=True,
    )

    # Task 2: Upload to MinIO
    upload_task = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio,
        provide_context=True,
    )

    # Set task dependencies
    generate_task >> upload_task
