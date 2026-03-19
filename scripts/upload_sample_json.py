"""
Helper script to upload sample JSON data to MinIO for testing
This will upload the hobse_mumbai.json file to the test bucket
"""

from minio import Minio
import os
import json


def upload_sample_data():
    """Upload sample JSON data to MinIO"""

    # MinIO configuration
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000").replace(
        "http://", ""
    )
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")

    # Initialize MinIO client
    client = Minio(
        minio_endpoint, access_key=access_key, secret_key=secret_key, secure=False
    )

    # Bucket and path configuration
    bucket_name = "test-data-lake"
    country = "india"
    supplier = "hobse"
    object_path = f"raw_input/{country}/{supplier}/hobse_mumbai.json"

    # Create bucket if it doesn't exist
    if not client.bucket_exists(bucket_name):
        print(f"Creating bucket: {bucket_name}")
        client.make_bucket(bucket_name)
    else:
        print(f"Bucket {bucket_name} already exists")

    # Path to the sample JSON file
    # Assuming this script is run from the project root
    sample_file = (
        "/Users/nakul.patil/Documents/hotel-match/input_data/hobse_mumbai.json"
    )

    if not os.path.exists(sample_file):
        print(f"ERROR: Sample file not found at {sample_file}")
        print("Please update the file path in this script")
        return

    # Upload the file
    print(f"\nUploading {sample_file}")
    print(f"To: s3a://{bucket_name}/{object_path}")

    try:
        client.fput_object(
            bucket_name, object_path, sample_file, content_type="application/json"
        )
        print(f"✓ Successfully uploaded to MinIO")

        # Verify upload
        stat = client.stat_object(bucket_name, object_path)
        print(f"\nFile Info:")
        print(f"  Size: {stat.size} bytes")
        print(f"  Content-Type: {stat.content_type}")
        print(f"  Last Modified: {stat.last_modified}")

        print(f"\nYou can now run the DAG with parameters:")
        print(f"  country: {country}")
        print(f"  supplier_name: {supplier}")

    except Exception as e:
        print(f"ERROR: Failed to upload file")
        print(f"Details: {str(e)}")
        raise


if __name__ == "__main__":
    print("=" * 80)
    print("MinIO Sample Data Upload Script")
    print("=" * 80)
    upload_sample_data()
    print("\n" + "=" * 80)
    print("Upload Complete!")
    print("=" * 80)
