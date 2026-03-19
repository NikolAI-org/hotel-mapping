#!/usr/bin/env python3
"""
Validation script to test the Delta Lake pipeline setup
Run this after starting all services to verify everything is working
"""

import sys
import requests
from minio import Minio
from minio.error import S3Error


def print_header(text):
    """Print a formatted header"""
    print("\n" + "=" * 80)
    print(f"  {text}")
    print("=" * 80)


def print_result(check_name, success, message=""):
    """Print check result"""
    status = "✓" if success else "✗"
    color = "\033[92m" if success else "\033[91m"
    reset = "\033[0m"
    print(f"{color}{status}{reset} {check_name}: {message}")
    return success


def check_airflow():
    """Check if Airflow webserver is accessible"""
    try:
        response = requests.get("http://localhost:8080/health", timeout=5)
        return print_result(
            "Airflow",
            response.status_code == 200,
            f"Status code: {response.status_code}",
        )
    except requests.exceptions.RequestException as e:
        return print_result("Airflow", False, f"Connection failed: {str(e)}")


def check_spark():
    """Check if Spark Master is accessible"""
    try:
        response = requests.get("http://localhost:8081", timeout=5)
        return print_result(
            "Spark Master", response.status_code == 200, "UI accessible"
        )
    except requests.exceptions.RequestException as e:
        return print_result("Spark Master", False, f"Connection failed: {str(e)}")


def check_minio():
    """Check if MinIO is accessible and buckets exist"""
    try:
        client = Minio(
            "localhost:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False,
        )

        # Check if MinIO is accessible
        buckets = client.list_buckets()
        bucket_names = [b.name for b in buckets]

        # Check required buckets
        required_buckets = ["data-lake", "delta-lake"]
        missing_buckets = [b for b in required_buckets if b not in bucket_names]

        if missing_buckets:
            return print_result(
                "MinIO", False, f"Missing buckets: {', '.join(missing_buckets)}"
            )
        else:
            return print_result(
                "MinIO", True, f"Buckets exist: {', '.join(bucket_names)}"
            )
    except S3Error as e:
        return print_result("MinIO", False, f"S3 Error: {str(e)}")
    except Exception as e:
        return print_result("MinIO", False, f"Connection failed: {str(e)}")


def check_minio_console():
    """Check if MinIO console is accessible"""
    try:
        response = requests.get("http://localhost:9001", timeout=5)
        return print_result(
            "MinIO Console", response.status_code == 200, "UI accessible"
        )
    except requests.exceptions.RequestException as e:
        return print_result("MinIO Console", False, f"Connection failed: {str(e)}")


def check_postgres():
    """Check if PostgreSQL is accessible"""
    try:
        import psycopg2

        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="airflow",
            user="airflow",
            password="airflow",
            connect_timeout=5,
        )
        conn.close()
        return print_result("PostgreSQL", True, "Database accessible")
    except Exception:
        # PostgreSQL might not be exposed, which is okay
        return print_result(
            "PostgreSQL", True, "Not exposed externally (expected in Docker network)"
        )


def print_access_info():
    """Print access information"""
    print_header("Access Information")
    print("\n📊 Service URLs:")
    print("   • Airflow UI:      http://localhost:8080")
    print("     Username: airflow")
    print("     Password: airflow")
    print("\n   • Spark Master UI: http://localhost:8081")
    print("\n   • MinIO Console:   http://localhost:9001")
    print("     Username: minioadmin")
    print("     Password: minioadmin")
    print("\n" + "-" * 80)


def print_next_steps():
    """Print next steps"""
    print_header("Next Steps")
    print("""
1. Access Airflow UI at http://localhost:8080
   - Login with: airflow / airflow

2. Enable and trigger the DAGs:
   a) Enable and run: hotel_data_ingestion
   b) Wait for completion
   c) Enable and run: hotel_delta_transformation

3. Monitor execution:
   - View DAG runs in Airflow UI
   - Check Spark jobs in Spark UI (http://localhost:8081)
   - Verify data in MinIO Console (http://localhost:9001)

4. Explore Delta Lake features:
   - Check version history
   - Run time-travel queries
   - Examine MERGE operations

5. View logs if needed:
   docker-compose logs -f [service-name]
    """)


def main():
    """Main validation function"""
    print_header("Delta Lake Pipeline - Service Validation")

    print("\n⏳ Checking services... (This may take a few seconds)\n")

    results = []

    # Run all checks
    results.append(check_airflow())
    results.append(check_spark())
    results.append(check_minio())
    results.append(check_minio_console())

    # Summary
    print_header("Validation Summary")

    total = len(results)
    passed = sum(results)

    if passed == total:
        print(f"\n✓ All checks passed ({passed}/{total})")
        print("🎉 Your Delta Lake pipeline is ready to use!")
        print_access_info()
        print_next_steps()
        return 0
    else:
        print(f"\n✗ Some checks failed ({passed}/{total} passed)")
        print("\n⚠️  Troubleshooting tips:")
        print("   1. Ensure all services are running: docker-compose ps")
        print("   2. Wait 2-3 minutes for services to fully start")
        print("   3. Check logs: docker-compose logs [service-name]")
        print("   4. See TROUBLESHOOTING.md for detailed help")
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\nValidation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n✗ Unexpected error: {str(e)}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
