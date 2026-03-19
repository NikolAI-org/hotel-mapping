"""
Airflow Spark Connection Configuration
Run this script to setup the Spark connection in Airflow
"""

from airflow import settings
from airflow.models import Connection
import os


def create_spark_connection():
    """
    Create Spark connection in Airflow
    """
    conn_id = "spark_default"

    # Check if connection already exists
    session = settings.Session()
    existing_conn = (
        session.query(Connection).filter(Connection.conn_id == conn_id).first()
    )

    if existing_conn:
        print(f"Connection '{conn_id}' already exists. Updating...")
        session.delete(existing_conn)
        session.commit()

    # Create new connection
    spark_conn = Connection(
        conn_id=conn_id,
        conn_type="spark",
        host="spark://spark-master",
        port=7077,
        extra={
            "deploy-mode": "client",
            "spark-binary": "spark-submit",
            "namespace": "default",
        },
    )

    session.add(spark_conn)
    session.commit()
    session.close()

    print(f"✓ Spark connection '{conn_id}' created successfully!")


if __name__ == "__main__":
    create_spark_connection()
