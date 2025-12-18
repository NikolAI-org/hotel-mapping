#!/usr/bin/env python3

import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from hotel_data.config.paths import DERBY_HOME, WAREHOUSE_DIR

Path(DERBY_HOME).mkdir(parents=True, exist_ok=True)


def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("HotelsPipelineRead")
        .master("local[*]")

        # Delta
        .config(
            "spark.jars.packages",
            ",".join([
                "io.delta:delta-spark_2.13:4.0.0",
                "org.apache.hadoop:hadoop-aws:3.4.1",
            ])
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )

        # Warehouse (Spark + Hive)
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        .config("hive.metastore.warehouse.dir", WAREHOUSE_DIR)

        # Hive metastore (Derby)
        .config(
            "javax.jdo.option.ConnectionURL",
            f"jdbc:derby:{DERBY_HOME}/metastore_db;create=true"
        )
        .config(
            "javax.jdo.option.ConnectionDriverName",
            "org.apache.derby.jdbc.EmbeddedDriver"
        )
        .config("datanucleus.schema.autoCreateAll", "true")

        # Catalog implementation
        .config("spark.sql.catalogImplementation", "hive")

        # S3 / MinIO
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.1.4:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark



if __name__ == "__main__":
    spark = create_spark()

    print("\nSpark Interactive Session Started")
    print(f"Spark Version : {spark.version}")
    print(f"Master        : {spark.sparkContext.master}")
    print("\nObjects available:")
    print("  spark  -> SparkSession")
    print("  F      -> pyspark.sql.functions")
    print("\nExample:")
    print("  spark.sql('SHOW DATABASES').show()\n")

    # Drop into interactive Python shell
    import code
    code.interact(
        local={
            "spark": spark,
            "F": F
        }
    )
