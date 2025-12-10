from pathlib import Path
from pyspark.sql import SparkSession

from hotel_data.config.paths import BASE_DELTA_PATH, SCHEMA_NAME, TABLE_HOTELS_NAME, WAREHOUSE_DIR
from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.schema.delta.hotel_bronze import flattened_hotel_schema

DERBY_HOME= "/home/akshay/spark_home/spark_derby_metastore"
Path(DERBY_HOME).mkdir(parents=True, exist_ok=True)

spark = (
        SparkSession.builder.appName("HotelsPipelineRead")
        # use EXACTLY the same configs as preprocessing_pipeline.py
        .config("spark.jars.packages", ",".join([
            "io.delta:delta-spark_2.13:4.0.0",
            "org.apache.hadoop:hadoop-aws:3.4.1",
        ]))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://172.16.16.152:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)  # or your S3A path
        .enableHiveSupport()
        .getOrCreate()
    )

# List all catalogs
spark.sql("SHOW CATALOGS").show(truncate=False)


print("current_catalog:", spark.catalog.currentCatalog())          # Spark 3.3+/4.0 API
print("current_database:", spark.catalog.currentDatabase())       # current database in current catalog

# spark.sql("SHOW CATALOGS").show(truncate=False)

# If using a catalog
# spark.sql("SHOW SCHEMAS").show(truncate=False)
# spark.sql("SHOW SCHEMAS IN spark_catalog").show(truncate=False)

# spark.sql("SELECT current_catalog(), current_schema()").show(truncate=False)

# If no catalog (OSS Spark), just use the database name
spark.sql("SHOW DATABASES").show(truncate=False)

spark.sql("SHOW TABLES in bronze").show(truncate=False)

# spark.sql("Select * from spark_catalog.bronze.hotel_pairs").show(truncate=False)
spark.sql("DESCRIBE TABLE spark_catalog.bronze.hotels").show(truncate=False)
spark.sql("DESCRIBE TABLE spark_catalog.bronze.hotel_pairs").show(truncate=False)


# Fallback mechanism to read the data from delta files directly.
# df = spark.read.format("delta").load(f"{BASE_DELTA_PATH}/{SCHEMA_NAME}/{TABLE_HOTELS_NAME}")
# df.show(5)
