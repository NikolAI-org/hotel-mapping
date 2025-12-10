from pathlib import Path
from pyspark.sql import SparkSession

from hotel_data.config.paths import (
    BASE_DELTA_PATH,
    SCHEMA_NAME,
    TABLE_HOTELS_NAME,
    TABLE_HOTELS_PAIRS_NAME,
    WAREHOUSE_DIR,
    DERBY_HOME,
    CATALOG_NAME,
)
from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.schema.delta.hotel_bronze import flattened_hotel_schema

Path(DERBY_HOME).mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder.appName("HotelsPipelineRead")
    .config("spark.jars.packages", ",".join([
        "io.delta:delta-spark_2.13:4.0.0",
        "org.apache.hadoop:hadoop-aws:3.4.1",
    ]))
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
    # ---- S3/MinIO config ----
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", "http://172.16.16.152:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    # ---- SAME Hive metastore as writer ----
    .config(
        "spark.hadoop.javax.jdo.option.ConnectionURL",
        f"jdbc:derby:{DERBY_HOME}/metastore_db;create=true",
    )
    .config(
        "spark.hadoop.javax.jdo.option.ConnectionDriverName",
        "org.apache.derby.jdbc.EmbeddedDriver",
    )
    .config("spark.hadoop.datanucleus.autoCreateSchema", "true")
    .config("spark.hadoop.datanucleus.fixedDatastore", "true")
    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
    .getOrCreate()
)

print("current_catalog:", spark.catalog.currentCatalog())
print("current_database:", spark.catalog.currentDatabase())

# Option A: catalog-style SQL reads (production-like)
# spark.sql("SHOW DATABASES").show(truncate=False)
# spark.sql(f"SHOW TABLES IN {SCHEMA_NAME}").show(truncate=False)

# spark.sql(f"SELECT * FROM {SCHEMA_NAME}.{TABLE_HOTELS_NAME}").show(5, truncate=False)
# spark.sql(f"DESCRIBE TABLE {SCHEMA_NAME}.{TABLE_HOTELS_NAME}").show(truncate=False)

# Option B: use DeltaTableManager
manager = DeltaTableManager(
    spark=spark,
    catalog_name=CATALOG_NAME,
    schema_name=SCHEMA_NAME,
    base_path=BASE_DELTA_PATH,
)

hotel_pair = manager.read_table(TABLE_HOTELS_PAIRS_NAME)
hotel_pair.show(5, truncate=False)

from pyspark.sql.functions import col
df_invalid = hotel_pair.filter(
    (col("normalized_name_score_sbert") < 0) |
    (col("normalized_name_score_sbert") > 1)
)

df_invalid.show(truncate=False)

# Optional fallback: direct path read (escape hatch)
# df_fallback = spark.read.format("delta").load(
#     f"{BASE_DELTA_PATH}/{SCHEMA_NAME}/{TABLE_HOTELS_NAME}"
# )
# df_fallback.show(5)
