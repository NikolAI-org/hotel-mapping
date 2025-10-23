from pyspark.sql import SparkSession
from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.schema.delta.hotel_bronze import flattened_hotel_schema

# spark = SparkSession.builder \
#     .appName("HotelDataPipeline") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.warehouse.dir", "/data/delta") \
#     .enableHiveSupport() \
#     .getOrCreate()
WAREHOUSE_DIR = "s3a://warehouse"
BASE_PATH = "s3a://delta-bucket/hotel_data/delta"

spark = (
        SparkSession.builder.appName("HotelsPipeline")
        # ✅ JARs for Delta + Hadoop AWS + AWS SDK
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "io.delta:delta-spark_2.13:4.0.0",
                    "org.apache.hadoop:hadoop-aws:3.4.1",
                ]
            ),
        )
        # ✅ Delta SQL extensions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ✅ S3A implementation
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://localhost:9000",
        )
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.mkdirs.enabled", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        # .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential")
        # .config("spark.hadoop.fs.s3a.fail.on.empty.path", "false")
        .config(
            "spark.databricks.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        )
        .config("spark.hadoop.fs.s3a.signing-region", "us-east-2") # Must match the signed region in the log
        # .config("spark.hadoop.fs.s3a.signing-algorithm", "AWS4-HMAC-SHA256")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # ✅ S3A timeouts & retries (milliseconds)
        # .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        # .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
        # .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
        # .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        # .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
        # .config("spark.hadoop.fs.s3a.log.events", "true")
        # ✅ Multipart uploads (values in bytes)
        # .config(
        #     "spark.hadoop.fs.s3a.multipart.threshold", str(128 * 1024 * 1024)
        # )  # 128 MB
        # .config("spark.hadoop.fs.s3a.multipart.size", str(64 * 1024 * 1024))  # 64 MB
        # .config("spark.hadoop.fs.s3a.multipart.purge", "false")
        # .config(
        #     "spark.hadoop.fs.s3a.multipart.purge.age", str(24 * 60 * 60 * 1000)
        # )  # 24h in ms
        # .config(
        #     "spark.hadoop.fs.s3a.multipart.purge.age.seconds", str(24 * 60 * 60)
        # )  # 24h in seconds
        # ✅ Delta + Hive compatibility
        # .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        # .config("spark.sql.catalogImplementation", "hive")
        # .enableHiveSupport()
        .getOrCreate()
    )

# Initialize manager with catalog and schema
manager = DeltaTableManager(
    spark=spark,
    catalog_name="spark_catalog",
    schema_name="bronze",
    base_path=BASE_PATH,
)

# Read and flatten JSON
# df = spark.read.json("/data/raw/hotels/*.json")

# empty_df = spark.createDataFrame([], schema=flattened_hotel_schema)

# Create table (if not exists)
# manager.create_table("hotels", empty_df, comment="Raw ingested hotel data")

# Write data with schema evolution
# manager.write_data("hotels", df)

# Read table
hotels_df = manager.read_table("hotels")
hotels_df.show(20)

# List CATALOG, SCHEMA, TABLES
manager.list_catalogs().show(truncate=False)
manager.list_schemas().show(truncate=False)
manager.list_tables().show(truncate=False)

# DROP TABLE
# manager.drop_table("hotels_err", delete_data=True)

# Merge new data
# new_df = spark.read.json("/data/raw/hotels_new/*.json")
# manager.merge_new_data("hotels", new_df, key_columns=["id"])
