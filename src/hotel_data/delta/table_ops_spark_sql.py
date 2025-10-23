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
    
WAREHOUSE_DIR = "s3a://delta-bucket/warehouse"
BASE_PATH = "s3a://delta-bucket/hotel_data/delta"

spark = (
    SparkSession.builder.appName("HotelsPipeline")
    # 👇 Force Spark to download the correct Delta JAR (Scala 2.13 build)
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.13:4.0.0,"
        "org.apache.hadoop:hadoop-aws:3.4.0,"
        "software.amazon.awssdk:bundle:2.20.20",
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider,"
        "software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    # ✅ SeaweedFS S3A configuration
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:8333")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Optional: for performance
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    # ✅ Use numeric milliseconds
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
    .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
    # Delta + Hive compatibility
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
    .getOrCreate()
)



# List all catalogs
# spark.sql("SHOW CATALOGS").show(truncate=False)

# If using a catalog
# spark.sql("SHOW SCHEMAS").show(truncate=False)

# If no catalog (OSS Spark), just use the database name
# spark.sql("SHOW DATABASES").show(truncate=False)

# spark.sql("SHOW TABLES in bronze").show(truncate=False)

spark.sql("Select * from spark_catalog.bronze.hotels").show(truncate=False)
