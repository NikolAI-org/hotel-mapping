"""
Spark Job: Delta Lake Time Travel
This script demonstrates Delta Lake's time-travel capabilities
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip, DeltaTable

# Create Spark session with Delta Lake support and S3A configuration
builder = SparkSession.builder \
    .appName("Delta Lake Time Travel") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.driver.memory", "1g")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("Delta Lake Time Travel Demonstration")
print("=" * 80)

# Define paths
delta_table_path = "s3a://test-delta-lake/hotel_bookings/"

try:
    # Load the Delta table
    print(f"\n1. Loading Delta table: {delta_table_path}")
    delta_table = DeltaTable.forPath(spark, delta_table_path)

    # Get current data
    df_current = spark.read.format("delta").load(delta_table_path)
    current_count = df_current.count()

    print(f"   Current record count: {current_count}")

    # Show Delta table history
    print("\n2. Delta Table History:")
    history_df = delta_table.history()

    print("   Version History:")
    history_df.select("version", "timestamp", "operation", "operationMetrics") \
        .orderBy(col("version").desc()) \
        .show(truncate=False)

    # Get version count
    versions = history_df.select("version").collect()
    version_count = len(versions)

    print(f"\n   Total versions available: {version_count}")

    if version_count >= 2:
        # Time travel to version 0 (initial load)
        print("\n3. Time Travel Query - Version 0 (Initial Load):")
        df_v0 = spark.read \
            .format("delta") \
            .option("versionAsOf", 0) \
            .load(delta_table_path)

        v0_count = df_v0.count()
        print(f"   Records in version 0: {v0_count}")

        # Time travel to latest version
        latest_version = versions[0][0]
        print(f"\n4. Time Travel Query - Version {latest_version} (Latest):")
        df_latest = spark.read \
            .format("delta") \
            .option("versionAsOf", latest_version) \
            .load(delta_table_path)

        latest_count = df_latest.count()
        print(f"   Records in version {latest_version}: {latest_count}")

        # Compare versions
        print("\n5. Version Comparison:")
        print(f"   Version 0 count: {v0_count}")
        print(f"   Version {latest_version} count: {latest_count}")
        print(f"   Difference: {latest_count - v0_count} records")

        # Compare cancellation rates across versions
        print("\n6. Cancellation Rate Analysis:")

        v0_cancelled = df_v0.filter(col("is_cancelled") == "true").count()
        v0_cancel_rate = (v0_cancelled / v0_count * 100) if v0_count > 0 else 0

        latest_cancelled = df_latest.filter(
            col("is_cancelled") == "true").count()
        latest_cancel_rate = (
            latest_cancelled / latest_count * 100) if latest_count > 0 else 0

        print(f"   Version 0 cancellation rate: {v0_cancel_rate:.2f}%")
        print(
            f"   Version {latest_version} cancellation rate: {latest_cancel_rate:.2f}%")
        print(f"   Change: {latest_cancel_rate - v0_cancel_rate:+.2f}%")

        # Revenue comparison
        print("\n7. Revenue Analysis:")

        v0_revenue = df_v0.agg({"revenue": "sum"}).collect()[0][0] or 0
        latest_revenue = df_latest.agg({"revenue": "sum"}).collect()[0][0] or 0

        print(f"   Version 0 revenue: ${v0_revenue:,.2f}")
        print(f"   Version {latest_version} revenue: ${latest_revenue:,.2f}")
        print(f"   Change: ${latest_revenue - v0_revenue:+,.2f}")

        # Time-based time travel (if available)
        print("\n8. Timestamp-based Time Travel:")

        timestamps = history_df.select(
            "timestamp").orderBy("timestamp").collect()
        if len(timestamps) >= 2:
            first_timestamp = timestamps[0][0]
            print(f"   Querying data as of: {first_timestamp}")

            df_timestamp = spark.read \
                .format("delta") \
                .option("timestampAsOf", str(first_timestamp)) \
                .load(delta_table_path)

            ts_count = df_timestamp.count()
            print(f"   Records at that timestamp: {ts_count}")

    else:
        print("\n   Note: Only one version available. Run the merge job to create more versions.")

    # Show detailed metrics for current version
    print("\n9. Current Version Detailed Metrics:")

    print("\n   Bookings by City:")
    df_current.groupBy("city") \
        .count() \
        .orderBy(col("count").desc()) \
        .show(truncate=False)

    print("   Bookings by Room Type:")
    df_current.groupBy("room_type") \
        .count() \
        .orderBy(col("count").desc()) \
        .show(truncate=False)

    print("   Top Hotels by Revenue:")
    df_current.groupBy("hotel_name") \
        .agg({"revenue": "sum", "booking_id": "count"}) \
        .withColumnRenamed("sum(revenue)", "total_revenue") \
        .withColumnRenamed("count(booking_id)", "total_bookings") \
        .orderBy(col("total_revenue").desc()) \
        .show(10, truncate=False)

    print("\n" + "=" * 80)
    print("Delta Lake Time Travel Queries Completed Successfully!")
    print("=" * 80)
    print("\nKey Takeaways:")
    print("- Delta Lake maintains full version history")
    print("- Query any previous version using versionAsOf or timestampAsOf")
    print("- Enables data auditing, debugging, and reproducibility")
    print("- No additional storage overhead for unchanged data")
    print("=" * 80)

except Exception as e:
    print(f"\n❌ Error during time travel query: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

finally:
    spark.stop()
