"""
Spark Job: Delta Lake MERGE operations (Upsert)
This script demonstrates Delta Lake's MERGE capabilities for upsert operations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, rand, when
from delta import configure_spark_with_delta_pip, DeltaTable

# Create Spark session with Delta Lake support and S3A configuration
builder = (
    SparkSession.builder.appName("Delta Lake MERGE Operations")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    .config("spark.executor.memory", "1g")
    .config("spark.executor.cores", "1")
    .config("spark.driver.memory", "1g")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("Starting Delta Lake MERGE Operations")
print("=" * 80)

# Define paths
delta_table_path = "s3a://test-delta-lake/hotel_bookings/"

try:
    # Read existing Delta table
    print(f"\n1. Reading Delta table from: {delta_table_path}")
    delta_table = DeltaTable.forPath(spark, delta_table_path)

    df_existing = delta_table.toDF()
    initial_count = df_existing.count()
    print(f"   Initial record count: {initial_count}")

    # Create updates dataset (simulate new/updated bookings)
    print("\n2. Creating updates dataset...")

    # Get a sample of existing bookings to update
    df_updates = df_existing.sample(fraction=0.1, seed=42).limit(50)

    # Modify some records (simulate cancellations and price changes)
    df_updates = (
        df_updates.withColumn(
            "is_cancelled", when(rand() > 0.7, lit(True)).otherwise(col("is_cancelled"))
        )
        .withColumn(
            "total_amount", (col("total_amount") * (0.9 + rand() * 0.2)).cast("double")
        )
        .withColumn(
            "revenue",
            when(col("is_cancelled") == True, lit(0)).otherwise(col("total_amount")),
        )
        .withColumn("processed_timestamp", current_timestamp())
    )

    # Add some new bookings
    df_new = (
        df_existing.sample(fraction=0.05, seed=123)
        .limit(20)
        .withColumn(
            "booking_id",
            concat(lit("BK9"), (rand() * 10000).cast("int").cast("string")),
        )
    )

    # Combine updates and new records
    df_upsert = df_updates.union(df_new)

    print(f"   Records to upsert: {df_upsert.count()}")
    print(f"   - Updates: ~{df_updates.count()}")
    print(f"   - New records: ~{df_new.count()}")

    # Perform MERGE operation (Upsert)
    print("\n3. Performing MERGE operation...")

    delta_table.alias("target").merge(
        df_upsert.alias("source"), "target.booking_id = source.booking_id"
    ).whenMatchedUpdate(
        set={
            "is_cancelled": col("source.is_cancelled"),
            "total_amount": col("source.total_amount"),
            "revenue": col("source.revenue"),
            "processed_timestamp": col("source.processed_timestamp"),
        }
    ).whenNotMatchedInsertAll().execute()

    print("   ✓ MERGE operation completed successfully")

    # Verify the merge
    print("\n4. Verifying merge results...")
    df_after_merge = spark.read.format("delta").load(delta_table_path)
    final_count = df_after_merge.count()

    print(f"   Initial records: {initial_count}")
    print(f"   Final records: {final_count}")
    print(f"   Net change: +{final_count - initial_count}")

    # Show statistics
    print("\n5. Post-merge statistics:")

    cancelled_count = df_after_merge.filter(col("is_cancelled") == "true").count()
    active_count = df_after_merge.filter(col("is_cancelled") == "false").count()

    print(f"   Active bookings: {active_count}")
    print(f"   Cancelled bookings: {cancelled_count}")
    print(f"   Cancellation rate: {(cancelled_count / final_count * 100):.2f}%")

    total_revenue = df_after_merge.agg({"revenue": "sum"}).collect()[0][0]
    print(f"   Total revenue: ${total_revenue:,.2f}")

    # Show sample updated records
    print("\n6. Sample of recently updated records:")
    df_after_merge.orderBy(col("processed_timestamp").desc()).select(
        "booking_id",
        "hotel_name",
        "is_cancelled",
        "total_amount",
        "revenue",
        "processed_timestamp",
    ).show(10, truncate=False)

    print("\n" + "=" * 80)
    print("Delta Lake MERGE Operations Completed Successfully!")
    print("=" * 80)

except Exception as e:
    print(f"\n❌ Error during MERGE operation: {str(e)}")
    import traceback

    traceback.print_exc()
    raise

finally:
    spark.stop()
