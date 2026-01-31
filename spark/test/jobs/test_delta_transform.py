"""
Spark Job: Transform raw data to Delta Lake format
This script reads CSV data from MinIO and writes it as a Delta table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, datediff, to_date, current_timestamp
from delta import configure_spark_with_delta_pip

# Create Spark session with Delta Lake support and S3A configuration
builder = SparkSession.builder \
    .appName("Delta Lake Transformation") \
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
print("Starting Delta Lake Transformation")
print("=" * 80)

# Define paths
raw_data_path = "s3a://test-data-lake/raw/hotel_bookings/hotel_bookings_raw.csv"
delta_table_path = "s3a://test-delta-lake/hotel_bookings/"

try:
    # Read raw CSV data
    print(f"\n1. Reading raw data from: {raw_data_path}")
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(raw_data_path)

    print(f"   Records read: {df_raw.count()}")
    print(f"   Schema:")
    df_raw.printSchema()

    # Data transformations
    print("\n2. Applying transformations...")

    df_transformed = df_raw \
        .withColumn("booking_date", to_date(col("booking_date"))) \
        .withColumn("checkin_date", to_date(col("checkin_date"))) \
        .withColumn("checkout_date", to_date(col("checkout_date"))) \
        .withColumn("nights_stayed",
                    datediff(col("checkout_date"), col("checkin_date"))) \
        .withColumn("booking_lead_time",
                    datediff(col("checkin_date"), col("booking_date"))) \
        .withColumn("revenue",
                    when(col("is_cancelled") == "false", col("total_amount")).otherwise(0)) \
        .withColumn("processed_timestamp", current_timestamp())

    print("   Transformations applied:")
    print("   - Date columns converted to proper date types")
    print("   - nights_stayed: calculated from check-in/out dates")
    print("   - booking_lead_time: days between booking and check-in")
    print("   - revenue: total_amount if not cancelled, else 0")
    print("   - processed_timestamp: current timestamp added")

    # Show sample data
    print("\n3. Sample transformed data:")
    df_transformed.show(5, truncate=False)

    # Write to Delta Lake
    print(f"\n4. Writing to Delta Lake: {delta_table_path}")
    df_transformed.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(delta_table_path)

    print("   ✓ Data successfully written to Delta Lake")

    # Verify the write
    print("\n5. Verifying Delta table...")
    df_verify = spark.read.format("delta").load(delta_table_path)
    record_count = df_verify.count()
    print(f"   ✓ Verified {record_count} records in Delta table")

    # Show Delta table statistics
    print("\n6. Delta Table Statistics:")
    df_verify.groupBy("city", "room_type").count().show()

    # Create aggregated view
    print("\n7. Creating aggregated metrics...")
    df_agg = df_verify.groupBy("hotel_name", "city") \
        .agg({
            "booking_id": "count",
            "revenue": "sum",
            "nights_stayed": "avg"
        }) \
        .withColumnRenamed("count(booking_id)", "total_bookings") \
        .withColumnRenamed("sum(revenue)", "total_revenue") \
        .withColumnRenamed("avg(nights_stayed)", "avg_nights_stayed")

    print("\n   Hotel Performance Metrics:")
    df_agg.orderBy(col("total_revenue").desc()).show(10, truncate=False)

    # Write aggregated data
    agg_path = "s3a://delta-lake/hotel_bookings_aggregated/"
    print(f"\n8. Writing aggregated data to: {agg_path}")
    df_agg.write \
        .format("delta") \
        .mode("overwrite") \
        .save(agg_path)

    print("   ✓ Aggregated data written successfully")

    print("\n" + "=" * 80)
    print("Delta Lake Transformation Completed Successfully!")
    print("=" * 80)

except Exception as e:
    print(f"\n❌ Error during transformation: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

finally:
    spark.stop()
