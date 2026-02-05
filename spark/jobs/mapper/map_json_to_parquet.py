"""
Spark Job: Map Raw JSON Hotel Data to Parquet Format
This script reads JSON hotel data from MinIO, applies schema mapping,
and writes to Parquet format for efficient querying.

Input: s3a://data-lake/raw_input/{country}/{supplier_name}/*.json
Output: s3a://data-lake/mapped_input/{country}/{supplier_name}/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, current_timestamp, lit,
    concat_ws, array_join, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, ArrayType, BooleanType
)
import os

# Get parameters from environment variables (set by DAG)
country = os.getenv('COUNTRY', 'india')
supplier_name = os.getenv('SUPPLIER_NAME', 'expedia')

# Create Spark session with S3A configuration for MinIO
builder = SparkSession.builder \
    .appName(f"Map JSON to Parquet - {country}/{supplier_name}") \
    .master("spark://spark-master:7077") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.driver.memory", "1g")

spark = builder.getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print(
    f"Starting JSON to Parquet Mapping - {country.upper()} / {supplier_name.upper()}")
print("=" * 80)

# Define paths
raw_input_path = f"s3a://data-lake/raw_input/{country}/{supplier_name}/"
mapped_output_path = f"s3a://data-lake/mapped_input/{country}/{supplier_name}/"

print(f"\nSource Path: {raw_input_path}")
print(f"Target Path: {mapped_output_path}")

try:
    # Define schema for the nested JSON structure
    print("\n1. Defining JSON schema...")

    # Address schema
    address_schema = StructType([
        StructField("line1", StringType(), True),
        StructField("line2", StringType(), True),
        StructField("city", StructType([
            StructField("name", StringType(), True)
        ]), True),
        StructField("state", StructType([
            StructField("name", StringType(), True),
            StructField("code", StringType(), True)
        ]), True),
        StructField("country", StructType([
            StructField("code", StringType(), True),
            StructField("name", StringType(), True)
        ]), True),
        StructField("postalCode", StringType(), True)
    ])

    # Contact schema
    contact_schema = StructType([
        StructField("address", address_schema, True),
        StructField("phones", ArrayType(StringType()), True),
        StructField("fax", ArrayType(StringType()), True),
        StructField("emails", ArrayType(StringType()), True)
    ])

    # GeoCode schema
    geoCode_schema = StructType([
        StructField("lat", StringType(), True),
        StructField("long", StringType(), True)
    ])

    # Hotel schema
    hotel_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("relevanceScore", StringType(), True),
        StructField("providerId", StringType(), True),
        StructField("providerHotelId", StringType(), True),
        StructField("providerName", StringType(), True),
        StructField("language", StringType(), True),
        StructField("geoCode", geoCode_schema, True),
        StructField("contact", contact_schema, True),
        StructField("type", StringType(), True),
        StructField("category", StringType(), True),
        StructField("starRating", StringType(), True),
        StructField("distance", StringType(), True),
        StructField("attributes", ArrayType(StringType()), True),
        StructField("imageCount", StringType(), True),
        StructField("availableSuppliers", ArrayType(StringType()), True),
        StructField("website", StringType(), True)
    ])

    # Root schema
    root_schema = StructType([
        StructField("hotels", ArrayType(hotel_schema), True),
        StructField("curatedHotels", ArrayType(hotel_schema), True)
    ])

    print("   ✓ Schema defined successfully")

    # Read JSON files
    print(f"\n2. Reading JSON files from: {raw_input_path}")
    df_raw = spark.read \
        .schema(root_schema) \
        .json(raw_input_path)

    print(f"   Files read successfully")

    # Explode hotels array to get individual hotel records
    print("\n3. Exploding hotels array...")
    df_hotels = df_raw.select(explode(col("hotels")).alias("hotel"))

    # Extract and flatten hotel data
    print("\n4. Flattening and transforming hotel data...")
    df_mapped = df_hotels.select(
        # Basic hotel information
        col("hotel.id").alias("hotel_id"),
        col("hotel.name").alias("hotel_name"),
        col("hotel.relevanceScore").alias("relevance_score"),

        # Provider information
        col("hotel.providerId").alias("provider_id"),
        col("hotel.providerHotelId").alias("provider_hotel_id"),
        col("hotel.providerName").alias("provider_name"),

        # Geo location
        col("hotel.geoCode.lat").cast(DoubleType()).alias("latitude"),
        col("hotel.geoCode.long").cast(DoubleType()).alias("longitude"),

        # Address information
        col("hotel.contact.address.line1").alias("address_line1"),
        col("hotel.contact.address.line2").alias("address_line2"),
        col("hotel.contact.address.city.name").alias("city"),
        col("hotel.contact.address.state.name").alias("state"),
        col("hotel.contact.address.state.code").alias("state_code"),
        col("hotel.contact.address.country.code").alias("country_code"),
        col("hotel.contact.address.country.name").alias("country_name"),
        col("hotel.contact.address.postalCode").alias("postal_code"),

        # Contact information (arrays to comma-separated strings)
        array_join(col("hotel.contact.phones"), ", ").alias("phone_numbers"),
        array_join(col("hotel.contact.fax"), ", ").alias("fax_numbers"),
        array_join(col("hotel.contact.emails"), ", ").alias("email_addresses"),

        # Hotel attributes
        col("hotel.type").alias("hotel_type"),
        col("hotel.category").alias("hotel_category"),
        col("hotel.starRating").cast(DoubleType()).alias("star_rating"),
        col("hotel.distance").cast(DoubleType()).alias("distance"),
        col("hotel.imageCount").cast(IntegerType()).alias("image_count"),
        col("hotel.website").alias("website"),

        # Attributes and suppliers (keep as arrays)
        col("hotel.attributes").alias("attributes"),
        col("hotel.availableSuppliers").alias("available_suppliers"),

        # Metadata
        col("hotel.language").alias("language"),
        lit(country).alias("source_country"),
        lit(supplier_name).alias("source_supplier"),
        current_timestamp().alias("processed_timestamp")
    )

    record_count = df_mapped.count()
    print(f"   ✓ Mapped {record_count} hotel records")

    # Show schema and sample data
    print("\n5. Mapped data schema:")
    df_mapped.printSchema()

    print("\n6. Sample mapped data:")
    df_mapped.select(
        "hotel_id", "hotel_name", "city", "state",
        "country_code", "star_rating", "latitude", "longitude"
    ).show(5, truncate=False)

    # Write to Parquet format
    print(f"\n7. Writing to Parquet: {mapped_output_path}")
    df_mapped.write \
        .mode("overwrite") \
        .parquet(mapped_output_path)

    print("   ✓ Data successfully written to Parquet")

    # Verify the write
    print("\n8. Verifying Parquet output...")
    df_verify = spark.read.parquet(mapped_output_path)
    verify_count = df_verify.count()
    print(f"   ✓ Verified {verify_count} records in Parquet files")

    # Show statistics
    print("\n9. Data Statistics:")
    print(f"\n   Hotels by City:")
    df_verify.groupBy("city").count().orderBy(col("count").desc()).show(10)

    print(f"\n   Hotels by Star Rating:")
    df_verify.groupBy("star_rating").count().orderBy("star_rating").show()

    print(f"\n   Hotels by Type:")
    df_verify.groupBy("hotel_type").count().orderBy(col("count").desc()).show()

    print("\n" + "=" * 80)
    print("JSON to Parquet Mapping Completed Successfully!")
    print("=" * 80)
    print(f"\nSummary:")
    print(f"  Total Hotels Mapped: {verify_count}")
    print(f"  Country: {country}")
    print(f"  Supplier: {supplier_name}")
    print(f"  Output Format: Parquet (partitioned)")
    print(f"  Output Location: {mapped_output_path}")
    print("=" * 80)

except Exception as e:
    print("\n" + "=" * 80)
    print("ERROR: JSON to Parquet mapping failed!")
    print("=" * 80)
    print(f"Error details: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

finally:
    # Stop Spark session
    spark.stop()
