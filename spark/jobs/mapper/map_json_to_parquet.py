"""
Spark Job: Map Raw JSON Hotel Data to Parquet Format
This script reads JSON hotel data from MinIO, applies schema mapping,
and writes to Parquet format for efficient querying.

Input: s3a://data-lake/raw_input/{country}/{supplier_name}/*.json
Output: s3a://data-lake/mapped_input/{country}/{supplier_name}/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    current_timestamp,
    lit,
    concat_ws,
    array_join,
    to_timestamp,
    udf,
    lower,
    regexp_replace,
    trim,
    coalesce,
    split,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    ArrayType,
    BooleanType,
)
import os
import re

# Get parameters from environment variables (set by DAG)
country = os.getenv("COUNTRY", "india")
supplier_name = os.getenv("SUPPLIER_NAME", "expedia")

# Get or create SparkSession - will reuse SparkContext from spark-submit
print("Getting SparkSession...")
try:
    # Try to get active session first
    spark = SparkSession.getActiveSession()
    if spark is None:
        # If no active session, create one (will reuse existing SparkContext)
        spark = SparkSession.builder.appName(
            f"MapJSON-{country}-{supplier_name}"
        ).getOrCreate()
    print(f"✓ Spark session ready: {spark.version}")
    print(f"✓ Master: {spark.sparkContext.master}")
except Exception as e:
    print(f"Error getting SparkSession: {e}")
    raise

# Set log level
spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print(f"Starting JSON to Parquet Mapping - {country.upper()} / {supplier_name.upper()}")
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
    address_schema = StructType(
        [
            StructField("line1", StringType(), True),
            StructField("line2", StringType(), True),
            StructField(
                "city", StructType([StructField("name", StringType(), True)]), True
            ),
            StructField(
                "state",
                StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("code", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField(
                "country",
                StructType(
                    [
                        StructField("code", StringType(), True),
                        StructField("name", StringType(), True),
                    ]
                ),
                True,
            ),
            StructField("postalCode", StringType(), True),
        ]
    )

    # Contact schema
    contact_schema = StructType(
        [
            StructField("address", address_schema, True),
            StructField("phones", ArrayType(StringType()), True),
            StructField("fax", ArrayType(StringType()), True),
            StructField("emails", ArrayType(StringType()), True),
        ]
    )

    # GeoCode schema
    geoCode_schema = StructType(
        [
            StructField("lat", StringType(), True),
            StructField("long", StringType(), True),
        ]
    )

    # Hotel schema
    hotel_schema = StructType(
        [
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
            StructField("website", StringType(), True),
        ]
    )

    # Root schema
    root_schema = StructType(
        [
            StructField("hotels", ArrayType(hotel_schema), True),
            StructField("curatedHotels", ArrayType(hotel_schema), True),
        ]
    )

    print("   ✓ Schema defined successfully")

    # Read JSON files
    print(f"\n2. Reading JSON files from: {raw_input_path}")
    df_raw = spark.read.schema(root_schema).json(raw_input_path)

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
        current_timestamp().alias("processed_timestamp"),
    )

    record_count = df_mapped.count()
    print(f"   ✓ Mapped {record_count} hotel records")

    # Add normalized name column
    print("\n5. Creating normalized hotel name...")

    # Common hotel industry words to remove
    common_hotel_words = {
        "hotel",
        "hotels",
        "inn",
        "inns",
        "resort",
        "resorts",
        "lodge",
        "lodges",
        "motel",
        "motels",
        "suites",
        "suite",
        "residences",
        "residence",
        "hostel",
        "hostels",
        "guest",
        "house",
        "home",
        "homes",
        "palace",
        "grand",
        "royal",
        "international",
        "plaza",
        "tower",
        "towers",
        "center",
        "centre",
        "comfort",
        "luxury",
        "deluxe",
        "premium",
        "business",
        "boutique",
        "heritage",
        "classic",
        "retreat",
        "villa",
        "villas",
        "apartment",
        "apartments",
        "serviced",
        "extended",
        "stay",
        "bed",
        "breakfast",
        "b&b",
        "bungalow",
        "cottage",
        "cottages",
        "the",
        "and",
        "by",
        "at",
        "in",
        "on",
        "of",
        "a",
        "an",
        "&",
        "for",
        "with",
        "to",
    }

    def normalize_hotel_name(
        name,
        city,
        state,
        country_name,
        hotel_type,
        hotel_category,
        address_line1,
        address_line2,
    ):
        """
        Normalize hotel name by removing common words, location info, and address tokens
        """
        if not name:
            return None

        # Convert to lowercase and keep only alphanumeric and spaces
        normalized = re.sub(r"[^a-z0-9\s]", " ", name.lower())

        # Split into tokens
        tokens = normalized.split()

        # Build exclusion set from various fields
        exclude_tokens = set(common_hotel_words)

        # Add location tokens (exact match)
        if city:
            exclude_tokens.update(city.lower().split())
        if state:
            exclude_tokens.update(state.lower().split())
        if country_name:
            exclude_tokens.update(country_name.lower().split())
        if hotel_type:
            exclude_tokens.update(hotel_type.lower().split())
        if hotel_category:
            exclude_tokens.update(hotel_category.lower().split())

        # Add address tokens (exact match)
        if address_line1:
            address1_clean = re.sub(r"[^a-z0-9\s]", " ", address_line1.lower())
            exclude_tokens.update(address1_clean.split())
        if address_line2:
            address2_clean = re.sub(r"[^a-z0-9\s]", " ", address_line2.lower())
            exclude_tokens.update(address2_clean.split())

        # Filter tokens - keep only those not in exclusion set
        filtered_tokens = [
            token for token in tokens if token and token not in exclude_tokens
        ]

        # Join and clean up multiple spaces
        result = " ".join(filtered_tokens).strip()

        # Return None if nothing left after normalization
        return result if result else None

    # Register UDF
    normalize_name_udf = udf(normalize_hotel_name, StringType())

    # Apply normalization
    df_mapped = df_mapped.withColumn(
        "normalized_name",
        normalize_name_udf(
            col("hotel_name"),
            col("city"),
            col("state"),
            col("country_name"),
            col("hotel_type"),
            col("hotel_category"),
            col("address_line1"),
            col("address_line2"),
        ),
    )

    print("   ✓ Normalized name column created")

    # Show schema and sample data
    print("\n6. Mapped data schema:")
    df_mapped.printSchema()

    print("\n7. Sample mapped data with normalized names:")
    df_mapped.select(
        "hotel_id",
        "hotel_name",
        "normalized_name",
        "city",
        "state",
        "country_code",
        "star_rating",
        "latitude",
        "longitude",
    ).show(10, truncate=False)

    # Write to Parquet format
    print(f"\n8. Writing to Parquet: {mapped_output_path}")
    df_mapped.write.mode("overwrite").parquet(mapped_output_path)

    print("   ✓ Data successfully written to Parquet")

    # Verify the write
    print("\n9. Verifying Parquet output...")
    df_verify = spark.read.parquet(mapped_output_path)
    verify_count = df_verify.count()
    print(f"   ✓ Verified {verify_count} records in Parquet files")

    # Show statistics
    print("\n10. Data Statistics:")
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
