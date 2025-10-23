from pyspark.sql.types import *

flattened_hotel_schema = StructType([
    # Top-level hotel fields
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("relevanceScore", StringType(), True),
    StructField("providerId", StringType(), True),
    StructField("providerHotelId", StringType(), True),
    StructField("providerName", StringType(), True),
    StructField("language", StringType(), True),

    # GeoCode
    StructField("geoCode_lat", StringType(), True),
    StructField("geoCode_long", StringType(), True),

    # Contact → Address
    StructField("contact_address_line1", StringType(), True),
    StructField("contact_address_city_name", StringType(), True),
    StructField("contact_address_state_name", StringType(), True),
    StructField("contact_address_country_code", StringType(), True),
    StructField("contact_address_country_name", StringType(), True),
    StructField("contact_address_postalCode", StringType(), True),

    # Contact → Phones/Fax/Emails
    StructField("contact_phones", ArrayType(StringType()), True),
    StructField("contact_fax", ArrayType(StringType()), True),
    StructField("contact_emails", ArrayType(StringType()), True),

    # Other hotel info
    StructField("type", StringType(), True),
    StructField("category", StringType(), True),
    StructField("starRating", StringType(), True),
    StructField("distance", StringType(), True),
    StructField("attributes", ArrayType(StringType()), True),
    StructField("imageCount", StringType(), True),
    StructField("availableSuppliers", ArrayType(StringType()), True),
    
    # New fields
    StructField("combined_address", StringType(), True),
    StructField("processing_time_utc", TimestampType(), True),
    StructField("original_message", StringType(), True)
])
