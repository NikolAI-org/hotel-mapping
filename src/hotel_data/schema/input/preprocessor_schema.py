from pyspark.sql.types import StructType, StructField, StringType, ArrayType

hotel_schema = StructType([
    StructField("hotels", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("relevanceScore", StringType(), True),
            StructField("providerId", StringType(), True),
            StructField("providerHotelId", StringType(), True),
            StructField("providerName", StringType(), True),
            StructField("language", StringType(), True),
            StructField("geoCode", StructType([
                StructField("lat", StringType(), True),
                StructField("long", StringType(), True)
            ]), True),
            StructField("contact", StructType([
                StructField("address", StructType([
                    StructField("line1", StringType(), True),
                    StructField("city", StructType([
                        StructField("name", StringType(), True)
                    ]), True),
                    StructField("state", StructType([
                        StructField("name", StringType(), True)
                    ]), True),
                    StructField("country", StructType([
                        StructField("code", StringType(), True),
                        StructField("name", StringType(), True)
                    ]), True),
                    StructField("postalCode", StringType(), True)
                ]), True),
                StructField("phones", ArrayType(StringType()), True),
                StructField("fax", ArrayType(StringType()), True),
                StructField("emails", ArrayType(StringType()), True)
            ]), True),
            StructField("type", StringType(), True),
            StructField("category", StringType(), True),
            StructField("starRating", StringType(), True),
            StructField("distance", StringType(), True),
            StructField("attributes", ArrayType(StringType()), True),
            StructField("imageCount", StringType(), True),
            StructField("availableSuppliers", ArrayType(StringType()), True)
        ])
    ), True),
    StructField("curatedHotels", ArrayType(
        StructType([])  # Empty schema
    ), True)
])
