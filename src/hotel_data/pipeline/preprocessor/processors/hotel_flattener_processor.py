# hotel_flattener_processor.py
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor
from hotel_data.pipeline.preprocessor.utils.hotel_data_flattner import GenericFlattener


class HotelFlattenerProcessor(BaseProcessor[DataFrame]):
    def __init__(self, explode_arrays=True):
        self.flattener = GenericFlattener(explode_arrays=explode_arrays)

    def process(self, df: DataFrame, prefix: str = "") -> DataFrame:
        # 1. Safely explode the old format.
        # `explode_outer` ensures that NEW format rows (where 'hotels' is null) are NOT deleted!
        if "hotels" in df.columns:
            df = df.withColumn("exploded_hotel", F.explode_outer(F.col("hotels")))
        else:
            df = df.withColumn("exploded_hotel", F.lit(None))

        # 2. The Coalesce Magic
        # If exploded_hotel.id exists, it uses it. If it doesn't, it falls back to the root id.
        df = df.withColumn("id", F.coalesce(F.col("exploded_hotel.id"), F.col("id"))) \
            .withColumn("name", F.coalesce(F.col("exploded_hotel.name"), F.col("name"))) \
            .withColumn("geoCode", F.coalesce(F.col("exploded_hotel.geoCode"), F.col("geoCode"))) \
            .withColumn("contact", F.coalesce(F.col("exploded_hotel.contact"), F.col("contact"))) \
            .withColumn("providerId", F.coalesce(F.col("exploded_hotel.providerId"), F.col("providerId"))) \
            .withColumn("providerHotelId",
                        F.coalesce(F.col("exploded_hotel.providerHotelId"), F.col("providerHotelId"))) \
            .withColumn("providerName", F.coalesce(F.col("exploded_hotel.providerName"), F.col("providerName")))

        # 3. Safe Root Extraction (Fixing the geoCode_lat issue)
        df = df.withColumn("geoCode_lat", F.col("geoCode.lat")) \
            .withColumn("geoCode_long", F.col("geoCode.long")) \
            .withColumn("contact_address_line1", F.col("contact.address.line1")) \
            .withColumn("contact_address_city_name", F.col("contact.address.city.name")) \
            .withColumn("contact_address_state_name", F.col("contact.address.state.name")) \
            .withColumn("contact_address_country_name", F.col("contact.address.country.name")) \
            .withColumn("contact_address_country_code", F.col("contact.address.country.code")) \
            .withColumn("contact_address_postalCode", F.col("contact.address.postalCode")) \
            .withColumn("contact_phones", F.col("contact.phones")) \
            .withColumn("contact_fax", F.col("contact.fax")) \
            .withColumn("contact_emails", F.col("contact.emails"))

        return df.drop("exploded_hotel", "hotels")
