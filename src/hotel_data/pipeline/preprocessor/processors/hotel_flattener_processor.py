# hotel_flattener_processor.py
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor
from hotel_data.pipeline.preprocessor.utils.hotel_data_flattner import GenericFlattener


class HotelFlattenerProcessor(BaseProcessor[DataFrame]):
    def __init__(self, explode_arrays=True):
        self.flattener = GenericFlattener(explode_arrays=explode_arrays)

    def process(self, df: DataFrame, prefix: str = "") -> DataFrame:
        # 1) Wrapped payload format: {"hotels": [...], "curatedHotels": [...]}.
        # Flatten to one hotel record per row.
        if "hotels" in df.columns:
            df = df.withColumn(
                "exploded_hotel", F.explode_outer(F.col("hotels"))
            ).select(F.col("exploded_hotel.*"))

        # 2) Flat payload format already has hotel fields at root level.
        # In this case, we use the DataFrame as-is.

        # 3) Extract commonly used nested fields.
        df = (
            df.withColumn("geoCode_lat", F.col("geoCode.lat"))
            .withColumn("geoCode_long", F.col("geoCode.long"))
            .withColumn("contact_address_line1", F.col("contact.address.line1"))
            .withColumn("contact_address_city_name", F.col("contact.address.city.name"))
            .withColumn(
                "contact_address_state_name", F.col("contact.address.state.name")
            )
            .withColumn(
                "contact_address_country_name", F.col("contact.address.country.name")
            )
            .withColumn(
                "contact_address_country_code", F.col("contact.address.country.code")
            )
            .withColumn(
                "contact_address_postalCode", F.col("contact.address.postalCode")
            )
            .withColumn("contact_phones", F.col("contact.phones"))
            .withColumn("contact_fax", F.col("contact.fax"))
            .withColumn("contact_emails", F.col("contact.emails"))
        )

        return df
