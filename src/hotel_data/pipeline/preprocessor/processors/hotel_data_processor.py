# hotel_flattener_processor.py
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor
from hotel_data.pipeline.preprocessor.utils.hotel_data_flattner import GenericFlattener


class HotelFlattenerProcessor(BaseProcessor[DataFrame]):
    def __init__(self, explode_arrays=True):
        self.flattener = GenericFlattener(explode_arrays=explode_arrays)

    def process(self, df: DataFrame, prefix: str = "") -> DataFrame:
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

        return df
        # hotels_df = (
        #     df.withColumn("hotel", F.explode_outer(F.col("hotels")))
        #     .select("hotel.*")
        #     .alias("hotel")
        # )
        # return self.flattener.flatten(hotels_df)
