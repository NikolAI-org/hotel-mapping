# hotel_flattener_processor.py
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor
from hotel_data.pipeline.preprocessor.utils.hotel_data_flattner import GenericFlattener


class HotelFlattenerProcessor(BaseProcessor):
    def __init__(self, explode_arrays=True):
        self.flattener = GenericFlattener(explode_arrays=explode_arrays)

    def process(self, df: DataFrame) -> DataFrame:
        hotels_df = (
            df.withColumn("hotel", F.explode_outer(F.col("hotels")))
            .select("hotel.*")
            .alias("hotel")
        )
        return self.flattener.flatten(hotels_df)
