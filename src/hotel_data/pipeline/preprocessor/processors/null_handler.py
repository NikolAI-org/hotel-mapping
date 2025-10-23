from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor

class NullHandler(BaseProcessor):
    def __init__(self, fill_values: dict):
        self.fill_values = fill_values

    def process(self, df: DataFrame, prefix: str = "") -> DataFrame:
        return df.fillna(self.fill_values)
