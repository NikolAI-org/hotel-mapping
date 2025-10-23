from pyspark.sql import DataFrame, functions as F
from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor

class SpecialCharCleaner(BaseProcessor):
    def __init__(self, columns: list[str]):
        self.columns = columns

    def process(self, df: DataFrame, prefix: str = "") -> DataFrame:
        for col in self.columns:
            df = df.withColumn(col, F.regexp_replace(F.col(col), r'[^a-zA-Z0-9\s]', ''))
        return df
