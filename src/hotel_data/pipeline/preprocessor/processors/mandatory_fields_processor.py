# critical_field_filter_processor.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor

class MandatoryFieldsFilterProcessor(BaseProcessor):
    def __init__(self, critical_fields: list[str]):
        self.critical_fields = critical_fields

    def process(self, df: DataFrame):
        valid_df = df
        for field in self.critical_fields:
            valid_df = valid_df.filter(col(field).isNotNull())

        invalid_df = df.exceptAll(valid_df)
        # Add timestamps to both
        valid_df = valid_df.withColumn("processing_time_utc", current_timestamp())
        invalid_df = invalid_df.withColumn("processing_time_utc", current_timestamp())
        return valid_df, invalid_df
