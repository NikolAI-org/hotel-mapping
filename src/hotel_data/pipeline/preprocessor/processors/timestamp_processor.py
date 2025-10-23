# timestamp_appender_processor.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor

class TimestampAppenderProcessor(BaseProcessor):
    def process(self, df: DataFrame) -> DataFrame:
        return df.withColumn("processing_time_utc", current_timestamp())
