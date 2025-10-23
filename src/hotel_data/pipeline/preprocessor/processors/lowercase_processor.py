# lowercase_processor.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import lower, col
from pyspark.sql.types import StringType

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor

class LowercaseProcessor(BaseProcessor):
    def __init__(self, exclude_fields: list[str]):
        self.exclude_fields = exclude_fields

    def process(self, df: DataFrame) -> DataFrame:
        # for f in self.fields:
        #     df = df.withColumn(f, lower(col(f)))
        for field in df.schema.fields:
            if isinstance(field.dataType, StringType) and field.name not in self.exclude_fields:
                df = df.withColumn(field.name, lower(col(field.name)))
        return df
