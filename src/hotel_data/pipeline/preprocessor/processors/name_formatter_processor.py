from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, trim

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor

class NameFormatterProcessor(BaseProcessor):
    def __init__(self, 
                 address_fields: list[str],
                 name_col: str = "name",
                 output_col="normalized_name"):
        self.address_fields = address_fields
        self.output_col = output_col
        self.name_col = name_col

    def process(self, df: DataFrame) -> DataFrame:
        # Remove city, state, and country from name, case-insensitive
        cleaned_col = col(self.name_col)
        for field in self.address_fields:
            cleaned_col = regexp_replace(cleaned_col, col(field), "")
        
        # Trim extra spaces
        cleaned_col = trim(cleaned_col)
        
        return df.withColumn(self.output_col, cleaned_col)
