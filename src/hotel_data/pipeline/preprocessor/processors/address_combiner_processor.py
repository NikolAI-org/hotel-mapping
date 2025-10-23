# address_combiner_processor.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import concat_ws, regexp_replace, col

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor

class AddressCombinerProcessor(BaseProcessor):
    def __init__(self, address_fields: list[str], output_col="combined_address"):
        self.address_fields = address_fields
        self.output_col = output_col

    def process(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            self.output_col,
            regexp_replace(
                concat_ws(" ", *[col(f) for f in self.address_fields]),
                r"[^a-zA-Z0-9\s]",
                ""
            )
        )
