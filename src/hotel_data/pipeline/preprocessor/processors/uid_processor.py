# address_combiner_processor.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, concat_ws, regexp_replace, lower, trim, base64

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor


class UIDProcessor(BaseProcessor[DataFrame]):
    def __init__(
        self,
        name_col: str = "normalized_name",
        address_col: str = "combined_address",
        output_col="uid",
    ):
        self.address_col = address_col
        self.name_col = name_col
        self.output_col = output_col

    def process(self, df: DataFrame, prefix: str = "") -> DataFrame:
        # Step 2: combine name + address (with delimiter)

        clean_name = F.regexp_replace(F.lower(F.col(self.name_col)), r"[^a-z0-9]", "")
        clean_addr = F.regexp_replace(
            F.lower(F.col(self.address_col)), r"[^a-z0-9]", ""
        )

        return df.withColumn(
            self.output_col,
            F.sha2(F.concat(F.col("providerId"), clean_name, clean_addr), 256),
        )
