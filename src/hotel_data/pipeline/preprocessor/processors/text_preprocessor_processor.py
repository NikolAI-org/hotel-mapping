from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor
from hotel_data.pipeline.preprocessor.processors.smart_cleaner import (
    normalize_real_estate_udf,
)


class TextPreprocessorProcessor(BaseProcessor[DataFrame]):
    """
    Adds preprocessed text columns used for mismatch-sensitive scoring.
    """

    def __init__(
        self,
        name_input_col: str = "name",
        name_output_col: str = "name_preprocessed",
        address_input_col: str = "combined_address",
        address_output_col: str = "combined_address_preprocessed",
    ):
        self.name_input_col = name_input_col
        self.name_output_col = name_output_col
        self.address_input_col = address_input_col
        self.address_output_col = address_output_col

    def process(self, df: DataFrame, prefix: str = "") -> DataFrame:
        name_expr = normalize_real_estate_udf(F.col(self.name_input_col))
        address_expr = normalize_real_estate_udf(F.col(self.address_input_col))

        return df.withColumn(
            self.name_output_col, F.trim(F.regexp_replace(name_expr, r"\\s+", " "))
        ).withColumn(
            self.address_output_col,
            F.trim(F.regexp_replace(address_expr, r"\\s+", " ")),
        )
