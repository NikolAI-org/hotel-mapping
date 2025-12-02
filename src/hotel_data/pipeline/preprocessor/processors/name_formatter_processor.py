from pyspark.sql import DataFrame
from pyspark.sql.functions import col, regexp_replace, trim, lit, concat, when

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor
from hotel_data.pipeline.preprocessor.processors.smart_cleaner import smart_suffix_udf


class NameFormatterProcessor(BaseProcessor):
    def __init__(self,
                 address_fields: list[str],
                 name_col: str = "name",
                 output_col="normalized_name"):
        self.address_fields = address_fields
        self.output_col = output_col
        self.name_col = name_col

    def process(self, df: DataFrame) -> DataFrame:
        cleaned_col = col(self.name_col)

        for field in self.address_fields:
            # 1. Build a Safe Regex Pattern
            #    (?i)  -> Enable Case Insensitivity
            #    \Q    -> Start Literal text (Escape all special chars like '(')
            #    ...   -> The dynamic address column value
            #    \E    -> End Literal text
            safe_pattern = concat(lit("(?i)\\Q"), col(field), lit("\\E"))

            # 2. Apply Replace ONLY if the address field is not null and not empty
            #    If we pass NULL to regexp_replace, the result becomes NULL (destroying the name)
            cleaned_col = when(
                (col(field).isNotNull()) & (col(field) != ""),
                regexp_replace(cleaned_col, safe_pattern, "")
            ).otherwise(cleaned_col)

        cleaned_col = smart_suffix_udf(cleaned_col, col("contact_address_line1"))

        # Trim extra spaces resulting from removal
        cleaned_col = trim(regexp_replace(cleaned_col, r"\s+", " "))

        return df.withColumn(self.output_col, cleaned_col)