# address_combiner_processor.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    regexp_replace,
    lower,
    trim,
    base64
)

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor
from hotel_data.pipeline.preprocessor.processors.smart_cleaner import normalize_real_estate_udf

class AddressCombinerProcessor(BaseProcessor[DataFrame]):
    def __init__(self, address_fields: list[str], output_col="combined_address"):
        self.address_fields = address_fields
        self.output_col = output_col
        self.name_col = "name"
        self.uid = "uid"

    def process(self, df: DataFrame, prefix: str = "") -> DataFrame:
        combined_address_expr = trim(
            lower(
                regexp_replace(
                    concat_ws(" ", *[col(f) for f in self.address_fields]),
                    r"[^a-zA-Z0-9\s]",
                    ""
                )
            )
        )

        combined_address_expr = normalize_real_estate_udf(combined_address_expr)
        
        # Step 2: combine name + address (with delimiter)
        combined_entity_expr = concat_ws(
            "||",
            trim(lower(col("providerId"))),
            trim(lower(col(self.name_col))),
            combined_address_expr
        )
        
        return (
            df
            .withColumn(self.output_col, combined_address_expr)
            .withColumn(self.uid, base64(combined_entity_expr))
        )
        
        # return df.withColumn(
        #     self.output_col,
        #     regexp_replace(
        #         concat_ws(" ", *[col(f) for f in self.address_fields]),
        #         r"[^a-zA-Z0-9\s]",
        #         ""
        #     )
        # )
