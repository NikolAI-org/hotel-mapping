import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from .base_blocker import BaseBlocker


class PostalCodeBlocker(BaseBlocker):
    def block(self, anchor_df: DataFrame, challenger_df: DataFrame) -> DataFrame:
        # Fallback exact match on postal code + country
        joined = anchor_df.alias("a").join(
            challenger_df.alias("c"),
            (
                F.col("a.contact_address_postalCode")
                == F.col("c.contact_address_postalCode")
            )
            & (
                F.lower(F.col("a.contact_address_country_code"))
                == F.lower(F.col("c.contact_address_country_code"))
            ),
            "inner",
        )

        # Dynamically rename all columns to _i (anchor) and _j (challenger)
        select_cols = []
        for col in anchor_df.columns:
            select_cols.append(F.col(f"a.{col}").alias(f"{col}_i"))
        for col in challenger_df.columns:
            select_cols.append(F.col(f"c.{col}").alias(f"{col}_j"))

        return joined.select(*select_cols)
