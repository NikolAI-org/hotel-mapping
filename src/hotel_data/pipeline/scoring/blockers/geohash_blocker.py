import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from .base_blocker import BaseBlocker


class GeoHashBlocker(BaseBlocker):
    def block(self, anchor_df: DataFrame, challenger_df: DataFrame) -> DataFrame:
        # 1. Create "Skinny" DFs. We temporarily drop the heavy SBERT embeddings
        # so Spark can join and shuffle at lightning speed.
        anchor_skinny = anchor_df.select(
            "uid", "geoHash", "contact_address_country_code"
        )
        challenger_skinny = challenger_df.select(
            "uid", "geoHash", "contact_address_country_code"
        )

        # 2. Explode the skinny DFs (Spark can explode tiny rows instantly)
        anchor_exploded = anchor_skinny.withColumn(
            "match_geohash_a", F.explode("geoHash")
        )
        challenger_exploded = challenger_skinny.withColumn(
            "match_geohash_c", F.explode("geoHash")
        )

        # 3. Fast Equi-Join
        joined_skinny = anchor_exploded.alias("a").join(
            challenger_exploded.alias("c"),
            (F.col("a.match_geohash_a") == F.col("c.match_geohash_c"))
            & (
                F.lower(F.col("a.contact_address_country_code"))
                == F.lower(F.col("c.contact_address_country_code"))
            ),
            "inner",
        )

        # 4. Deduplicate IMMEDIATELY on the skinny pairs.
        # No heavy vectors are being shuffled here!
        unique_pairs = joined_skinny.select(
            F.col("a.uid").alias("uid_i"), F.col("c.uid").alias("uid_j")
        ).dropDuplicates(["uid_i", "uid_j"])

        return unique_pairs
