import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from .base_blocker import BaseBlocker

class GeoHashBlocker(BaseBlocker):
    def block(self, anchor_df: DataFrame, challenger_df: DataFrame) -> DataFrame:
        # 1. Create "Skinny" DFs. We temporarily drop the heavy SBERT embeddings
        # so Spark can join and shuffle at lightning speed.
        anchor_skinny = anchor_df.select("id", "geoHash", "contact_address_country_code")
        challenger_skinny = challenger_df.select("id", "geoHash", "contact_address_country_code")

        # 2. Explode the skinny DFs (Spark can explode tiny rows instantly)
        anchor_exploded = anchor_skinny.withColumn("match_geohash_a", F.explode("geoHash"))
        challenger_exploded = challenger_skinny.withColumn("match_geohash_c", F.explode("geoHash"))

        # 3. Fast Equi-Join
        joined_skinny = anchor_exploded.alias("a").join(
            challenger_exploded.alias("c"),
            (F.col("a.match_geohash_a") == F.col("c.match_geohash_c")) &
            (F.lower(F.col("a.contact_address_country_code")) == F.lower(F.col("c.contact_address_country_code"))),
            "inner"
        )

        # 4. Deduplicate IMMEDIATELY on the skinny pairs.
        # No heavy vectors are being shuffled here!
        unique_pairs = joined_skinny.select(
            F.col("a.id").alias("id_i"),
            F.col("c.id").alias("id_j")
        ).dropDuplicates(["id_i", "id_j"])

        # 5. The Fetch-Back! Join the unique pairs back to the original massive DFs
        # to retrieve the names, lat/longs, and SBERT embeddings.
        final_pairs = unique_pairs \
            .join(anchor_df.alias("a"), F.col("id_i") == F.col("a.id"), "inner") \
            .join(challenger_df.alias("c"), F.col("id_j") == F.col("c.id"), "inner")

        # 6. Dynamically rename to _i and _j as expected by the Scorer
        select_cols = []
        for col in anchor_df.columns:
            select_cols.append(F.col(f"a.{col}").alias(f"{col}_i"))
        for col in challenger_df.columns:
            select_cols.append(F.col(f"c.{col}").alias(f"{col}_j"))

        return final_pairs.select(*select_cols)