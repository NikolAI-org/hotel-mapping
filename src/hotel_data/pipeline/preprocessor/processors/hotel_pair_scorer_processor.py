# hotel_pair_scorer_processor.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor
from hotel_data.pipeline.preprocessor.utils.geo_utils import haversine
from hotel_data.pipeline.preprocessor.utils.name_utils import enhanced_name_scorer
from hotel_data.pipeline.preprocessor.utils.star_ratings_utils import star_rating_score


class HotelPairScorerProcessor(BaseProcessor):
    """
    Compute pairwise attribute scores for hotels grouped by geohash.
    Returns a DataFrame with id_i, id_j, geoHash, and attribute scores.
    """

    def __init__(self, id_col="id", geohash_col="geoHash"):
        self.id_col = id_col
        self.geohash_col = geohash_col

    def process(self, df: DataFrame) -> DataFrame:
        a = df.alias("a")
        b = df.alias("b")

        # Pair logic based on geohash overlap
        pairs = (
            a.join(
                b,
                F.size(
                    F.array_intersect(F.col(f"a.{self.geohash_col}"),
                                      F.col(f"b.{self.geohash_col}"))
                ) > 0
            )
            .filter(F.col("a.id") < F.col("b.id"))  # avoid self-join and reverse duplicates
            .select(
                F.col("a.id").alias("id_i"),
                F.col("b.id").alias("id_j"),
                F.col(f"a.providerHotelId").alias("providerHotelId_i"),
                F.col(f"b.providerHotelId").alias("providerHotelId_j"),
                F.col(f"a.name").alias("name_i"),
                F.col(f"b.name").alias("name_j"),
                F.col(f"a.geoCode_lat").alias("geoCode_lat_i"),
                F.col(f"a.geoCode_long").alias("geoCode_long_i"),
                F.col(f"b.geoCode_lat").alias("geoCode_lat_j"),
                F.col(f"b.geoCode_long").alias("geoCode_long_j"),
                F.col(f"a.starRating").alias("starRating_i"),
                F.col(f"b.starRating").alias("starRating_j"),
                F.array_intersect(
                    F.col("a.geohash"), F.col("b.geohash")
                ).alias("geo_intersection")
            )
        )

        print("👉 Pair generation complete. First few pairs:")
        pairs.show(20, truncate=False)

        haversine_udf = F.udf(haversine, "double")

        pairs_with_distance = pairs.withColumn(
            "geo_distance_km",
            haversine_udf(
                F.col("geoCode_lat_i").cast("double"),
                F.col("geoCode_long_i").cast("double"),
                F.col("geoCode_lat_j").cast("double"),
                F.col("geoCode_long_j").cast("double")
            )
        )

        pairs_filtered = pairs_with_distance.filter(F.col("geo_distance_km") <= 0.5)

        name_udf = F.udf(enhanced_name_scorer, "float")
        pairs_with_name_score = pairs_filtered.withColumn(
            "name_score",
            name_udf(
                F.col("name_i"),
                F.col("name_j")
            )
        )

        ratings_udf = F.udf(star_rating_score, "float")
        pairs_with_ratings_score = pairs_with_name_score.withColumn(
            "star_ratings_score",
            ratings_udf(
                F.col("starRating_i"),
                F.col("starRating_j")
            )
        )

        print("👉 Pair within 500 m generation complete. First few neighbour pairs:")
        pairs_with_ratings_score.show(20, truncate=False)

        cols_to_remove = ["geoCode_lat_i", "geoCode_lat_j", "geoCode_long_i", "geoCode_long_j", "geo_intersection", "starRating_i", "starRating_j"]
        required_df = pairs_with_ratings_score.drop(*cols_to_remove)

        return required_df

        # # Explode geoHash arrays for proper join
        # h1_exploded = h1.withColumn("gh", F.explode_outer("geoHash"))
        # h2_exploded = h2.withColumn("gh", F.explode_outer("geoHash"))
        #
        # pairs_df = (
        #     h1_exploded.alias("h1")
        #     .join(h2_exploded.alias("h2"), F.col("h1.gh") == F.col("h2.gh"))
        #     .filter(F.col("h1.id") < F.col("h2.id"))
        # )
        #
        # scored_pairs_df = pairs_df.select(
        #     F.col("h1.id").alias("id_i"),
        #     F.col("h2.id").alias("id_j"),
        #     F.array_distinct(F.flatten(F.array(F.col("h1.geoHash")))).cast(ArrayType(StringType(), True)).alias(
        #         "geoHash"),
        #     F.when(
        #         F.col("h1.contact_address_city_name") == F.col("h2.contact_address_city_name"),
        #         1.0
        #     ).otherwise(0.0).alias("city_score"),
        #     (1 - F.abs(F.col("h1.starRating").cast("double") - F.col("h2.starRating").cast("double")) / 5).alias(
        #         "rating_score"),
        #     F.lit(0.0).alias("name_score")
        # )
        #
        # scored_pairs_df = scored_pairs_df.withColumn(
        #     "total_score",
        #     (F.col("city_score") + F.col("rating_score") + F.col("name_score")) / 3
        # )
        #
        # return scored_pairs_df



