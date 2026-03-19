# hotel_pair_scorer_processor.py
from pyspark.sql import DataFrame
from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor
from hotel_data.pipeline.preprocessor.utils.address_utils import token_sort_score
from hotel_data.pipeline.preprocessor.utils.geo_utils import haversine
from hotel_data.pipeline.preprocessor.utils.name_utils import (
    enhanced_name_scorer,
    name_residual_udf,
    # get_numeric_penalty,
    JACCARD_ALGO,
    LCS_ALGO,
    LEVENSHTEIN_ALGO,
    CONTAINMENT_ALGO,
)
from hotel_data.config.scoring_config import ScoringConstants
from hotel_data.pipeline.preprocessor.utils.phone_number_utils import (
    normalize_phone_expr,
    arrays_overlap_check,
)
from hotel_data.pipeline.preprocessor.utils.star_ratings_utils import star_rating_score

from pyspark.sql import functions as F


def native_haversine(lat1_col, lon1_col, lat2_col, lon2_col):
    """Calculates geographic distance directly in the Spark JVM (C-level speed)"""
    # Convert string coordinates to double and then to radians
    lat1 = F.radians(F.col(lat1_col).cast("double"))
    lon1 = F.radians(F.col(lon1_col).cast("double"))
    lat2 = F.radians(F.col(lat2_col).cast("double"))
    lon2 = F.radians(F.col(lon2_col).cast("double"))

    # Haversine formula entirely using native PySpark functions
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = F.sin(dlat / 2.0) ** 2 + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2.0) ** 2
    c = 2 * F.asin(F.sqrt(a))
    r = 6371.0  # Radius of earth in kilometers

    return c * r


def get_cosine_similarity_expr(col_a, col_b):
    # Calculate Dot Product
    dot_product = F.aggregate(
        F.zip_with(col_a, col_b, lambda x, y: x * y), F.lit(0.0), lambda acc, x: acc + x
    )
    # Calculate Magnitudes
    mag_a = F.sqrt(F.aggregate(col_a, F.lit(0.0), lambda acc, x: acc + (x * x)))
    mag_b = F.sqrt(F.aggregate(col_b, F.lit(0.0), lambda acc, x: acc + (x * x)))

    # Cosine Similarity = Dot Product / (||a|| * ||b||)
    return F.when((mag_a > 0) & (mag_b > 0), dot_product / (mag_a * mag_b)).otherwise(
        F.lit(0.0)
    )


class CandidateScorer(BaseProcessor[DataFrame]):
    """
    Compute pairwise attribute scores for hotels grouped by geohash.
    Returns a DataFrame with id_i, id_j, geoHash, and attribute scores.
    """

    def __init__(self, id_col="id", geohash_col="geoHash"):
        self.id_col = id_col
        self.geohash_col = geohash_col

    def process(self, df: DataFrame, prefix: str = "") -> DataFrame:
        haversine_udf = F.udf(haversine, "double")

        # pairs_with_distance = df.withColumn(
        #     "geo_distance_km",
        #     haversine_udf(
        #         F.col("geoCode_lat_i").cast("double"),
        #         F.col("geoCode_long_i").cast("double"),
        #         F.col("geoCode_lat_j").cast("double"),
        #         F.col("geoCode_long_j").cast("double")
        #     )
        # )
        pairs_with_distance = df.withColumn(
            "geo_distance_km",
            native_haversine(
                "geoCode_lat_i", "geoCode_long_i", "geoCode_lat_j", "geoCode_long_j"
            ),
        )

        pairs_filtered = pairs_with_distance.filter(F.col("geo_distance_km") <= 0.5)
        name_udf = F.udf(enhanced_name_scorer, "float")

        containment = pairs_filtered.withColumn(
            "name_score_containment",
            name_udf(
                F.col("name_i"), F.col("name_j"), F.lit(CONTAINMENT_ALGO), F.lit(False)
            ),
        ).withColumn(
            "normalized_name_score_containment",
            name_udf(
                F.col("normalized_name_i"),
                F.col("normalized_name_j"),
                F.lit(CONTAINMENT_ALGO),
                F.lit(True),
            ),
        )

        jaccard_lcs_levenshtein = (
            containment.withColumn(
                "name_score_jaccard",
                name_udf(
                    F.col("name_i"),
                    F.col("name_j"),
                    F.lit(JACCARD_ALGO),
                    F.lit(False),
                ),
            )
            .withColumn(
                "normalized_name_score_jaccard",
                name_udf(
                    F.col("normalized_name_i"),
                    F.col("normalized_name_j"),
                    F.lit(JACCARD_ALGO),
                    F.lit(True),
                ),
            )
            .withColumn(
                "name_score_lcs",
                name_udf(
                    F.col("name_i"),
                    F.col("name_j"),
                    F.lit(LCS_ALGO),
                    F.lit(False),
                ),
            )
            .withColumn(
                "normalized_name_score_lcs",
                name_udf(
                    F.col("normalized_name_i"),
                    F.col("normalized_name_j"),
                    F.lit(LCS_ALGO),
                    F.lit(True),
                ),
            )
            .withColumn(
                "name_score_levenshtein",
                name_udf(
                    F.col("name_i"),
                    F.col("name_j"),
                    F.lit(LEVENSHTEIN_ALGO),
                    F.lit(False),
                ),
            )
            .withColumn(
                "normalized_name_score_levenshtein",
                name_udf(
                    F.col("normalized_name_i"),
                    F.col("normalized_name_j"),
                    F.lit(LEVENSHTEIN_ALGO),
                    F.lit(True),
                ),
            )
        )

        # penalty_udf = F.udf(get_numeric_penalty, FloatType())

        sbert_df = jaccard_lcs_levenshtein.withColumn(
            "raw_sbert_score",
            get_cosine_similarity_expr(
                F.col("name_embedding_i"), F.col("name_embedding_j")
            ).cast("float"),
        ).withColumn(
            "raw_norm_sbert_score",
            get_cosine_similarity_expr(
                F.col("normalized_name_embedding_i"),
                F.col("normalized_name_embedding_j"),
            ).cast("float"),
        )

        # HELPER: Check for Empty/Null Strings
        def is_empty(col_name):
            return F.col(col_name).isNull() | (F.trim(F.col(col_name)) == "")

        # 3. Apply Empty Logic + Penalty to SBERT
        sbert_penalized = sbert_df.withColumn(
            "name_score_sbert",
            F.when(
                is_empty("name_i") & is_empty("name_j"),
                F.lit(ScoringConstants.BOTH_EMPTY_SCORE),  # 0.5
            )
            .when(
                is_empty("name_i") | is_empty("name_j"),
                F.lit(ScoringConstants.ONE_SIDE_EMPTY_SCORE),  # 0.0
            )
            .otherwise(
                F.greatest(
                    F.lit(0.0),
                    # F.col("raw_sbert_score") - penalty_udf(F.col("name_i"), F.col("name_j"))
                    F.col("raw_sbert_score"),
                )
            )
            .cast("float"),
        ).withColumn(
            "normalized_name_score_sbert",
            F.when(
                # CASE: Both Normalized Names are Empty -> 0.5 (Matches Jaccard)
                is_empty("normalized_name_i") & is_empty("normalized_name_j"),
                F.lit(ScoringConstants.BOTH_EMPTY_SCORE),
            )
            .when(
                # CASE: One is Empty -> 0.0
                is_empty("normalized_name_i") | is_empty("normalized_name_j"),
                F.lit(ScoringConstants.ONE_SIDE_EMPTY_SCORE),
            )
            .otherwise(
                # CASE: Normal -> Cosine - Penalty
                F.greatest(
                    F.lit(0.0),
                    # F.col("raw_norm_sbert_score") - penalty_udf(F.col("normalized_name_i"),
                    #                                           F.col("normalized_name_j"))
                    F.col("raw_norm_sbert_score"),
                )
            )
            .cast("float"),
        )

        # We take the mean of Jaccard, LCS, Levenshtein, and SBERT
        df_with_averages = (
            sbert_penalized.withColumn(
                "average_name_score",
                (
                    F.col("name_score_jaccard")
                    + F.col("name_score_lcs")
                    + F.col("name_score_levenshtein")
                    + F.col("name_score_sbert")
                )
                / 4.0,
            )
            .withColumn(
                "name_residual_score",
                name_residual_udf(
                    F.col("normalized_name_i"),
                    F.col("normalized_name_j"),
                    F.col(
                        "normalized_name_score_jaccard"
                    ),  # Passing the existing score!
                ),
            )
            .withColumn(
                "average_normalized_name_score",
                (
                    F.col("normalized_name_score_jaccard")
                    + F.col("normalized_name_score_lcs")
                    + F.col("normalized_name_score_levenshtein")
                    + F.col("normalized_name_score_sbert")
                )
                / 4.0,
            )
        )
        # Clean up temporary columns
        df_final = df_with_averages.drop("raw_sbert_score", "raw_norm_sbert_score")

        # Handle cases where embedding might be null (fill with 0.0)
        sbert = df_final.fillna(0.0, subset=["name_score_sbert"])

        # Handle cases where embedding might be null (fill with 0.0)
        normalized_sbert = sbert.fillna(0.0, subset=["normalized_name_score_sbert"])

        # Register UDF
        token_sort_udf = F.udf(token_sort_score, "float")

        address_score = normalized_sbert.withColumn(
            "address_line1_score",
            token_sort_udf(
                F.col("contact_address_line1_i"), F.col("contact_address_line1_j")
            ),
        )

        address_score = (
            address_score.withColumn(
                "postal_code_match",
                F.when(
                    # Scenario 1: Missing Data -> Neutral Score
                    (F.col("contact_address_postalCode_i").isNull())
                    | (F.col("contact_address_postalCode_j").isNull()),
                    F.lit(0.5),
                )
                .when(
                    # Scenario 2: Both Present & Match -> Perfect Score
                    F.col("contact_address_postalCode_i")
                    == F.col("contact_address_postalCode_j"),
                    F.lit(1.0),
                )
                .otherwise(
                    # Scenario 3: Both Present & Different -> Mismatch Penalty
                    F.lit(0.0)
                )
                .cast("float"),
            )
            .withColumn(
                "country_match",
                F.when(
                    # Scenario 1: Missing Data -> Neutral Score
                    (F.col("contact_address_country_name_i").isNull())
                    | (F.col("contact_address_country_name_j").isNull()),
                    F.lit(0.5),
                )
                .when(
                    # Scenario 2: Both Present & Match -> Perfect Score
                    F.col("contact_address_country_name_i")
                    == F.col("contact_address_country_name_j"),
                    F.lit(1.0),
                )
                .otherwise(
                    # Scenario 3: Both Present & Different -> Mismatch Penalty
                    F.lit(0.0)
                )
                .cast("float"),
            )
            .withColumn(
                "address_sbert_score",
                get_cosine_similarity_expr(
                    F.col("address_embedding_i"), F.col("address_embedding_j")
                ).cast("float"),  # Ensure cast to match schema
            )
        )

        # Helper to check for "Missing Data" (Null OR Empty Array)
        def is_empty_or_null(col_name):
            return (F.col(col_name).isNull()) | (F.size(F.col(col_name)) == 0)

        phone_normalized = (
            address_score.withColumn(
                "norm_phones_i", normalize_phone_expr(F.col("contact_phones_i"))
            )
            .withColumn(
                "norm_phones_j", normalize_phone_expr(F.col("contact_phones_j"))
            )
            .withColumn(
                "norm_faxes_i", normalize_phone_expr(F.col("contact_fax_i"), 10)
            )
            .withColumn(
                "norm_faxes_j", normalize_phone_expr(F.col("contact_fax_j"), 10)
            )
        )

        # B. Calculate Scores with Neutral Fallback (0.5)
        df_scores = (
            phone_normalized.withColumn(
                "phone_match_score",
                F.when(
                    # Scenario 1: Data Missing on Either Side -> Neutral
                    is_empty_or_null("norm_phones_i")
                    | is_empty_or_null("norm_phones_j"),
                    F.lit(0.5),
                )
                .when(
                    # Scenario 2: Data Present & Overlaps -> Match
                    arrays_overlap_check("norm_phones_i", "norm_phones_j"),
                    F.lit(1.0),
                )
                .otherwise(
                    # Scenario 3: Data Present & No Overlap -> Mismatch
                    F.lit(0.0)
                )
                .cast("float"),
            )
            .withColumn(
                "email_match_score",
                F.when(
                    is_empty_or_null("contact_emails_i")
                    | is_empty_or_null("contact_emails_j"),
                    F.lit(0.5),
                )
                .when(
                    arrays_overlap_check("contact_emails_i", "contact_emails_j"),
                    F.lit(1.0),
                )
                .otherwise(F.lit(0.0))
                .cast("float"),
            )
            .withColumn(
                "fax_match_score",
                F.when(
                    is_empty_or_null("contact_fax_i")
                    | is_empty_or_null("contact_fax_j"),
                    F.lit(0.5),
                )
                .when(
                    arrays_overlap_check("contact_fax_i", "contact_fax_j"), F.lit(1.0)
                )
                .otherwise(F.lit(0.0))
                .cast("float"),
            )
        )

        # Ensure scores are 0.0 instead of NULL where comparison failed due to missing data
        address_score = df_scores.fillna(
            0.0, subset=["address_line1_score", "address_sbert_score"]
        )

        ratings_udf = F.udf(star_rating_score, "float")
        pairs_with_ratings_score = address_score.withColumn(
            "star_ratings_score",
            ratings_udf(F.col("starRating_i"), F.col("starRating_j")),
        )

        # print("👉 Pair within 500 m generation complete. First few neighbour pairs:")
        # pairs_with_ratings_score.show(2, truncate=False)

        cols_to_remove = [
            "norm_phones_i",
            "norm_phones_j",
            "norm_faxes_i",
            "norm_faxes_j",
        ]
        required_df = pairs_with_ratings_score.drop(*cols_to_remove)
        return required_df
