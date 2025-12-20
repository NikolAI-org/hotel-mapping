# hotel_pair_scorer_processor.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from sympy.physics.quantum.gate import normalized

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor
from hotel_data.pipeline.preprocessor.utils.address_utils import token_sort_score
from hotel_data.pipeline.preprocessor.utils.geo_utils import haversine
from hotel_data.pipeline.preprocessor.utils.name_utils import enhanced_name_scorer
from hotel_data.pipeline.preprocessor.utils.phone_number_utils import normalize_phone_expr, arrays_overlap_check
from hotel_data.pipeline.preprocessor.utils.star_ratings_utils import star_rating_score

from pyspark.sql import functions as F



def get_cosine_similarity_expr(col_a, col_b):
    # Calculate Dot Product
    dot_product = F.aggregate(
        F.zip_with(col_a, col_b, lambda x, y: x * y),
        F.lit(0.0),
        lambda acc, x: acc + x
    )
    # Calculate Magnitudes
    mag_a = F.sqrt(F.aggregate(col_a, F.lit(0.0), lambda acc, x: acc + (x * x)))
    mag_b = F.sqrt(F.aggregate(col_b, F.lit(0.0), lambda acc, x: acc + (x * x)))
    
    # Cosine Similarity = Dot Product / (||a|| * ||b||)
    return F.when((mag_a > 0) & (mag_b > 0), dot_product / (mag_a * mag_b)).otherwise(F.lit(0.0))

class HotelPairScorerProcessor(BaseProcessor[DataFrame]):
    """
    Compute pairwise attribute scores for hotels grouped by geohash.
    Returns a DataFrame with id_i, id_j, geoHash, and attribute scores.
    """

    def __init__(self, id_col="id", geohash_col="geoHash"):
        self.id_col = id_col
        self.geohash_col = geohash_col

    def process(self, df: DataFrame, prefix: str = "") -> DataFrame:
        df = df.withColumn("unique_key",
                                       F.concat(F.col("providerId"), F.lit("_"), F.col("providerHotelId")))
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
            .filter(F.col("a.unique_key") < F.col("b.unique_key"))  # avoid self-join and reverse duplicates
            .select(
                F.col("a.id").alias("id_i"),
                F.col("b.id").alias("id_j"),
                F.col(f"a.providerHotelId").alias("providerHotelId_i"),
                F.col(f"b.providerHotelId").alias("providerHotelId_j"),
                F.col(f"a.name").alias("name_i"),
                F.col(f"b.name").alias("name_j"),
                F.col(f"a.uid").alias("uid_i"),
                F.col(f"b.uid").alias("uid_j"),
                F.col(f"a.normalized_name").alias("normalized_name_i"),
                F.col(f"b.normalized_name").alias("normalized_name_j"),
                F.col(f"a.name_embedding").alias("name_embedding_i"),
                F.col(f"b.name_embedding").alias("name_embedding_j"),
                F.col(f"a.normalized_name_embedding").alias("normalized_name_embedding_i"),
                F.col(f"b.normalized_name_embedding").alias("normalized_name_embedding_j"),
                F.col(f"a.geoCode_lat").alias("geoCode_lat_i"),
                F.col(f"a.geoCode_long").alias("geoCode_long_i"),
                F.col(f"b.geoCode_lat").alias("geoCode_lat_j"),
                F.col(f"b.geoCode_long").alias("geoCode_long_j"),
                F.col(f"a.starRating").alias("starRating_i"),
                F.col(f"b.starRating").alias("starRating_j"),
                F.col(f"a.contact_address_line1").alias("contact_address_line1_i"),
                F.col(f"b.contact_address_line1").alias("contact_address_line1_j"),
                F.col(f"a.contact_address_postalCode").alias("contact_address_postalCode_i"),
                F.col(f"b.contact_address_postalCode").alias("contact_address_postalCode_j"),
                F.col(f"a.contact_address_country_name").alias("contact_address_country_name_i"),
                F.col(f"b.contact_address_country_name").alias("contact_address_country_name_j"),
                F.col(f"a.address_embedding").alias("address_embedding_i"),
                F.col(f"b.address_embedding").alias("address_embedding_j"),
                F.col(f"a.combined_address").alias("combined_address_i"),
                F.col(f"b.combined_address").alias("combined_address_j"),
                F.col(f"a.contact_phones").alias("contact_phones_i"),
                F.col(f"b.contact_phones").alias("contact_phones_j"),
                F.col(f"a.contact_fax").alias("contact_fax_i"),
                F.col(f"b.contact_fax").alias("contact_fax_j"),
                F.col(f"a.contact_emails").alias("contact_emails_i"),
                F.col(f"b.contact_emails").alias("contact_emails_j"),
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
        jaccard_lcs = pairs_filtered.withColumn(
            "name_score_jaccard_lcs",
            name_udf(
                F.col("name_i"),
                F.col("name_j")
            )
        )

        normalized_jaccard_lcs = jaccard_lcs.withColumn(
            "normalized_name_score_jaccard_lcs",
            name_udf(
                F.col("normalized_name_i"),
                F.col("normalized_name_j")
            )
        )

        sbert = normalized_jaccard_lcs.withColumn(
            "name_score_sbert",
            get_cosine_similarity_expr(F.col("name_embedding_i"), F.col("name_embedding_j")).cast("float")
        )
        # Handle cases where embedding might be null (fill with 0.0)
        sbert = sbert.fillna(0.0, subset=["name_score_sbert"])

        normalized_sbert = sbert.withColumn(
            "normalized_name_score_sbert",
            get_cosine_similarity_expr(F.col("normalized_name_embedding_i"), F.col("normalized_name_embedding_j")).cast("float")
        )
        # Handle cases where embedding might be null (fill with 0.0)
        normalized_sbert = normalized_sbert.fillna(0.0, subset=["normalized_name_score_sbert"])

        # Register UDF
        token_sort_udf = F.udf(token_sort_score, "float")

        address_score = normalized_sbert.withColumn("address_line1_score", token_sort_udf(
            F.col("contact_address_line1_i"),
            F.col("contact_address_line1_j")
        ))

        address_score = address_score.withColumn(
            "postal_code_match",
            F.when(
                # Scenario 1: Missing Data -> Neutral Score
                (F.col("contact_address_postalCode_i").isNull()) |
                (F.col("contact_address_postalCode_j").isNull()),
                F.lit(0.5)
            ).when(
                # Scenario 2: Both Present & Match -> Perfect Score
                F.col("contact_address_postalCode_i") == F.col("contact_address_postalCode_j"),
                F.lit(1.0)
            ).otherwise(
                # Scenario 3: Both Present & Different -> Mismatch Penalty
                F.lit(0.0)
            ).cast("float")
        ).withColumn(
            "country_match",
            F.when(
                # Scenario 1: Missing Data -> Neutral Score
                (F.col("contact_address_country_name_i").isNull()) |
                (F.col("contact_address_country_name_j").isNull()),
                F.lit(0.5)
            ).when(
                # Scenario 2: Both Present & Match -> Perfect Score
                F.col("contact_address_country_name_i") == F.col("contact_address_country_name_j"),
                F.lit(1.0)
            ).otherwise(
                # Scenario 3: Both Present & Different -> Mismatch Penalty
                F.lit(0.0)
            ).cast("float")
        ).withColumn(
            "address_sbert_score",
            get_cosine_similarity_expr(
                F.col("address_embedding_i"),
                F.col("address_embedding_j")
            ).cast("float") # Ensure cast to match schema
        )

        # Helper to check for "Missing Data" (Null OR Empty Array)
        def is_empty_or_null(col_name):
            return (F.col(col_name).isNull()) | (F.size(F.col(col_name)) == 0)

        phone_normalized = address_score.withColumn("norm_phones_i", normalize_phone_expr(F.col("contact_phones_i"))) \
            .withColumn("norm_phones_j", normalize_phone_expr(F.col("contact_phones_j"))) \
            .withColumn("norm_faxes_i", normalize_phone_expr(F.col("contact_fax_i"), 10)) \
            .withColumn("norm_faxes_j", normalize_phone_expr(F.col("contact_fax_j"), 10))

        # B. Calculate Scores with Neutral Fallback (0.5)
        df_scores = phone_normalized.withColumn(
            "phone_match_score",
            F.when(
                # Scenario 1: Data Missing on Either Side -> Neutral
                is_empty_or_null("norm_phones_i") | is_empty_or_null("norm_phones_j"),
                F.lit(0.5)
            ).when(
                # Scenario 2: Data Present & Overlaps -> Match
                arrays_overlap_check("norm_phones_i", "norm_phones_j"),
                F.lit(1.0)
            ).otherwise(
                # Scenario 3: Data Present & No Overlap -> Mismatch
                F.lit(0.0)
            ).cast("float")
        ).withColumn(
            "email_match_score",
            F.when(
                is_empty_or_null("contact_emails_i") | is_empty_or_null("contact_emails_j"),
                F.lit(0.5)
            ).when(
                arrays_overlap_check("contact_emails_i", "contact_emails_j"),
                F.lit(1.0)
            ).otherwise(F.lit(0.0)).cast("float")
        ).withColumn(
            "fax_match_score",
            F.when(
                is_empty_or_null("contact_fax_i") | is_empty_or_null("contact_fax_j"),
                F.lit(0.5)
            ).when(
                arrays_overlap_check("contact_fax_i", "contact_fax_j"),
                F.lit(1.0)
            ).otherwise(F.lit(0.0)).cast("float")
        )

        # Ensure scores are 0.0 instead of NULL where comparison failed due to missing data
        address_score = df_scores.fillna(0.0, subset=["address_line1_score", "address_sbert_score"])

        ratings_udf = F.udf(star_rating_score, "float")
        pairs_with_ratings_score = address_score.withColumn(
            "star_ratings_score",
            ratings_udf(
                F.col("starRating_i"),
                F.col("starRating_j")
            )
        )

        print("👉 Pair within 500 m generation complete. First few neighbour pairs:")
        pairs_with_ratings_score.show(20, truncate=False)

        cols_to_remove = ["geoCode_lat_i", "geoCode_lat_j", "geoCode_long_i", "geoCode_long_j", "geo_intersection"
            , "starRating_i", "starRating_j", "name_embedding_i", "name_embedding_j"
            , "normalized_name_embedding_i", "normalized_name_embedding_j", "contact_address_line1_i", "contact_address_line1_j"
            , "contact_address_postalCode_i", "contact_address_postalCode_j", "contact_address_country_name_i", "contact_address_country_name_j"
            , "address_embedding_i", "address_embedding_j", "contact_phones_i", "contact_phones_j", "contact_fax_i", "contact_fax_j"
            , "contact_emails_i", "contact_emails_j", "norm_phones_i", "norm_phones_j", "norm_faxes_i", "norm_faxes_j"]
        required_df = pairs_with_ratings_score.drop(*cols_to_remove)
        return required_df
