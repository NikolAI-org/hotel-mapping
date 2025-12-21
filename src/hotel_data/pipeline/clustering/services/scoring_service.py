# services/scoring_service.py - REWRITTEN THRESHOLD VERSION

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import col, lit, current_timestamp, when, struct
from hotel_data.config.scoring_config import ScoringConfig # Assuming this config can hold the new thresholds
from hotel_data.pipeline.clustering.core.exceptions import ScoringException
from hotel_data.pipeline.clustering.core.clustering_interfaces import ScoringStrategy
from functools import reduce
from pyspark.sql.column import Column

class ThresholdScoringStrategy(ScoringStrategy):
    """
    Threshold-based scoring implementation: a match requires ALL signals to meet 
    their minimum configurable threshold.
    """
    
    SCORING_VERSION = "v2.0-Threshold"
    
    SIGNAL_COLUMNS = [ 
                      "geo_distance_km", 
                      "name_score_jaccard",
                      "normalized_name_score_jaccard",
                      "name_score_lcs",
                      "normalized_name_score_lcs",
                      "name_score_levenshtein",
                      "normalized_name_score_levenshtein",
                      "name_score_sbert",
                      "normalized_name_score_sbert",
                      "star_ratings_score", 
                      "address_line1_score", 
                      "postal_code_match", 
                      "country_match", 
                      "address_sbert_score", 
                      "phone_match_score", 
                      "email_match_score", 
                      "fax_match_score" 
                    ]

    
    def __init__(self, config: ScoringConfig, logger):
        self.config = config
        self.logger = logger
        
        if not config.validate():
            raise ValueError("ScoringConfig validation failed")
        
        self.logger.log("INFO", "ThresholdScoringStrategy initialized")
        
        self.signal_thresholds = {
            "name_score_jaccard": self.config.thresholds.get('name_score_jaccard', 0.15),
            "normalized_name_score_jaccard": self.config.thresholds.get('normalized_name_score_jaccard', 0.15),
            "name_score_lcs": self.config.thresholds.get('name_score_lcs', 0.15),
            "normalized_name_score_lcs": self.config.thresholds.get('normalized_name_score_lcs', 0.15),
            "name_score_levenshtein": self.config.thresholds.get('name_score_levenshtein', 0.15),
            "normalized_name_score_levenshtein": self.config.thresholds.get('normalized_name_score_levenshtein', 0.15),
            "name_score_sbert": self.config.thresholds.get('name_score_sbert', 0.0),
            "normalized_name_score_sbert": self.config.thresholds.get('normalized_name_score_sbert', 0.0),
            "star_ratings_score": self.config.thresholds.get('star_ratings_score', 0.7),
            "address_line1_score": self.config.thresholds.get('address_line1_score', 0.4),
            "postal_code_match": self.config.thresholds.get('postal_code_match', 0.6),
            "country_match": self.config.thresholds.get('country_match', 0.9),
            "address_sbert_score": self.config.thresholds.get('address_sbert_score', 0.0),
            "phone_match_score": self.config.thresholds.get('phone_match_score', 0.008),
            "email_match_score": self.config.thresholds.get('email_match_score', 0.00004),
            "fax_match_score": self.config.thresholds.get('fax_match_score', 0.0),
            "geo_distance_km": self.config.thresholds.get('geo_distance_km', 0.5)
        }
        
        # Default comparators: all >= except geo_distance_km which uses <=
        self.signal_comparators = {
            "geo_distance_km": self.config.comparators.get("geo_distance_km", "lte"),
            "name_score_jaccard_lcs": self.config.comparators.get("name_score_jaccard_lcs", "gte"),
            "normalized_name_score_jaccard": self.config.comparators.get("normalized_name_score_jaccard", "gte"),
            "normalized_name_score_lcs": self.config.comparators.get("normalized_name_score_lcs", "gte"),
            "normalized_name_score_levenshtein": self.config.comparators.get("normalized_name_score_levenshtein", "gte"),
            "name_score_sbert": self.config.comparators.get("name_score_sbert", "gte"),
            "normalized_name_score_sbert": self.config.comparators.get("normalized_name_score_sbert", "gte"),
            "star_ratings_score": self.config.comparators.get("star_ratings_score", "gte"),
            "address_line1_score": self.config.comparators.get("address_line1_score", "gte"),
            "postal_code_match": self.config.comparators.get("postal_code_match", "gte"),
            "country_match": self.config.comparators.get("country_match", "gte"),
            "address_sbert_score": self.config.comparators.get("address_sbert_score", "gte"),
            "phone_match_score": self.config.comparators.get("phone_match_score", "gte"),
            "email_match_score": self.config.comparators.get("email_match_score", "gte"),
            "fax_match_score": self.config.comparators.get("fax_match_score", "gte"),
        }

    
    def score(self, pairs_df: DataFrame) -> DataFrame:
        """
        Main scoring method for threshold-based classification.
        """
        try:
            self.logger.log("INFO", "Starting threshold-based scoring (All Signals Must Pass)")
            
            df = pairs_df 
            
            # ═════════════════════════════════════════════════════════════
            # STEP 1: CALCULATE PASS/FAIL FOR EACH SIGNAL
            # ═════════════════════════════════════════════════════════════
            df = self._add_signal_pass_status(df)
            
            # ═════════════════════════════════════════════════════════════
            # STEP 2: APPLY ALL-PASS LOGIC (CORE CLASSIFICATION)
            # ═════════════════════════════════════════════════════════════
            df = self._add_match_classification(df)
            
            # ═════════════════════════════════════════════════════════════
            # STEP 3: METADATA & CLEANUP
            # ═════════════════════════════════════════════════════════════
            df = (df
                .withColumn("scoring_version", lit(self.SCORING_VERSION))
                .withColumn("scoring_timestamp", current_timestamp())
            )
            
            self.logger.log("INFO", "Scoring completed successfully")
            return df
        
        except Exception as e:
            self.logger.log("ERROR", f"Threshold Scoring failed: {str(e)}")
            raise ScoringException(f"Threshold Scoring failed: {str(e)}")
    
    def _add_signal_pass_status(self, df: DataFrame) -> DataFrame:
        """
        Adds a boolean column for each signal indicating if it met its threshold,
        using a configurable comparator per signal:
        - "gte": col >= threshold
        - "lte": col <= threshold
        
        Also adds a single JSON column with all scoring details:
        {
        "geo_distance_km": {
            "actual": 0.3,
            "threshold": 0.5,
            "comparator": "lte",
            "passed": true
        },
        "name_score_jaccard_lcs": {
            "actual": 0.18,
            "threshold": 0.15,
            "comparator": "gte",
            "passed": true
        },
        ...
        }
        """
        self.logger.log("INFO", "Evaluating individual signal thresholds (configurable)...")

        result = df
        scoring_metadata_structs = []
        
        for signal_col in self.SIGNAL_COLUMNS:
            threshold = self.signal_thresholds[signal_col]
            comparator = self.signal_comparators.get(signal_col, "gte")

            # Build the comparison expression
            if comparator == "lte":
                cmp_expr = col(signal_col) <= F.lit(threshold)
            elif comparator == "gt":
                cmp_expr = col(signal_col) > F.lit(threshold)
            elif comparator == "lt":
                cmp_expr = col(signal_col) < F.lit(threshold)
            else:
                # default / "gte"
                cmp_expr = col(signal_col) >= F.lit(threshold)

            pass_expr = col(signal_col).isNotNull() & cmp_expr

            # ✅ Add pass/fail column (still keep for filtering/debugging)
            result = result.withColumn(f"{signal_col}_passed", pass_expr)

            # ✅ Build struct for JSON metadata
            signal_metadata = struct(
                col(signal_col).alias("actual"),
                F.lit(threshold).alias("threshold"),
                F.lit(comparator).alias("comparator"),
                pass_expr.alias("passed")
            )
            
            scoring_metadata_structs.append((signal_col, signal_metadata))

        # ✅ Combine all signal metadata into a single column (map<string, struct>)
        # This creates: {"geo_distance_km": {...}, "name_score_jaccard_lcs": {...}, ...}
        col_name = "metadata"
        metadata_map = F.create_map(
            *[
                item
                for col_name, metadata_struct in scoring_metadata_structs
                for item in (F.lit(col_name), metadata_struct)
            ]
        )

        
        result = result.withColumn("scoring_metadata", F.to_json(metadata_map))

        return result

    def _add_match_classification(self, df: DataFrame) -> DataFrame:
        """
        Classifies a pair as MATCHED only if ALL signals passed their thresholds.
        """
        self.logger.log("INFO", "Applying 'All Signals Must Pass' logic...")
        
        # Create a list of all boolean 'passed' columns
        pass_columns = [col(f"{s}_passed") for s in self.SIGNAL_COLUMNS]
        
        # Combine all boolean columns using the AND operator (all must be TRUE)
        # We also need to handle cases where ALL signals were NULL (not ideal, but safer)
        # The 'pass_columns' expression already handles NULLs in the signal columns.
        all_passed_expr = reduce(lambda a, b: a & b, pass_columns)
        
        # The pair is considered MATCHED only if all signals passed.
        df = (df
            .withColumn("is_matched", all_passed_expr)
            .withColumn(
                "match_status", 
                when(col("is_matched"), lit("MATCHED")).otherwise(lit("UNMATCHED"))
            )
        )
        
        match_count = df.filter(col('match_status') == 'MATCHED').count()
        unmatch_count = df.count() - match_count
        
        self.logger.log(
            "INFO", 
            f"Classification complete: {match_count} matched, {unmatch_count} unmatched."
        )
        
        # OPTIMIZATION: Combine match status and a simplified "score" (1.0 for Match, 0.0 for Unmatch)
        df = df.withColumn(
            "match_score", 
            when(col("is_matched"), F.lit(1.0)).otherwise(F.lit(0.0))
        )
        
        return df

    # NOTE: The methods below from the original CompositeScoringStrategy 
    # (e.g., _calculate_weighted_scores, _add_composite_and_confidence, 
    # _add_signal_quality, _add_score_components) are no longer relevant 
    # for this simplified threshold-based approach and are omitted for optimization.
    
    # You might want to keep utility methods like _validate_input_schema 
    # and _clip_signals_to_range (which should be uncommented in `score`).
    
    def _validate_input_schema(self, df: DataFrame) -> None:
        """Validate required columns exist (kept for safety)"""
        required = ["id_i", "id_j"] + self.SIGNAL_COLUMNS
        missing = set(required) - set(df.columns)
        
        if missing:
            raise ScoringException(f"Missing columns: {missing}")
        
    def _clip_signals_to_range(self, df: DataFrame) -> DataFrame:
        """Clip all signal values to [0, 1] range (kept for data quality)"""
        # ... (implementation from original file remains the same) ...
        self.logger.log("INFO", "Clipping signals to [0, 1]...")
        
        result = df
        
        for signal_col in self.SIGNAL_COLUMNS:
            clipped_expr = F.when(
                col(signal_col).isNull(),
                F.lit(None)
            ).otherwise(
                F.greatest(
                    F.lit(0.0),
                    F.least(col(signal_col), F.lit(1.0))
                )
            )
            
            result = result.withColumn(signal_col, clipped_expr)
        
        self.logger.log("INFO", "Signal clipping complete")
        return result