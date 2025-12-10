# services/scoring_service.py - COMPLETE UPDATED VERSION

from operator import add
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, struct
)
from hotel_data.config.scoring_config import ScoringConfig
from hotel_data.pipeline.clustering.core.exceptions import ScoringException
from hotel_data.pipeline.clustering.core.clustering_interfaces import ScoringStrategy
from functools import reduce
from pyspark.sql.column import Column

class CompositeScoringStrategy(ScoringStrategy):
    """Weighted composite scoring implementation with robust validation"""
    
    SCORING_VERSION = "v1.0"
    
    SIGNAL_COLUMNS = [
        "geo_distance_km",
        "normalized_name_score_sbert",
        "name_score_jaccard_lcs",
        "star_ratings_score",
        "address_sbert_score",
        "phone_match_score",
        "postal_code_match",
        "country_match",
        "email_match_score"
    ]
    
    def __init__(self, config: ScoringConfig, logger):
        self.config = config
        self.logger = logger
        
        if not config.validate():
            raise ValueError("ScoringConfig validation failed")
        
        self.logger.log("INFO", "CompositeScoringStrategy initialized")
    
    def score(self, pairs_df: DataFrame) -> DataFrame:
        """
        Main scoring method with data cleaning
        
        INPUT:  DataFrame with 10 columns (id_i, id_j, 8 signals)
        OUTPUT: DataFrame with 26 columns (all above + weights, score, etc.)
        
        Processes:
        1. Validate schema
        2. Clip signals to [0, 1]
        3. Validate ranges (tolerant)
        4. Report data quality
        5. Calculate weights
        6. Composite + confidence
        7. Exclusion rules
        8. Signal quality
        9. Score components
        10. Metadata
        """
        try:
            self.logger.log("INFO", "Starting scoring")
            
            # ═════════════════════════════════════════════════════════════
            # STEP 1: VALIDATE INPUT SCHEMA
            # ═════════════════════════════════════════════════════════════
            self._validate_input_schema(pairs_df)
            self.logger.log("INFO", "Input schema validated")
            
            # ═════════════════════════════════════════════════════════════
            # STEP 1b: CLIP SIGNALS TO [0, 1] (NEW!)
            # ═════════════════════════════════════════════════════════════
            df = self._clip_signals_to_range(pairs_df)
            self.logger.log("INFO", "Signals clipped to [0, 1]")
            
            # ═════════════════════════════════════════════════════════════
            # STEP 2: REPORT DATA QUALITY
            # ═════════════════════════════════════════════════════════════
            self._report_data_quality(df)
            
            # ═════════════════════════════════════════════════════════════
            # STEP 3: VALIDATE RANGES (Tolerant)
            # ═════════════════════════════════════════════════════════════
            self._validate_signal_ranges(df)
            self.logger.log("INFO", "Signals validated")
            
            # ═════════════════════════════════════════════════════════════
            # STEP 4: CALCULATE WEIGHTED SIGNALS
            # ═════════════════════════════════════════════════════════════
            df = self._calculate_weighted_scores(df)
            
            # ═════════════════════════════════════════════════════════════
            # STEP 5: COMPOSITE SCORE + CONFIDENCE
            # ═════════════════════════════════════════════════════════════
            df = self._add_composite_and_confidence(df)
            
            # ═════════════════════════════════════════════════════════════
            # STEP 6: EXCLUSION RULES
            # ═════════════════════════════════════════════════════════════
            df = self._add_exclusion_rules(df)
            
            # ═════════════════════════════════════════════════════════════
            # STEP 7: SIGNAL QUALITY
            # ═════════════════════════════════════════════════════════════
            df = self._add_signal_quality(df)
            
            # ═════════════════════════════════════════════════════════════
            # STEP 8: SCORE COMPONENTS
            # ═════════════════════════════════════════════════════════════
            df = self._add_score_components(df)
            
            # ═════════════════════════════════════════════════════════════
            # STEP 9: METADATA
            # ═════════════════════════════════════════════════════════════
            df = (df
                .withColumn("scoring_version", lit(self.SCORING_VERSION))
                .withColumn("scoring_timestamp", current_timestamp())
            )
            
            self.logger.log("INFO", "Scoring completed successfully")
            return df
        
        except Exception as e:
            self.logger.log("ERROR", f"Scoring failed: {str(e)}")
            raise ScoringException(f"Scoring failed: {str(e)}")
    
    def _validate_input_schema(self, df: DataFrame) -> None:
        """Validate required columns exist"""
        required = ["id_i", "id_j"] + self.SIGNAL_COLUMNS
        missing = set(required) - set(df.columns)
        
        if missing:
            raise ScoringException(f"Missing columns: {missing}")
    
    def _clip_signals_to_range(self, df: DataFrame) -> DataFrame:
        """
        Clip all signal values to [0, 1] range
        
        Handles:
        - Values < 0 → clipped to 0
        - Values > 1 → clipped to 1
        - NULL values → preserved
        """
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
    
    def _validate_signal_ranges(self, df: DataFrame) -> None:
        """
        Validate and report signal ranges (TOLERANT - no blocking)
        
        Reports:
        - NULL counts
        - Out-of-range percentages
        - Min/max values
        """
        self.logger.log("INFO", "Validating signal ranges...")
        
        for signal_col in self.SIGNAL_COLUMNS:
            null_count = df.filter(col(signal_col).isNull()).count()
            
            # These should be 0 after clipping
            below_zero = df.filter(
                (col(signal_col).isNotNull()) & 
                (col(signal_col) < 0.0)
            ).count()
            
            above_one = df.filter(
                (col(signal_col).isNotNull()) & 
                (col(signal_col) > 1.0)
            ).count()
            
            if null_count > 0 or below_zero > 0 or above_one > 0:
                self.logger.log(
                    "WARNING",
                    f"Signal '{signal_col}' has issues (after clipping)",
                    null_count=null_count,
                    below_zero=below_zero,
                    above_one=above_one
                )
        
        self.logger.log("INFO", "Signal validation complete")
    
    def _report_data_quality(self, df: DataFrame) -> None:
        """Report data quality metrics"""
        total_rows = df.count()
        self.logger.log("INFO", "Data Quality Report:")
        self.logger.log("INFO", f"  Total rows: {total_rows}")
        
        for signal_col in self.SIGNAL_COLUMNS:
            null_count = df.filter(col(signal_col).isNull()).count()
            non_null_df = df.filter(col(signal_col).isNotNull())

            if non_null_df.count() > 0:
                stats = non_null_df.select(
                    F.min(signal_col).alias("min"),
                    F.max(signal_col).alias("max"),
                    F.avg(signal_col).alias("avg"),
                ).collect()

                row = stats[0]  # Row object

                self.logger.log(
                    "INFO",
                    f"  {signal_col}",
                    null_count=null_count,
                    min=round(float(row["min"]), 4),
                    max=round(float(row["max"]), 4),
                    avg=round(float(row["avg"]), 4),
                )
    
    def _calculate_weighted_scores(self, df: DataFrame) -> DataFrame:
        """Create weighted score columns"""
        result = df
        
        for signal_col in self.SIGNAL_COLUMNS:
            weight = self.config.weights[signal_col]
            weighted_col = f"{signal_col}_weighted"
            
            result = result.withColumn(
                weighted_col,
                when(
                    col(signal_col).isNull(),
                    F.lit(None)
                ).otherwise(
                    col(signal_col) * F.lit(weight)
                )
            )
        
        return result
    
    def _add_composite_and_confidence(self, df: DataFrame) -> DataFrame:
        """Calculate composite score + confidence level"""
        
        weighted_cols = [f"{col_name}_weighted" for col_name in self.SIGNAL_COLUMNS]
        weighted_exprs = [col(w) for w in weighted_cols]
        
        weighted_array = F.array(*weighted_exprs)
        all_non_null = F.forall(weighted_array, lambda x: x.isNotNull())
        composite_sum = reduce(add, weighted_exprs)
        
        composite_expr = (
            F.when(all_non_null, composite_sum)
            .otherwise(F.lit(None))
        )
        
        df = df.withColumn("composite_score", composite_expr)
        
        confidence_expr = (
            when(col("composite_score").isNull(), lit("UNCERTAIN"))
            .when(col("composite_score") >= 0.85, lit("HIGH"))
            .when(col("composite_score") >= 0.70, lit("MEDIUM"))
            .when(col("composite_score") >= 0.55, lit("LOW"))
            .otherwise(lit("UNCERTAIN"))
        )
        
        df = df.withColumn("confidence_level", confidence_expr)
        
        individual_scores_expr = F.create_map(
            *[item for pair in [(lit(col_name), col(col_name))
              for col_name in self.SIGNAL_COLUMNS] for item in pair]
        )
        
        df = df.withColumn("individual_scores", individual_scores_expr)
        
        return df
    
    def _add_exclusion_rules(self, df: DataFrame) -> DataFrame:
        """Apply exclusion rules"""
        
        meets_rules = lit(True)
        
        if "max_geo_distance_km" in self.config.exclusion_rules:
            max_dist = self.config.exclusion_rules["max_geo_distance_km"]
            meets_rules = meets_rules & (col("geo_distance_km") <= max_dist)
        
        if "min_country_match" in self.config.exclusion_rules:
            min_country = self.config.exclusion_rules["min_country_match"]
            meets_rules = meets_rules & (col("country_match") >= min_country)
        
        if "min_normalized_name_score" in self.config.exclusion_rules:
            min_name = self.config.exclusion_rules["min_normalized_name_score"]
            meets_rules = meets_rules & (col("normalized_name_score_sbert") >= min_name)
        
        return df.withColumn("meets_exclusion_rules", meets_rules)
    
    def _add_signal_quality(self, df: DataFrame) -> DataFrame:
        """Calculate signal quality metrics"""
        
        quality_expr = struct(
            F.coalesce(
                F.when(col("geo_distance_km").isNull(), F.lit(1)).otherwise(F.lit(0)) +
                F.when(col("normalized_name_score_sbert").isNull(), F.lit(1)).otherwise(F.lit(0)) +
                F.when(col("name_score_jaccard_lcs").isNull(), F.lit(1)).otherwise(F.lit(0)) +
                F.when(col("star_ratings_score").isNull(), F.lit(1)).otherwise(F.lit(0)) +
                F.when(col("postal_code_match").isNull(), F.lit(1)).otherwise(F.lit(0)) +
                F.when(col("country_match").isNull(), F.lit(1)).otherwise(F.lit(0)) +
                F.when(col("address_sbert_score").isNull(), F.lit(1)).otherwise(F.lit(0)) +
                F.when(col("phone_match_score").isNull(), F.lit(1)).otherwise(F.lit(0)) +
                F.when(col("email_match_score").isNull(), F.lit(1)).otherwise(F.lit(0)),
                F.lit(0)
            ).alias("null_count"),
            
            F.greatest(
                F.coalesce(col("geo_distance_km"), F.lit(0.0)),
                F.coalesce(col("normalized_name_score_sbert"), F.lit(0.0)),
                F.coalesce(col("name_score_jaccard_lcs"), F.lit(0.0)),
                F.coalesce(col("star_ratings_score"), F.lit(0.0)),
                F.coalesce(col("postal_code_match"), F.lit(0.0)),
                F.coalesce(col("country_match"), F.lit(0.0)),
                F.coalesce(col("address_sbert_score"), F.lit(0.0)),
                F.coalesce(col("phone_match_score"), F.lit(0.0)),
                F.coalesce(col("email_match_score"), F.lit(0.0))
            ).alias("max_signal_value"),
            
            F.least(
                F.coalesce(col("geo_distance_km"), F.lit(1.0)),
                F.coalesce(col("normalized_name_score_sbert"), F.lit(1.0)),
                F.coalesce(col("name_score_jaccard_lcs"), F.lit(1.0)),
                F.coalesce(col("star_ratings_score"), F.lit(1.0)),
                F.coalesce(col("postal_code_match"), F.lit(1.0)),
                F.coalesce(col("country_match"), F.lit(1.0)),
                F.coalesce(col("address_sbert_score"), F.lit(1.0)),
                F.coalesce(col("phone_match_score"), F.lit(1.0)),
                F.coalesce(col("email_match_score"), F.lit(1.0))
            ).alias("min_signal_value"),
            
            F.lit(0.0).alias("signal_variance")
        )
        
        return df.withColumn("signal_quality", quality_expr)
    
    def _add_score_components(self, df: DataFrame) -> DataFrame:
        """Add score components breakdown"""
        
        score_components_expr = struct(
            col("geo_distance_km_weighted"),
            col("normalized_name_score_sbert_weighted"),
            col("name_score_jaccard_lcs_weighted"),
            col("star_ratings_score_weighted"),
            col("postal_code_match_weighted"),
            col("country_match_weighted"),
            col("address_sbert_score_weighted"),
            col("phone_match_score_weighted"),
            col("email_match_score_weighted")
        )
        
        return df.withColumn("score_components", score_components_expr)
