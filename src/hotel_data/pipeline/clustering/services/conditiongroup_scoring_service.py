# services/conditiongroup_scoring_service.py

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import col, lit, current_timestamp, when, struct
from hotel_data.config.scoring_config import ConditionGroupConfig, ScoringConfig
from hotel_data.pipeline.clustering.core.exceptions import ScoringException
from hotel_data.pipeline.clustering.core.clustering_interfaces import Logger, ScoringStrategy
from functools import reduce
from typing import List, Dict, Tuple
from pyspark.sql.column import Column


class ConditionGroup:
    """
    Represents a group of conditions combined with a logical operator (AND/OR).
    
    Example:
        group1 = ConditionGroup(
            operator="AND",
            conditions={
                "name_score_jaccard_lcs": {"threshold": 0.18, "comparator": "gte"},
                "normalized_name_score_jaccard_lcs": {"threshold": 0.18, "comparator": "gte"},
            }
        )
    """
    
    def __init__(self, operator: str, conditions: Dict[str, Dict], logger: Logger):
        """
        Args:
            operator: "AND" or "OR" - how to combine conditions within this group
            conditions: dict of {signal_name: {threshold, comparator}}
        """
        if operator.upper() not in ["AND", "OR"]:
            raise ValueError(f"Operator must be AND or OR, got: {operator}")
        
        self.operator = operator.upper()
        self.conditions = conditions
        self.logger = logger
    
    def build_expression(self, signal_thresholds: Dict, signal_comparators: Dict, logger=None) -> Column:
        """
        Build a PySpark Column expression for this condition group.
        Optionally logs each condition if logger is provided.
        """
        condition_expressions = []
        
        for signal_name, config in self.conditions.items():
            threshold = config.get("threshold", signal_thresholds.get(signal_name, 0.0))
            comparator = config.get("comparator", signal_comparators.get(signal_name, "gte"))

            if logger:
                logger.log(
                    "INFO",
                    f"[ConditionGroup] operator={self.operator} "
                    f"signal={signal_name} threshold={threshold} comparator={comparator}"
                )
            
            expr = _build_comparison_expr(signal_name, threshold, comparator)
            condition_expressions.append(expr)
        
        if not condition_expressions:
            if logger:
                logger.log("INFO", "[ConditionGroup] Empty group, returning TRUE")
            return lit(True)
        
        if self.operator == "AND":
            combined = reduce(lambda a, b: a & b, condition_expressions)
        else:
            combined = reduce(lambda a, b: a | b, condition_expressions)
        
        if logger:
            logger.log(
                "INFO",
                f"[ConditionGroup] Built expression for group with operator={self.operator} "
                f"conditions={list(self.conditions.keys())}"
            )
        
        return combined
    
    
    def describe(self) -> str:
        """
        Return a human-readable description of this group.
        Example: (name_score_jaccard_lcs >= 0.18 AND address_line1_score >= 0.4)
        """
        parts = []
        for signal_name, cfg in self.conditions.items():
            threshold = cfg.get("threshold")
            comparator = cfg.get("comparator")

            if hasattr(comparator, "value"):
                comparator = comparator.value # type: ignore

            op = {
                "gte": ">=",
                "lte": "<=",
                "gt":  ">",
                "lt":  "<",
            }.get(str(comparator).lower(), str(comparator))

            parts.append(f"{signal_name} {op} {threshold}")
        inner = f" {self.operator} ".join(parts)
        return f"({inner})"



class ThresholdScoringStrategy(ScoringStrategy):
    """
    Threshold-based scoring with support for AND/OR logic between condition groups.
    """
    
    SCORING_VERSION = "v3.0-AdvancedThreshold"
    
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
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # EXTRACT thresholds and comparators FROM condition_groups
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        self.signal_thresholds = {}
        self.signal_comparators = {}
        
        if config.condition_groups:
            for group in config.condition_groups:
                if group.conditions is not None:
                    for signal_name, cond_config in group.conditions.items():
                        if signal_name not in self.signal_thresholds:
                            self.signal_thresholds[signal_name] = cond_config.threshold
                            self.signal_comparators[signal_name] = (
                                cond_config.comparator.value
                                if hasattr(cond_config.comparator, "value")
                                else cond_config.comparator
                            )
        
        self.group_operator = (
            config.group_operator.value
            if hasattr(config.group_operator, "value")
            else config.group_operator
        )
        
        self.condition_groups = self._parse_condition_groups()
        
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        # LOG FULL AND/OR CONDITION LOGIC
        # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        if self.condition_groups:
            group_descriptions = [g.describe() for g in self.condition_groups]
            if len(group_descriptions) == 1:
                full_expr = group_descriptions
            else:
                joiner = f" {self.group_operator} "
                full_expr = joiner.join(group_descriptions)
            self.logger.log("INFO", f"[ScoringLogic] Match condition: {full_expr}")


    
    def _parse_condition_groups(self) -> List[ConditionGroup]:
        """Convert ConditionGroupConfig dataclasses to ConditionGroup objects."""
        groups = []
        
        if self.config.condition_groups:            
            for group_config in self.config.condition_groups:
                operator = (
                    group_config.operator.value
                    if hasattr(group_config.operator, "value")
                    else group_config.operator
                )
                
                conditions = {}
                if group_config.conditions is not None:
                    for signal_name, cond_config in group_config.conditions.items():
                        comparator = (
                            cond_config.comparator.value
                            if hasattr(cond_config.comparator, "value")
                            else cond_config.comparator
                        )
                        conditions[signal_name] = {
                            "threshold": cond_config.threshold,
                            "comparator": comparator,
                        }
                
                groups.append(ConditionGroup(operator, conditions, self.logger))
        
        self.logger.log(
            "INFO",
            f"Using {len(groups)} condition groups combined with {self.group_operator}",
        )
        return groups


    
    def score(self, pairs_df: DataFrame) -> DataFrame:
        """
        Main scoring method for threshold-based classification with AND/OR logic.
        """
        try:
            self.logger.log("INFO", "Starting threshold-based scoring with condition groups")
            
            df = pairs_df
            
            df = self._add_signal_pass_status(df)
            df = self._add_match_classification(df)
            
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
        Adds a boolean column for each signal indicating if it met its threshold.
        Also adds a JSON column with all scoring details.
        """
        self.logger.log("INFO", "Evaluating individual signal thresholds...")
        
        result = df
        scoring_metadata_structs = []
        
        for signal_col in self.SIGNAL_COLUMNS:
            threshold = self.signal_thresholds[signal_col]
            # Direct access - all signals guaranteed to exist after validation
            comparator = self.signal_comparators[signal_col]

            
            # Build the comparison expression
            cmp_expr = _build_comparison_expr(signal_col, threshold, comparator)
            pass_expr = col(signal_col).isNotNull() & cmp_expr
            
            # Add pass/fail column
            result = result.withColumn(f"{signal_col}_passed", pass_expr)
            
            # Build struct for JSON metadata
            signal_metadata = struct(
                col(signal_col).alias("actual"),
                F.lit(threshold).alias("threshold"),
                F.lit(comparator).alias("comparator"),
                pass_expr.alias("passed")
            )
            
            scoring_metadata_structs.append((signal_col, signal_metadata))
        
        # Combine all signal metadata into a single JSON column
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
        Apply condition logic to classify matches.
        Works with BOTH old ConditionGroup and new ConditionGroupConfig.
        """
        
        # Get operator value safely
        group_op_str = getattr(self.group_operator, "value", self.group_operator)
        
        self.logger.log(
            "INFO",
            f"Applying {len(self.condition_groups)} condition groups "
            f"combined with {group_op_str}"
        )
        
        if not self.condition_groups:
            all_passed_expr = lit(True)
        elif len(self.condition_groups) == 1:
            # Single group - check type and process accordingly
            group = self.condition_groups
            
            # Check if it's new ConditionGroupConfig with nesting
            if (isinstance(group, ConditionGroupConfig) and 
                hasattr(group, 'condition_groups') and 
                group.condition_groups):
                # New nested structure
                all_passed_expr = self._build_group_expression(group)
            else:
                # Old ConditionGroup or flat ConditionGroupConfig
                all_passed_expr = self._build_old_style_expression(group)
        else:
            # Multiple groups
            group_expressions = []
            
            for group in self.condition_groups:
                # Check if it's new ConditionGroupConfig with nesting
                if (isinstance(group, ConditionGroupConfig) and 
                    hasattr(group, 'condition_groups') and 
                    group.condition_groups):
                    # New nested structure
                    group_expr = self._build_group_expression(group)
                else:
                    # Old ConditionGroup or flat ConditionGroupConfig
                    group_expr = self._build_old_style_expression(group)
                
                group_expressions.append(group_expr)
            
            # Combine groups
            op_value = getattr(self.group_operator, "value", self.group_operator)
            
            if op_value == "OR":
                all_passed_expr = reduce(lambda a, b: a | b, group_expressions)
            else:
                all_passed_expr = reduce(lambda a, b: a & b, group_expressions)
        
        # Classify and score
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
        
        df = df.withColumn(
            "match_score",
            when(col("is_matched"), F.lit(1.0)).otherwise(F.lit(0.0))
        )
        
        return df

    def _build_old_style_expression(self, group) -> Column:
        """
        Build expression for old ConditionGroup or flat ConditionGroupConfig.
        This is the existing logic.
        """
        # Check if it's old ConditionGroup with build_expression method
        if hasattr(group, 'build_expression'):
            # Old ConditionGroup - use existing method
            return group.build_expression(
                self.signal_thresholds,
                self.signal_comparators
            )
        
        # Otherwise handle as flat ConditionGroupConfig
        expressions = []
        
        if hasattr(group, 'conditions') and group.conditions:
            for signal_name, config in group.conditions.items():
                threshold = config.threshold
                comparator = (
                    config.comparator.value 
                    if hasattr(config.comparator, 'value') 
                    else str(config.comparator).lower()
                )
                expr = _build_comparison_expr(signal_name, threshold, comparator)
                expressions.append(expr)
        
        if not expressions:
            return lit(True)
        
        group_op = (
            group.operator.value.upper() 
            if hasattr(group.operator, 'value') 
            else str(group.operator).upper()
        )
        
        if group_op == "AND":
            return reduce(lambda a, b: a & b, expressions)
        else:
            return reduce(lambda a, b: a | b, expressions)


    def _build_group_expression(self, group: ConditionGroupConfig) -> Column:
        """
        Recursively build expression for NEW nested ConditionGroupConfig.
        Only called for new-style nested groups.
        """
        expressions = []
        
        # Process conditions
        if group.conditions:
            for signal_name, config in group.conditions.items():
                threshold = config.threshold
                comparator = (
                    config.comparator.value 
                    if hasattr(config.comparator, 'value') 
                    else str(config.comparator).lower()
                )
                expr = _build_comparison_expr(signal_name, threshold, comparator)
                expressions.append(expr)
        
        # Process nested groups (recursive)
        if group.condition_groups:
            for nested_group in group.condition_groups:
                nested_expr = self._build_group_expression(nested_group)
                expressions.append(nested_expr)
        
        # Combine
        if not expressions:
            return lit(True)
        
        group_op = (
            group.operator.value.upper() 
            if hasattr(group.operator, 'value') 
            else str(group.operator).upper()
        )
        
        if group_op == "AND":
            return reduce(lambda a, b: a & b, expressions)
        else:
            return reduce(lambda a, b: a | b, expressions)
    
    def _validate_input_schema(self, df: DataFrame) -> None:
        """Validate required columns exist"""
        required = ["id_i", "id_j"] + self.SIGNAL_COLUMNS
        missing = set(required) - set(df.columns)
        
        if missing:
            raise ScoringException(f"Missing columns: {missing}")
    
    def _clip_signals_to_range(self, df: DataFrame) -> DataFrame:
        """Clip all signal values to [0, 1] range"""
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


def _build_comparison_expr(signal_col: str, threshold: float, comparator: str) -> Column:
    """
    Helper function to build a comparison expression.
    
    Args:
        signal_col: Column name
        threshold: Threshold value
        comparator: "gte", "lte", "gt", "lt"
    
    Returns:
        A PySpark Column expression (boolean)
    """
    if comparator == "lte":
        return col(signal_col) <= F.lit(threshold)
    elif comparator == "lt":
        return col(signal_col) < F.lit(threshold)
    elif comparator == "gt":
        return col(signal_col) > F.lit(threshold)
    else:
        # default / "gte"
        return col(signal_col) >= F.lit(threshold)