# services/nested_scoring_service.py
# Enhanced version with support for nested AND/OR conditions

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import col, lit, current_timestamp, when, struct
from hotel_data.config.scoring_config import ScoringConfig
from hotel_data.pipeline.clustering.core.exceptions import ScoringException
from hotel_data.pipeline.clustering.core.clustering_interfaces import Logger, ScoringStrategy
from functools import reduce
from typing import List, Dict, Union, Optional, Tuple
from pyspark.sql.column import Column
from dataclasses import dataclass
from enum import Enum


class OperatorType(Enum):
    """Enum for logical operators"""
    AND = "AND"
    OR = "OR"


@dataclass
class ConditionLeaf:
    """
    Represents a single condition (leaf node in the tree).
    Example: geo_distance_km <= 0.5
    """
    signal_name: str
    threshold: float
    comparator: str  # "gte", "lte", "gt", "lt"

    def build_expression(self) -> Column:
        """Build PySpark Column expression for this single condition"""
        return _build_comparison_expr(self.signal_name, self.threshold, self.comparator)

    def describe(self) -> str:
        """Return human-readable description"""
        op = {
            "gte": ">=",
            "lte": "<=",
            "gt": ">",
            "lt": "<",
        }.get(self.comparator.lower(), self.comparator)
        return f"{self.signal_name} {op} {self.threshold}"


class ConditionNode:
    """
    Represents a node in the condition tree. Can be:
    - A leaf: single condition
    - A branch: operator + list of children (nodes or leaves)
    
    This enables arbitrary nesting of AND/OR logic.
    """
    
    def __init__(
        self,
        operator: OperatorType,
        children: List[Union['ConditionNode', ConditionLeaf]],
        logger: Optional[Logger] = None
    ):
        """
        Args:
            operator: AND or OR - how to combine children
            children: List of ConditionNode or ConditionLeaf objects
            logger: Optional logger instance
        """
        if not isinstance(operator, OperatorType):
            operator = OperatorType[operator.upper()]
        
        self.operator = operator
        self.children = children
        self.logger = logger
        
        if not children:
            raise ValueError("ConditionNode must have at least one child")

    def build_expression(self) -> Column:
        """
        Recursively build PySpark expression for this node and all children.
        Handles nested structures automatically.
        """
        if not self.children:
            return lit(True)

        # Build expressions for all children (recursively handles nested nodes)
        child_expressions = []
        for child in self.children:
            if isinstance(child, ConditionNode):
                # Recursively build expression for nested node
                child_expressions.append(child.build_expression())
            else:  # ConditionLeaf
                child_expressions.append(child.build_expression())

        # Combine child expressions with this node's operator
        if self.operator == OperatorType.AND:
            combined = reduce(lambda a, b: a & b, child_expressions)
        else:  # OR
            combined = reduce(lambda a, b: a | b, child_expressions)

        return combined

    def describe(self) -> str:
        """
        Return human-readable description with proper parentheses.
        Recursively handles nested structures.
        
        Examples:
            - "geo_distance_km <= 0.5"
            - "(name_score_jaccard >= 0.5 OR normalized_name_score_jaccard >= 0.7)"
            - "((name_score_jaccard >= 0.5 OR normalized_name_score_jaccard >= 0.7) AND geo_distance_km <= 0.5)"
        """
        parts = []
        for child in self.children:
            if isinstance(child, ConditionNode):
                parts.append(child.describe())
            else:  # ConditionLeaf
                parts.append(child.describe())

        joiner = f" {self.operator.value} "
        inner = joiner.join(parts)
        return f"({inner})"

    def get_leaf_count(self) -> int:
        """Get total number of leaf conditions in tree"""
        count = 0
        for child in self.children:
            if isinstance(child, ConditionNode):
                count += child.get_leaf_count()
            else:
                count += 1
        return count


class ThresholdScoringStrategyV2(ScoringStrategy):
    """
    Enhanced threshold-based scoring with support for nested AND/OR logic.
    Uses recursive ConditionNode structure instead of flat lists.
    """

    SCORING_VERSION = "v4.0-NestedThreshold"
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

    def __init__(self, config: ScoringConfig, logger: Logger):
        self.config = config
        self.logger = logger
        
        if not config.validate():
            raise ValueError("ScoringConfig validation failed")

        self.logger.log("INFO", "ThresholdScoringStrategyV2 initialized (nested support)")

        # Extract all unique signal thresholds and comparators
        self.signal_thresholds = {}
        self.signal_comparators = {}
        if config.condition_groups is not None:
            self._extract_signal_configs(config.condition_groups)
            # Build the condition tree from config
            self.condition_tree = self._build_condition_tree(config.condition_groups)

        # Log the complete condition logic
        if self.condition_tree:
            self.logger.log(
                "INFO",
                f"[ScoringLogic] Match condition: {self.condition_tree.describe()}"
            )

    def _extract_signal_configs(self, condition_groups: List) -> None:
        """Recursively extract all signal configs from nested structure"""
        if not condition_groups:
            return

        for group in condition_groups:
            if hasattr(group, 'condition_groups'):
                # Nested group - recurse
                self._extract_signal_configs(group.condition_groups)
            elif hasattr(group, 'conditions'):
                # Leaf group with conditions
                for signal_name, cond_config in group.conditions.items():
                    if signal_name not in self.signal_thresholds:
                        self.signal_thresholds[signal_name] = cond_config.threshold
                        self.signal_comparators[signal_name] = (
                            cond_config.comparator.value
                            if hasattr(cond_config.comparator, "value")
                            else cond_config.comparator
                        )

    def _build_condition_tree(self, condition_groups: List) -> Optional[ConditionNode]:
        """
        Build a recursive condition tree from config structure.
        
        Expects config structure like:
        condition_groups:
            - operator: AND
              conditions:
                signal1: {threshold: X, comparator: gte}
              # OR nested condition_groups:
              condition_groups:
                - operator: OR
                  conditions: {...}
        """
        if not condition_groups:
            return None

        children = []

        for group in condition_groups:
            operator = (
                group.operator.value
                if hasattr(group.operator, "value")
                else group.operator
            ).upper()

            if hasattr(group, 'condition_groups') and group.condition_groups:
                # This group contains nested groups - recurse
                nested_tree = self._build_condition_tree(group.condition_groups)
                if nested_tree:
                    children.append(nested_tree)
            
            if hasattr(group, 'conditions') and group.conditions:
                # This group contains conditions - create leaves
                leaves = []
                for signal_name, cond_config in group.conditions.items():
                    comparator = (
                        cond_config.comparator.value
                        if hasattr(cond_config.comparator, "value")
                        else cond_config.comparator
                    )
                    leaves.append(ConditionLeaf(
                        signal_name=signal_name,
                        threshold=cond_config.threshold,
                        comparator=comparator
                    ))

                if leaves:
                    # Create a node for this group's conditions
                    group_node = ConditionNode(
                        operator=OperatorType[operator],
                        children=leaves,
                        logger=self.logger
                    )
                    children.append(group_node)

        if not children:
            return None

        # If only one child, return it directly (avoid unnecessary nesting)
        if len(children) == 1:
            return children[0]

        # Multiple children - create root node with AND (default behavior)
        return ConditionNode(
            operator=OperatorType.AND,
            children=children,
            logger=self.logger
        )

    def score(self, pairs_df: DataFrame) -> DataFrame:
        """Main scoring method"""
        try:
            self.logger.log("INFO", "Starting nested threshold-based scoring")
            
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
        """Add pass/fail columns for each signal"""
        self.logger.log("INFO", "Evaluating individual signal thresholds...")

        result = df
        scoring_metadata_structs = []

        for signal_col in self.SIGNAL_COLUMNS:
            if signal_col not in self.signal_thresholds:
                continue

            threshold = self.signal_thresholds[signal_col]
            comparator = self.signal_comparators[signal_col]

            cmp_expr = _build_comparison_expr(signal_col, threshold, comparator)
            pass_expr = col(signal_col).isNotNull() & cmp_expr

            result = result.withColumn(f"{signal_col}_passed", pass_expr)

            signal_metadata = struct(
                col(signal_col).alias("actual"),
                F.lit(threshold).alias("threshold"),
                F.lit(comparator).alias("comparator"),
                pass_expr.alias("passed")
            )

            scoring_metadata_structs.append((signal_col, signal_metadata))

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
        """Apply condition tree to classify matches"""
        self.logger.log("INFO", f"Applying nested condition logic")

        if not self.condition_tree:
            all_passed_expr = lit(True)
        else:
            all_passed_expr = self.condition_tree.build_expression()

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


def _build_comparison_expr(signal_col: str, threshold: float, comparator: str) -> Column:
    """Helper function to build a comparison expression"""
    if comparator == "lte":
        return col(signal_col) <= F.lit(threshold)
    elif comparator == "lt":
        return col(signal_col) < F.lit(threshold)
    elif comparator == "gt":
        return col(signal_col) > F.lit(threshold)
    else:  # default / "gte"
        return col(signal_col) >= F.lit(threshold)