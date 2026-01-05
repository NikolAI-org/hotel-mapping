# services/nested_scoring_service.py
# Refactored for Unified "Rule-Based" Configuration

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import col, lit, current_timestamp, when, struct
from hotel_data.config.scoring_config import RuleConfig, ScoringConfig
from hotel_data.pipeline.clustering.core.exceptions import ScoringException
from hotel_data.pipeline.clustering.core.clustering_interfaces import Logger, ScoringStrategy
from functools import reduce
from typing import List, Dict, Union, Optional
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
    Represents a single condition (leaf node).
    Configuration Source:
      { "signal": "geo_distance_km", "threshold": 0.5, "comparator": "lte" }
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
            "gte": ">=", "lte": "<=", "gt": ">", "lt": "<",
        }.get(self.comparator.lower(), self.comparator)
        return f"{self.signal_name} {op} {self.threshold}"


class ConditionNode:
    """
    Represents a Logical Operator Node (AND/OR).
    Configuration Source:
      { "operator": "AND", "rules": [ ... ] }
    """
    
    def __init__(
        self,
        operator: OperatorType,
        children: List[Union['ConditionNode', ConditionLeaf]],
        logger: Optional[Logger] = None
    ):
        if not isinstance(operator, OperatorType):
            operator = OperatorType[operator.upper()]
        
        self.operator = operator
        self.children = children
        self.logger = logger
        
        if not children:
            raise ValueError("ConditionNode must have at least one child")

    def build_expression(self) -> Column:
        """Recursively build PySpark expression"""
        if not self.children:
            return lit(True)

        child_expressions = [child.build_expression() for child in self.children]

        if self.operator == OperatorType.AND:
            return reduce(lambda a, b: a & b, child_expressions)
        else:  # OR
            return reduce(lambda a, b: a | b, child_expressions)

    def describe(self) -> str:
        """Recursively describe structure"""
        parts = [child.describe() for child in self.children]
        joiner = f" {self.operator.value} "
        return f"({joiner.join(parts)})"

    def get_leaves(self) -> List[ConditionLeaf]:
        """
        Recursively collect all leaf nodes from this branch.
        Used to elegantly extract thresholds.
        """
        leaves = []
        for child in self.children:
            if isinstance(child, ConditionLeaf):
                leaves.append(child)
            elif isinstance(child, ConditionNode):
                leaves.extend(child.get_leaves())
        return leaves


class ThresholdScoringStrategyV2(ScoringStrategy):
    """
    Unified Recursive Scoring Strategy.
    Supports arbitrarily nested AND/OR logic defined in a single 'rules' structure.
    """

    SCORING_VERSION = "v5.0-UnifiedRules"
    # List of all known/supported columns (for safety/filtering)
    SUPPORTED_SIGNALS = {
        "geo_distance_km", "name_score_jaccard", "normalized_name_score_jaccard",
        "name_score_lcs", "normalized_name_score_lcs", "name_score_levenshtein",
        "normalized_name_score_levenshtein", "name_score_sbert", "normalized_name_score_sbert",
        "star_ratings_score", "address_line1_score", "postal_code_match",
        "country_match", "address_sbert_score", "phone_match_score",
        "email_match_score", "fax_match_score"
    }

    def __init__(self, config: ScoringConfig, logger: Logger):
        self.config = config
        self.logger = logger
        
        if not config.validate():
            raise ValueError("ScoringConfig validation failed")

        self.logger.log("INFO", "ThresholdScoringStrategyV2 initialized")

        # 1. Get the root logic object
        # It is now a RuleConfig object, NOT a dictionary
        match_logic_config = getattr(config, "match_logic", None)
        
        if not match_logic_config:
             raise ValueError("Config missing 'match_logic' block")

        # 2. Build the Tree
        self.condition_tree = self._build_condition_tree(match_logic_config)

        # 3. Extract Thresholds (Updates to support objects)
        self.signal_thresholds = {}
        self.signal_comparators = {}
        
        if self.condition_tree:
            self._extract_signals_from_tree(self.condition_tree)
            self.logger.log("INFO", f"[ScoringLogic] {self.condition_tree.describe()}")
        else:
            self.logger.log("WARN", "No condition tree built.")
            

    def _build_condition_tree(self, config_item: Union[Dict, RuleConfig]) -> Optional[Union[ConditionNode, ConditionLeaf]]:
        """
        Recursive factory method. 
        Now handles BOTH RuleConfig objects and legacy dictionaries.
        """
        if not config_item:
            return None

        # --- BRANCH 1: Handle RuleConfig Object (The new standard) ---
        if isinstance(config_item, RuleConfig):
            
            # CASE A: It is a Group (has an operator)
            if config_item.operator:
                # Convert Enum to string for lookup
                op_str = config_item.operator.value.upper()
                
                children = []
                # Use "or []" to handle None safely for iteration
                for rule in (config_item.rules or []):
                    child = self._build_condition_tree(rule)
                    if child:
                        children.append(child)
                
                if not children:
                    return None
                
                if len(children) == 1:
                    return children[0]

                return ConditionNode(
                    operator=OperatorType[op_str],
                    children=children,
                    logger=self.logger
                )

            # CASE B: It is a Leaf (has a signal)
            elif config_item.signal:
                
                # FIX: Explicitly check for None to satisfy type checker
                if config_item.threshold is None:
                    raise ValueError(f"Threshold cannot be None for signal: {config_item.signal}")

                # Handle comparator Enum
                comp_val = (
                    config_item.comparator.value 
                    if hasattr(config_item.comparator, "value") 
                    else config_item.comparator
                )

                return ConditionLeaf(
                    signal_name=config_item.signal,
                    threshold=float(config_item.threshold), # Now safe because we checked for None above
                    comparator=comp_val
                )

        # --- BRANCH 2: Handle Dictionary (Legacy / Fallback) ---
        elif isinstance(config_item, dict):
            # ... (No changes needed for the legacy branch)
            if "operator" in config_item:
                op_str = config_item["operator"].upper()
                rules = config_item.get("rules", [])
                
                children = []
                for rule_cfg in rules:
                    child = self._build_condition_tree(rule_cfg)
                    if child:
                        children.append(child)
                
                if not children:
                    return None
                
                if len(children) == 1:
                    return children[0]

                return ConditionNode(
                    operator=OperatorType[op_str],
                    children=children,
                    logger=self.logger
                )

            elif "signal" in config_item:
                comparator = config_item.get("comparator", "gte")
                if hasattr(comparator, "value"):
                    comparator = comparator.value

                return ConditionLeaf(
                    signal_name=config_item["signal"],
                    threshold=float(config_item["threshold"]),
                    comparator=comparator
                )

        return None

    def _extract_signals_from_tree(self, node: Union[ConditionNode, ConditionLeaf]):
        """
        Visitor method that walks the constructed tree to populate signal maps.
        This is the source of truth for 'what signals are actually being used'.
        """
        if isinstance(node, ConditionLeaf):
            # If the same signal appears twice with different thresholds, 
            # this logic preserves the FIRST one encountered (or you can toggle to overwrite).
            if node.signal_name not in self.signal_thresholds:
                self.signal_thresholds[node.signal_name] = node.threshold
                self.signal_comparators[node.signal_name] = node.comparator
        
        elif isinstance(node, ConditionNode):
            for child in node.children:
                self._extract_signals_from_tree(child)

    def score(self, pairs_df: DataFrame) -> DataFrame:
        """Main scoring method"""
        try:
            self.logger.log("INFO", "Starting Unified Rules scoring")
            
            df = pairs_df
            # 1. Calculate pass/fail metadata for every signal found in the tree
            df = self._add_signal_pass_status(df)
            
            # 2. Evaluate the complex boolean logic
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
        """Add pass/fail columns and metadata json"""
        self.logger.log("INFO", "Evaluating individual signal thresholds...")

        result = df
        scoring_metadata_structs = []

        # We iterate over SUPPORTED_SIGNALS to ensure we don't process garbage,
        # but we check against 'self.signal_thresholds' which was populated from the tree.
        for signal_col in self.SUPPORTED_SIGNALS:
            
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
        """Apply the recursive logic tree to classify matches"""
        self.logger.log("INFO", f"Applying nested condition logic")

        if not self.condition_tree:
            # Default to False or True depending on business logic if config is empty
            all_passed_expr = lit(False) 
        else:
            all_passed_expr = self.condition_tree.build_expression()

        df = (df
            .withColumn("is_matched", all_passed_expr)
            .withColumn(
                "match_status",
                when(col("is_matched"), lit("MATCHED")).otherwise(lit("UNMATCHED"))
            )
        )
        
        # Logging stats
        match_count = df.filter(col('match_status') == 'MATCHED').count()
        self.logger.log("INFO", f"Classification stats: {match_count} Matches found.")

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