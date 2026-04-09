import functools
from abc import ABC, abstractmethod
from typing import List
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column

# --- INTERFACES ---
class VetoRule(ABC):
    @property
    @abstractmethod
    def rule_name(self) -> str: pass

    @abstractmethod
    def get_condition(self) -> Column: pass
    
class DynamicVetoRule(VetoRule):
    """A Veto Rule driven entirely by a JSON configuration DAG."""
    def __init__(self, rule_name: str, logic_config: dict):
        self._rule_name = rule_name
        self.logic_config = logic_config
        # Reuse your existing evaluator engine!
        self.evaluator = MatchLogicEvaluator(logic_config)

    @property
    def rule_name(self) -> str: 
        return self._rule_name
    
    def get_condition(self) -> Column:
        # Compiles the JSON into a PySpark boolean expression
        return self.evaluator.build_match_expression(self.logic_config)    

# --- CONCRETE RULES (Add more as needed) ---
class DualBrandVeto(VetoRule):
    @property
    def rule_name(self) -> str: return "VETO_DUAL_BRAND_TRAP"
    def get_condition(self) -> Column:
        # Example: Very close geographically, but name scores are terrible
        return (F.col("geo_distance_km") < 0.05) & (F.col("name_score_jaccard") < 0.4)

class MissingGeoTiebreakerVeto(VetoRule):
    @property
    def rule_name(self) -> str: return "VETO_MISSING_GEO_TIEBREAKER"
    def get_condition(self) -> Column:
        # Example: High name score, but absolutely no geo context to confirm
        return (F.col("name_score_jaccard") > 0.9) & (F.col("geo_distance_km").isNull()) & (F.col("address_line1_score").isNull())


class MatchLogicEvaluator:
    """Parses JSON DAG configurations into PySpark Boolean Expressions."""
    def __init__(self, match_logic_config: dict):
        self.match_logic_config = match_logic_config

    def evaluate(self, df: DataFrame) -> DataFrame:
        """Applies the boolean logic and appends failure reasons."""
        match_expr = self.build_match_expression(self.match_logic_config)
        evaluated_df = df.withColumn("is_matched", match_expr)
        return self.append_failure_reasons(evaluated_df)

    def build_match_expression(self, rule_node: dict) -> Column:
        if "operator" in rule_node:
            op = rule_node["operator"].upper()
            child_exprs = [self.build_match_expression(child) for child in rule_node["rules"]]
            if not child_exprs: return F.lit(False)
            return functools.reduce(lambda x, y: x & y if op == "AND" else x | y, child_exprs)
        
        elif "signal" in rule_node:
            comp = rule_node["comparator"].lower()
            raw_col = F.col(rule_node["signal"])
            
            # NEW: Handle missing data checks BEFORE coalescing to 0.0
            if comp == "isnull": return raw_col.isNull()
            if comp == "isnotnull": return raw_col.isNotNull()

            # Existing numerical comparisons
            signal_col = F.coalesce(raw_col, F.lit(0.0))
            threshold = float(rule_node.get("threshold", 0.0))
            
            if comp == "gte": return signal_col >= threshold
            if comp == "lte": return signal_col <= threshold
            if comp == "gt":  return signal_col > threshold
            if comp == "lt":  return signal_col < threshold
            if comp == "eq":  return signal_col == threshold
            
        return F.lit(False)

    def _get_rule_name(self, rule_node: dict) -> str:
        if "signal" in rule_node:
            return f"{rule_node['signal']} {rule_node['comparator']} {rule_node['threshold']}"
        elif "operator" in rule_node:
            signals = []
            for r in rule_node.get("rules", []):
                if "signal" in r:
                    signals.append(r["signal"])
                elif "rules" in r and "signal" in r["rules"][0]:
                    signals.append(r["rules"][0]["signal"])
                    
            signal_names = ", ".join(signals[:2]) + ("..." if len(signals) > 2 else "")
            return f"Failed {rule_node['operator']} Block ({signal_names})"
        return "Unknown Rule Block"

    def append_failure_reasons(self, df: DataFrame) -> DataFrame:
        if self.match_logic_config.get("operator", "").upper() != "AND":
            return df.withColumn("failed_conditions", F.expr("CAST(array() AS array<string>)"))

        failure_cols = []
        for rule in self.match_logic_config.get("rules", []):
            rule_expr = self.build_match_expression(rule)
            rule_name = self._get_rule_name(rule)
            failure_col = F.when(~rule_expr, F.lit(rule_name)).otherwise(F.lit(None))
            failure_cols.append(failure_col)

        if not failure_cols:
            return df.withColumn("failed_conditions", F.expr("CAST(array() AS array<string>)"))
            
        # FIX: Create a temporary array, filter nulls using Spark SQL, and drop the temp column.
        return df.withColumn("raw_failures", F.array(*failure_cols)) \
                 .withColumn("failed_conditions", F.expr("filter(raw_failures, x -> x is not null)")) \
                 .drop("raw_failures")
        
# --- ENGINES ---
class VetoEngine:
    """Applies Veto rules and zeroes out composite scores if triggered."""
    def __init__(self, rules: List[VetoRule]):
        self.rules = rules

    def apply(self, df: DataFrame) -> DataFrame:
        veto_condition = F.lit(False)
        veto_reason = F.lit(None).cast("string")

        for rule in self.rules:
            veto_condition = veto_condition | rule.get_condition()
            veto_reason = F.when(rule.get_condition(), F.lit(rule.rule_name)).otherwise(veto_reason)

        return df.withColumn("is_vetoed", veto_condition) \
                 .withColumn("veto_reason", veto_reason) \
                 .withColumn("composite_score", 
                             F.when(F.col("is_vetoed"), F.lit(0.0)).otherwise(F.col("composite_score")))


class RoutingDecisionEngine:
    def __init__(self, match_threshold: float, conflict_margin: float):
        self.match_threshold = match_threshold
        self.conflict_margin = conflict_margin

    def route(self, scored_pairs_df: DataFrame) -> DataFrame:
        w_rank = Window.partitionBy("uid_i").orderBy(F.col("composite_score").desc())
        ranked_df = scored_pairs_df.withColumn("rank", F.row_number().over(w_rank))

        w_uid = Window.partitionBy("uid_i")
        top_1 = F.max(F.when(F.col("rank") == 1, F.col("composite_score"))).over(w_uid)
        top_2 = F.max(F.when(F.col("rank") == 2, F.col("composite_score"))).over(w_uid)
        
        score_diff = top_1 - F.coalesce(top_2, F.lit(0.0))
        df_with_diff = ranked_df.withColumn("score_diff", score_diff)

        top_candidates = df_with_diff.filter(F.col("rank") == 1)

        # FIX: Inject the boolean 'is_matched' as the primary gatekeeper
        return top_candidates.withColumn(
            "routing_decision",
            F.when(F.col("is_matched") == False, "FAILED_BOOLEAN_LOGIC")  # <-- STRICT RULE OVERRIDE
             .when(F.col("is_vetoed"), "CONFLICT_VETOED")
             .when((F.col("composite_score") >= self.match_threshold) & (F.col("score_diff") < self.conflict_margin), "CONFLICT_AMBIGUOUS")
             .when(F.col("composite_score") >= self.match_threshold, "ATTACH_TO_CANONICAL")
             .otherwise("CREATE_NEW_SINGLETON")
        )

class CanonicalIdGenerator:
    """Generates Immutable Geo-Hashed IDs: [COUNTRY]-[CITY]-[AREA]-[HASH]"""
    def generate(self, df: DataFrame, uid_col: str = "uid") -> DataFrame:
        # 1. Coalesce nulls, 2. Trim outer spaces, 3. Replace inner spaces with underscores
        clean_country = F.regexp_replace(
            F.trim(F.coalesce(F.col("contact_address_country_code"), F.lit("XX"))), r"\s+", "_"
        )
        clean_city = F.regexp_replace(
            F.trim(F.coalesce(F.col("contact_address_city_name"), F.lit("UNKNOWN"))), r"\s+", "_"
        )
        clean_area = F.regexp_replace(
            F.trim(F.coalesce(F.col("contact_address_state_name"), F.lit("UNKNOWN"))), r"\s+", "_"
        )

        geo_prefix = F.concat_ws("-", F.upper(clean_country), F.upper(clean_city), F.upper(clean_area))
        
        # Create Hash
        return df.withColumn(
            "cluster_id",
            F.concat_ws("-", geo_prefix, F.substring(F.sha2(F.col(uid_col), 256), 1, 12))
        )
        

class ClusterCohesionValidator:
    """Verifies that an incoming hotel matches REQUIRED sibling providers in the target cluster."""
    def __init__(self, required_providers: List[str]):
        self.required_providers = required_providers

    def validate(self, proposed_attachments: DataFrame, existing_mappings: DataFrame, valid_pairs: DataFrame) -> DataFrame:
        # If no required providers are configured or no mappings exist yet, skip.
        if not self.required_providers or existing_mappings is None:
            return proposed_attachments

        attachments = proposed_attachments.filter(F.col("routing_decision") == "ATTACH_TO_CANONICAL")
        others = proposed_attachments.filter(F.col("routing_decision") != "ATTACH_TO_CANONICAL")

        # 1. Get active members of the target clusters that belong to the REQUIRED providers
        active_req_members = existing_mappings.filter(
            (F.col("mapping_state") == "ACTIVE") & 
            F.col("providerName").isin(self.required_providers)
        ).select(
            F.col("canonical_id").alias("assigned_canonical_id"),
            F.col("uid").alias("req_sibling_uid")
        )

        # 2. Join proposed attachments to find out which specific UIDs they MUST match
        requirements = attachments.join(
            active_req_members, 
            on="assigned_canonical_id", 
            how="left"
        )

        # 3. Check if valid pairs exist against these req_sibling_uids
        valid_pairs_aliased = valid_pairs.select(
            F.col("uid_i").alias("pair_uid_i"), 
            F.col("uid_j").alias("pair_uid_j")
        ).withColumn("pair_exists", F.lit(True))

        validation_check = requirements.join(
            valid_pairs_aliased,
            (requirements.uid_i == valid_pairs_aliased.pair_uid_i) & (requirements.req_sibling_uid == valid_pairs_aliased.pair_uid_j),
            how="left"
        )

        # 4. Flag failures: A sibling exists in the cluster, but the pair is missing/failed
        failures = validation_check.filter(
            F.col("req_sibling_uid").isNotNull() & F.col("pair_exists").isNull()
        ).select("uid_i").distinct().withColumn("failed_cohesion", F.lit(True))

        # 5. Apply failures back to the proposed attachments (Downgrade them to Conflicts)
        updated_attachments = attachments.join(failures, on="uid_i", how="left").withColumn(
            "routing_decision",
            F.when(F.col("failed_cohesion") == True, F.lit("CONFLICT_FAILED_COHESION"))
             .otherwise(F.col("routing_decision"))
        ).withColumn(
            "veto_reason",
            F.when(F.col("failed_cohesion") == True, F.lit("MISSING_REQUIRED_PROVIDER_MATCH"))
             .otherwise(F.col("veto_reason"))
        ).drop("failed_cohesion")

        # Re-combine with the ones that were already singletons/conflicts
        return updated_attachments.unionByName(others)