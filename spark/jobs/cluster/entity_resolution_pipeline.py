
from pyspark.sql import Window, functions as F, DataFrame
from pyspark.shell import SparkSession
import functools

from spark.jobs.cluster.flexible_clustering import FlexibleClustering
from spark.jobs.cluster.pair_scorer import PairScorer


class EntityResolutionPipeline:
    def __init__(self, spark: SparkSession, config: dict, match_logic_config: dict):
        self.spark = spark
        self.scorer = PairScorer(config['weights'], config['t_high'], config['t_low'])
        # Pass a standard Python logger
        import logging
        transitity = config.get('transitivity', True)
        self.resolver = FlexibleClustering(spark, transitivity_enabled=transitity)
        self.match_logic_config = match_logic_config

    def run(self, manager, table_hotels, table_pairs, table_output, table_history, current_provider):
        # 1. Load Data & SHIELD AGAINST DUPLICATES
        new_hotels_df = manager.read_table(table_hotels) \
            .filter(F.col("providerName") == current_provider) \
            .dropDuplicates(["uid"])
            
        new_hotels_df.cache()
        new_hotels_df.count() 
        
        try:
            new_pairs_df = manager.read_table(table_pairs).filter(
                (F.col("providerName_i") == current_provider) |
                (F.col("providerName_j") == current_provider)
            )
            has_pairs = not new_pairs_df.isEmpty()
        except Exception:
            has_pairs = False

        # ---------------------------------------------------------
        # THE FALLBACK BLOCK (If no pairs exist)
        # ---------------------------------------------------------
        if not has_pairs:
            if not manager._table_exists(table_output):
                w = Window.orderBy("uid")
                output_df = new_hotels_df.withColumn("cluster_id", (F.row_number().over(w)).cast("string"))
                
                # Ironclad Deduplication before writing
                clean_output = output_df.dropDuplicates(["uid"])
                manager.create_table(table_output, df=clean_output.select("cluster_id", "name", "uid", "providerName"))
            else:
                existing_df = manager.read_table(table_output)
                max_row = existing_df.select(F.max(F.col("cluster_id").cast("int")).alias("max_id")).collect()[0]
                max_id = max_row["max_id"] if max_row["max_id"] is not None else 0
                
                w = Window.orderBy("uid")
                output_df = new_hotels_df.withColumn("cluster_id", (F.row_number().over(w) + F.lit(max_id)).cast("string"))
                
                # Ironclad Deduplication before writing
                clean_output = output_df.dropDuplicates(["uid"])
                manager.merge_table(
                    table_name=table_output,
                    df=clean_output.select("cluster_id", "name", "uid", "providerName"),
                    key_columns=["uid"]
                )
            return

        # ---------------------------------------------------------
        # THE MAIN BLOCK (If pairs DO exist)
        # ---------------------------------------------------------
        scored_pairs = self.scorer.process(new_pairs_df)
        
        # 1. Evaluate the strict boolean logic first
        match_expr = self.build_match_expression(self.match_logic_config)
        evaluated_pairs_df = scored_pairs.withColumn("is_matched", match_expr)
        
        
        # 2. Define the core audit columns
        evaluated_pairs_df = self.append_failure_reasons(evaluated_pairs_df, self.match_logic_config)
        
        base_cols = [
            F.col("uid_i").alias("hotel_id"), F.col("name_i").alias("hotel_name"),
            F.col("uid_j").alias("compared_with_id"), F.col("name_j").alias("compared_with_name"),
            F.col("composite_score").alias("score"), F.col("classification").alias("status"),
            F.col("is_matched"), 
            F.col("failed_conditions"), # <--- Added here!
            F.current_timestamp().alias("comparison_at")
        ]
        
        # 3. Define all the raw signals used by your YAML logic
        signal_cols = [
            F.col("geo_distance_km"), 
            F.col("name_score_jaccard"), F.col("normalized_name_score_jaccard"),
            F.col("name_score_lcs"), F.col("normalized_name_score_lcs"),
            F.col("name_score_levenshtein"), F.col("normalized_name_score_levenshtein"),
            F.col("name_score_sbert"), F.col("normalized_name_score_sbert"),
            F.col("address_line1_score"), F.col("address_sbert_score"), 
            F.col("star_ratings_score"), F.col("postal_code_match"), 
            F.col("phone_match_score"), F.col("email_match_score"), F.col("fax_match_score")
        ]
        
        # 4. Write EVERYTHING to the audit log
        history_df = evaluated_pairs_df.select(*(base_cols + signal_cols)).withColumn(
            "failed_conditions",
            F.coalesce(
                F.col("failed_conditions").cast("array<string>"),
                F.expr("CAST(array() AS array<string>)")
            )
        )
        manager.write_table(table_history, history_df, mode="append")
        
        # ---------------------------------------------------------
        # 4. Form/Merge Clusters FIRST to determine final assignments
        # ---------------------------------------------------------
        if not manager._table_exists(table_output):
            existing_clusters_df = None
        else:
            existing_clusters_df = manager.read_table(table_output)
            
        output_df = self.resolver.cluster(new_hotels_df, evaluated_pairs_df, existing_clusters_df)
        
        # ---------------------------------------------------------
        # 5. Build a unified UID -> Cluster ID mapping for the audit
        # ---------------------------------------------------------        
        new_mapping = output_df.select("uid", "cluster_id")
        if existing_clusters_df is not None:  # <--- FIXED HERE
            old_mapping = existing_clusters_df.select("uid", "cluster_id")
            full_mapping = new_mapping.union(old_mapping).dropDuplicates(["uid"])
        else:
            full_mapping = new_mapping
        # ---------------------------------------------------------
        # 6. Flag Transitivity Issues 
        # (Matched by logic, but assigned to different clusters)
        # ---------------------------------------------------------
        evaluated_pairs_df = evaluated_pairs_df \
            .join(full_mapping.withColumnRenamed("cluster_id", "cluster_id_i"), 
                  evaluated_pairs_df.uid_i == full_mapping.uid, "left").drop("uid") \
            .join(full_mapping.withColumnRenamed("cluster_id", "cluster_id_j"), 
                  evaluated_pairs_df.uid_j == full_mapping.uid, "left").drop("uid")
                  
        evaluated_pairs_df = evaluated_pairs_df.withColumn(
            "transitivity_issue",
            F.when(
                (F.col("is_matched") == True) & (F.col("cluster_id_i") != F.col("cluster_id_j")), 
                True
            ).otherwise(False)
        )

        # ---------------------------------------------------------
        # 7. Update Base Columns for Audit Log
        # ---------------------------------------------------------
        base_cols = [
            F.col("uid_i").alias("hotel_id"), F.col("name_i").alias("hotel_name"),
            F.col("uid_j").alias("compared_with_id"), F.col("name_j").alias("compared_with_name"),
            F.col("composite_score").alias("score"), F.col("classification").alias("status"),
            F.col("is_matched"), 
            F.col("failed_conditions"),
            F.col("transitivity_issue"), # <-- New Flag
            F.col("cluster_id_i"),       # <-- Context for debugging
            F.col("cluster_id_j"),       # <-- Context for debugging
            F.current_timestamp().alias("comparison_at")
        ]
        
        # ---------------------------------------------------------
        # 8. Write EVERYTHING to the audit log
        # ---------------------------------------------------------
        history_df = evaluated_pairs_df.select(*(base_cols + signal_cols)).withColumn(
            "failed_conditions",
            F.coalesce(
                F.col("failed_conditions").cast("array<string>"),
                F.expr("CAST(array() AS array<string>)")
            )
        )
        manager.write_table(table_history, history_df, mode="append")
        
        # ---------------------------------------------------------
        # 9. Write final clusters to the output table
        # ---------------------------------------------------------
        clean_output = output_df.dropDuplicates(["uid"])
        if existing_clusters_df is None:  # <--- FIXED HERE
            manager.create_table(table_output, df=clean_output.select("cluster_id", "name", "uid", "providerName"))
        else:
            manager.merge_table(
                table_name=table_output,
                df=clean_output.select("cluster_id", "name", "uid", "providerName"),
                key_columns=["uid"]
            )
    
    def build_match_expression(self, rule_node: dict) -> F.Column:
        """Recursively builds a PySpark boolean column expression from the nested config."""
        if "operator" in rule_node:
            op = rule_node["operator"].upper()
            child_exprs = [self.build_match_expression(child) for child in rule_node["rules"]]
            if not child_exprs: return F.lit(False)
            return functools.reduce(lambda x, y: x & y if op == "AND" else x | y, child_exprs)
        
        elif "signal" in rule_node:
            # Default nulls to 0.0 to prevent boolean logic from breaking
            signal_col = F.coalesce(F.col(rule_node["signal"]), F.lit(0.0))
            threshold = float(rule_node["threshold"])
            comp = rule_node["comparator"].lower()
            
            if comp == "gte": return signal_col >= threshold
            if comp == "lte": return signal_col <= threshold
            if comp == "gt":  return signal_col > threshold
            if comp == "lt":  return signal_col < threshold
            if comp == "eq":  return signal_col == threshold
            
        return F.lit(False)
    
    def _get_rule_name(self, rule_node: dict) -> str:
        """Helper function to generate a readable name for a rule or block."""
        if "signal" in rule_node:
            return f"{rule_node['signal']} {rule_node['comparator']} {rule_node['threshold']}"
        elif "operator" in rule_node:
            # Extract the first few signals to name the block (e.g., "OR Block (name_score_jaccard...)")
            signals = []
            for r in rule_node.get("rules", []):
                if "signal" in r:
                    signals.append(r["signal"])
                elif "rules" in r and "signal" in r["rules"][0]:
                    signals.append(r["rules"][0]["signal"])
                    
            signal_names = ", ".join(signals[:2]) + ("..." if len(signals) > 2 else "")
            return f"Failed {rule_node['operator']} Block ({signal_names})"
        return "Unknown Rule Block"

    def append_failure_reasons(self, df: DataFrame, match_logic_config: dict) -> DataFrame:
        """Evaluates each top-level rule and appends a list of failure reasons."""
        if match_logic_config.get("operator", "").upper() != "AND":
            # If root isn't AND (or config is empty), emit a typed empty array.
            return df.withColumn("failed_conditions", F.expr("CAST(array() AS array<string>)"))

        failure_cols = []
        
        # Iterate through the 7 top-level rules in your JSON
        for rule in match_logic_config.get("rules", []):
            # 1. Generate the boolean expression for this specific block
            rule_expr = self.build_match_expression(rule)
            
            # 2. Generate a readable name for this block
            rule_name = self._get_rule_name(rule)
            
            # 3. If the expression is FALSE, return the rule name. Otherwise, return NULL.
            failure_col = F.when(~rule_expr, F.lit(rule_name)).otherwise(F.lit(None))
            failure_cols.append(failure_col)

        if not failure_cols:
            return df.withColumn("failed_conditions", F.expr("CAST(array() AS array<string>)"))
            
        # Combine all failures into a single array and remove the NULLs
        # F.array_compact is available in Spark 3.4+ (You are on 3.5.3)
        return df.withColumn(
            "failed_conditions",
            F.array_compact(F.array(*failure_cols)).cast("array<string>")
        )