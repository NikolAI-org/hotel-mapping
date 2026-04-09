from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# --- THE CONTRACT ---
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# --- NEW: STANDARDIZED DTO ---
@dataclass
class ClusteringResult:
    """Standardized output for all clustering strategies."""
    mapped_hotels: DataFrame  # The final joined output
    audit_decisions: Optional[DataFrame] = None # Used by 3-tier
    new_singletons: Optional[DataFrame] = None  # Used by 3-tier


# --- THE CONTRACT ---
class ClusteringStrategy(ABC):
    @abstractmethod
    def cluster(self, new_hotels_df: DataFrame, pairs_df: DataFrame, 
                existing_state_df: Optional[DataFrame] = None, 
                existing_mappings_df: Optional[DataFrame] = None) -> ClusteringResult:
        """
        existing_state_df could be the Output Table (Transitive) 
        or the Canonical Registry (Non-Transitive).
        """
        pass


# --- STRATEGY 1: NON-TRANSITIVE (Hub-and-Spoke / 0-FP) ---
# --- Update NonTransitiveStrategy ---
class NonTransitiveStrategy(ClusteringStrategy):
    def __init__(self, veto_engine, router, id_generator, scorer, match_evaluator, cohesion_validator):
        self.veto_engine = veto_engine
        self.router = router
        self.id_generator = id_generator
        self.scorer = scorer
        self.match_evaluator = match_evaluator
        self.cohesion_validator = cohesion_validator # <-- INJECT IT HERE

    def cluster(self, new_hotels_df: DataFrame, pairs_df: DataFrame, 
                existing_state_df: Optional[DataFrame] = None, 
                existing_mappings_df: Optional[DataFrame] = None) -> ClusteringResult:
        
        # 1. Base Scoring & Logic
        scored_pairs = self.scorer.process(pairs_df)
        boolean_evaluated_pairs = self.match_evaluator(scored_pairs)
        
        vetoed_pairs = self.veto_engine.apply(boolean_evaluated_pairs)
        # Cache the dataframe temporarily so the .show() doesn't recalculate the whole DAG
        vetoed_pairs.cache() 
        
        veto_count = vetoed_pairs.filter(F.col("is_vetoed") == True).count()
        print(f"🛑 VETO ENGINE LOG: Caught {veto_count} vetoed pairs!")
        
        if veto_count > 0:
            print("🛑 VETO ENGINE LOG: Sample of records destroyed by the Veto rules:")
            vetoed_pairs.filter(F.col("is_vetoed") == True).select(
                "uid_i", 
                "uid_j", 
                "name_i", 
                "name_j", 
                "geo_distance_km", 
                "name_score_jaccard",
                "veto_reason",
                "composite_score" # You will see this is exactly 0.0
            ).show(10, truncate=False)
        # --------------------------------------
        routed_decisions = self.router.route(vetoed_pairs)

        # --- FIX: Map uid_j to Canonical ID ---
        # Because pairs are scored against existing spokes (h1), we need to look up h1's cluster ID.
        if existing_mappings_df is not None:
            hub_lookup = existing_mappings_df.select(
                F.col("uid").alias("uid_j"), 
                F.col("canonical_id").alias("assigned_canonical_id")
            )
            routed_with_hubs = routed_decisions.join(hub_lookup, on="uid_j", how="left")
            # If the target hotel was somehow missing from mappings, fall back to its own UID
            routed_with_hubs = routed_with_hubs.withColumn(
                "assigned_canonical_id", F.coalesce(F.col("assigned_canonical_id"), F.col("uid_j"))
            )
        else:
            routed_with_hubs = routed_decisions.withColumn("assigned_canonical_id", F.col("uid_j"))


        # --- NEW: INTRA-CLUSTER COHESION CHECK ---
        # Extract pairs that passed all boolean and score thresholds
        valid_pairs = vetoed_pairs.filter((F.col("is_matched") == True) & (F.col("is_vetoed") == False))
        
        # Run Cohesion Validator
        validated_decisions = self.cohesion_validator.validate(routed_with_hubs, existing_mappings_df, valid_pairs)

        # ----------------------------------------
        
        # Now filter based on the VALIDATED decisions
        to_attach = validated_decisions.filter(F.col("routing_decision") == "ATTACH_TO_CANONICAL")
        singletons = validated_decisions.filter(
            F.col("routing_decision").isin(["CREATE_NEW_SINGLETON", "CONFLICT_AMBIGUOUS", "CONFLICT_VETOED", "FAILED_BOOLEAN_LOGIC", "CONFLICT_FAILED_COHESION"])
        )

        # CATCH ORPHANED HOTELS (NO PAIRS)
        routed_uids = validated_decisions.select(F.col("uid_i").alias("uid")).distinct()
        pure_singletons_df = new_hotels_df.join(routed_uids, on="uid", how="left_anti")
        pure_singletons_generated = self.id_generator.generate(pure_singletons_df, "uid")
        
        pure_singletons_decisions = pure_singletons_generated.select(
            F.col("uid").alias("uid_i"), F.lit(None).cast("string").alias("uid_j"),
            F.lit("CREATE_NEW_SINGLETON").alias("routing_decision"), F.lit(0.0).cast("double").alias("composite_score"),
            F.lit("NO_CANDIDATE_PAIRS").alias("veto_reason"), F.col("cluster_id").alias("assigned_canonical_id")
        )

        # Generate IDs for Conflict Singletons
        hotels_to_gen = new_hotels_df.join(singletons.select(F.col("uid_i").alias("uid")), on="uid", how="inner")
        singletons_generated = self.id_generator.generate(hotels_to_gen, "uid")
        
        singletons_final = singletons.drop("assigned_canonical_id").join(
            singletons_generated.select(
                F.col("uid").alias("uid_i"), 
                F.col("cluster_id").alias("assigned_canonical_id")
            ),
            on="uid_i", how="inner"
        )
        
        # Generate Attachments
        attached_df = to_attach.select("uid_i", "uid_j", "routing_decision", "composite_score", "veto_reason", "assigned_canonical_id")
        
        if existing_state_df is None:
            # FIX: During a cold start, all targets (uid_j) are UIDs from the current batch.
            # We must generate the Hub ID using the TARGET hotel's data, not the source hotel's data!
            attached_with_geo = new_hotels_df.join(
                attached_df.drop("assigned_canonical_id").withColumnRenamed("uid_j", "uid"), 
                on="uid", 
                how="inner"
            )
            attached_df = self.id_generator.generate(attached_with_geo, "uid") \
                                           .withColumnRenamed("uid", "uid_j") \
                                           .withColumn("assigned_canonical_id", F.col("cluster_id"))

        # Union EVERYTHING
        final_decisions = attached_df.select("uid_i", "uid_j", "routing_decision", "composite_score", "veto_reason", "assigned_canonical_id") \
            .unionByName(singletons_final.select("uid_i", "uid_j", "routing_decision", "composite_score", "veto_reason", "assigned_canonical_id"), allowMissingColumns=True) \
            .unionByName(pure_singletons_decisions, allowMissingColumns=True)

        all_new_singletons = singletons_generated.unionByName(pure_singletons_generated, allowMissingColumns=True)

        final_mapping_df = final_decisions.select(F.col("uid_i").alias("uid"), F.col("assigned_canonical_id").alias("cluster_id")).dropDuplicates(["uid"])
        mapped_hotels = new_hotels_df.join(final_mapping_df, on="uid", how="left")

        return ClusteringResult(mapped_hotels=mapped_hotels, audit_decisions=final_decisions, new_singletons=all_new_singletons)


# --- STRATEGY 2: TRANSITIVE (Graph / Union-Find) ---
class TransitiveStrategy(ClusteringStrategy):
    def __init__(self, spark, id_generator, match_evaluator):
        self.spark = spark
        self.id_generator = id_generator
        self.match_evaluator = match_evaluator # A function/class that applies your YAML boolean logic

    def cluster(self, new_hotels_df: DataFrame, pairs_df: DataFrame, existing_state_df: Optional[DataFrame] = None, existing_mappings_df: Optional[DataFrame] = None) -> ClusteringResult:
        # 1. Apply strict boolean matching logic
        matched_pairs_df = self.match_evaluator(pairs_df).filter(F.col("is_matched") == True)
        edges_list = matched_pairs_df.select("uid_i", "uid_j").rdd.map(lambda row: (row[0], row[1])).collect()
        
        new_uids = [row.uid for row in new_hotels_df.select("uid").distinct().collect()]
        
        existing_map = {}
        if existing_state_df:
            existing_rows = existing_state_df.select("uid", "cluster_id").collect()
            existing_map = {str(r['uid']): str(r['cluster_id']) for r in existing_rows}

        # 2. Driver-side Union-Find
        parent_map = self._driver_side_union_find(edges_list, existing_map, new_uids)
        
        # 3. Map roots back to Spark
        cluster_data = [(uid, str(root)) for uid, root in parent_map.items() if uid in new_uids]
        cluster_df = self.spark.createDataFrame(cluster_data, ["uid", "assigned_root"])
        
        joined_df = new_hotels_df.join(cluster_df, on="uid", how="left")

        # 4. Apply Immutable Geo-Hash logic to the root
        clean_country = F.regexp_replace(F.coalesce(F.col("contact_address_country_code"), F.lit("XX")), r"\s+", "_")
        clean_city = F.regexp_replace(F.coalesce(F.col("contact_address_city_name"), F.lit("UNKNOWN")), r"\s+", "_")
        clean_area = F.regexp_replace(F.coalesce(F.col("contact_address_state_name"), F.lit("UNKNOWN")), r"\s+", "_")

        w = Window.partitionBy("assigned_root").orderBy("uid")
        geo_prefix = F.concat_ws("-",
            F.upper(F.first(clean_country, ignorenulls=True).over(w)),
            F.upper(F.first(clean_city, ignorenulls=True).over(w)),
            F.upper(F.first(clean_area, ignorenulls=True).over(w))
        )
        
        final_df = joined_df.withColumn(
            "cluster_id",
            F.when(F.col("assigned_root").contains("-"), F.col("assigned_root"))
             .otherwise(F.concat_ws("-", geo_prefix, F.substring(F.sha2(F.col("assigned_root"), 256), 1, 12)))
        ).drop("assigned_root")

        # Returns the DTO with ONLY mapped_hotels (audit/singletons remain None)
        return ClusteringResult(mapped_hotels=final_df)

        # return joined_df.withColumn(
        #     "cluster_id",
        #     F.when(F.col("assigned_root").contains("-"), F.col("assigned_root"))
        #      .otherwise(F.concat_ws("-", geo_prefix, F.substring(F.sha2(F.col("assigned_root"), 256), 1, 12)))
        # ).drop("assigned_root")

    def _driver_side_union_find(self, edges, existing_map, new_uids):
        # Your existing robust Union-Find logic goes here exactly as previously defined
        # Priority 1: Keep existing String IDs. Priority 2: Smallest UID wins.
        parent = {}
        for uid, cid in existing_map.items():
            parent[uid] = cid
            if cid not in parent: parent[cid] = cid
        for uid in new_uids:
            if uid not in parent: parent[uid] = uid

        def find(i):
            if parent[i] == i: return i
            parent[i] = find(parent[i])
            return parent[i]

        for u, v in edges:
            if u in parent and v in parent:
                root_u, root_v = find(u), find(v)
                if root_u != root_v:
                    is_cluster_u = "-" in str(root_u)
                    is_cluster_v = "-" in str(root_v)
                    if is_cluster_u and not is_cluster_v: parent[root_v] = root_u
                    elif is_cluster_v and not is_cluster_u: parent[root_u] = root_v
                    else:
                        if str(root_u) < str(root_v): parent[root_v] = root_u
                        else: parent[root_u] = root_v
        
        return {uid: find(uid) for uid in list(existing_map.keys()) + new_uids}