from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from spark.jobs.cluster.strategies import ClusteringResult

class EntityResolutionPipeline:
    def __init__(self, spark, strategy, id_generator):
        self.spark = spark
        self.strategy = strategy
        self.id_generator = id_generator

    def run(self, manager, table_hotels, table_pairs, table_registry, table_mappings, table_audit, current_provider):
        new_hotels_df = manager.read_table(table_hotels) \
            .filter(F.col("providerName") == current_provider) \
            .dropDuplicates(["uid"])

        try:
            new_pairs_df = manager.read_table(table_pairs).filter(
                (F.col("providerName_i") == current_provider) | (F.col("providerName_j") == current_provider)
            )
            has_pairs = not new_pairs_df.isEmpty()
        except Exception:
            has_pairs = False
            new_pairs_df = None

        # existing_state_df = manager.read_table(table_registry) if manager._table_exists(table_registry) else None

        existing_registry_df = manager.read_table(table_registry) if manager._table_exists(table_registry) else None
        existing_mappings_df = manager.read_table(table_mappings) if manager._table_exists(table_mappings) else None
        # --- EXECUTE STRATEGY ---
        if has_pairs:
            # Pairs are stored anchor=_i, challenger=_j. Ensure current_provider is always uid_i
            # so RoutingDecisionEngine partitions by the right hotel.
            new_pairs_df = self._normalize_pair_orientation(new_pairs_df, current_provider)
            # result = self.strategy.cluster(new_hotels_df, new_pairs_df, existing_registry_df)
            result = self.strategy.cluster(new_hotels_df, new_pairs_df, existing_registry_df, existing_mappings_df)
        else:
            # Fallback: A2 scenario (No pairs = all new singletons)
            singletons_df = self.id_generator.generate(new_hotels_df, uid_col="uid")
            
            # Create a mock decisions dataframe for the Audit Log & Mappings
            mock_decisions = singletons_df.select(
                F.col("uid").alias("uid_i"),
                F.lit(None).cast("string").alias("uid_j"),
                F.lit(0.0).alias("composite_score"),
                F.lit("CREATE_NEW_SINGLETON").alias("routing_decision"),
                F.lit(None).cast("string").alias("veto_reason"),
                F.col("cluster_id").alias("assigned_canonical_id")
            )
            
            result = ClusteringResult(
                mapped_hotels=singletons_df,
                audit_decisions=mock_decisions,
                new_singletons=singletons_df
            )

        # --- DYNAMIC PERSISTENCE ---
        if result.audit_decisions is not None:
            # 3-Tier Architecture (Non-Transitive / Hub-and-Spoke)
            self._write_audit_log(manager, table_audit, result.audit_decisions)
            
            # FIX: Pass audit_decisions to the cluster mappings, NOT mapped_hotels
            # self._write_cluster_mappings(manager, table_mappings, result.audit_decisions)
            self._write_cluster_mappings(manager, table_mappings, result.audit_decisions, current_provider)
            
        else:
            # 1-Tier Architecture (Transitive Legacy Output)
            # In this mode, table_mappings acts as the main combined output table
            clean_output = result.mapped_hotels.dropDuplicates(["uid"]).select("cluster_id", "name", "uid", "providerName")
            if not manager._table_exists(table_mappings):
                manager.create_table(table_mappings, df=clean_output)
            else:
                manager.merge_table(table_mappings, df=clean_output, key_columns=["uid"])

        # Write Canonical Registry Hubs (If provided by the strategy)
        if result.new_singletons is not None:
            self._write_canonical_registry(manager, table_registry, result.new_singletons)
            

    # --- TIER 1: The Audit Log (Append Only) ---
    def _write_audit_log(self, manager, table_audit, decisions_df: DataFrame):
        audit_df = decisions_df.select(
            F.col("uid_i").alias("uid"), 
            F.col("uid_j").alias("target_canonical_id"),
            F.col("routing_decision").alias("decision"),
            F.col("composite_score").alias("confidence_score"), 
            F.col("veto_reason"),
            F.current_timestamp().alias("timestamp")
        )
        manager.write_table(table_audit, audit_df, mode="append")

    # --- TIER 2: Cluster Mappings (The Spokes - Upsert) ---
    def _write_cluster_mappings(self, manager, table_mappings, decisions_df: DataFrame, current_provider: str):
        # We map records that successfully attached or were created as singletons
        valid_mappings = decisions_df.filter(
            F.col("routing_decision").isin([
                "ATTACH_TO_CANONICAL", 
                "CREATE_NEW_SINGLETON", 
                "CONFLICT_AMBIGUOUS", 
                "CONFLICT_VETOED", 
                "FAILED_BOOLEAN_LOGIC" 
            ])
        ).select(
            F.col("uid_i").alias("uid"),
            F.col("assigned_canonical_id").alias("canonical_id"),
            F.col("composite_score").alias("confidence_score"),
            F.lit("ACTIVE").alias("mapping_state"),
            F.lit(current_provider).alias("providerName"), # <-- FIX: Append the provider name!
            F.current_timestamp().alias("updated_at")
        ).dropDuplicates(["uid"])

        if not manager._table_exists(table_mappings):
            manager.create_table(table_mappings, df=valid_mappings)
        else:
            manager.merge_table(table_name=table_mappings, df=valid_mappings, key_columns=["uid"])

    # --- PAIR ORIENTATION NORMALIZER ---
    @staticmethod
    def _normalize_pair_orientation(pairs_df: DataFrame, current_provider: str) -> DataFrame:
        """Flip *_i/*_j column pairs so current_provider's hotel is always uid_i.

        Pairs are written by the scoring job with anchor→_i and challenger→_j.
        When the current provider was the challenger its hotels sit on the _j side,
        causing RoutingDecisionEngine (which partitions by uid_i) to route the wrong
        supplier.  Any pair where providerName_j == current_provider is flipped here.
        """
        needs_flip = F.col("providerName_j") == current_provider

        cols = pairs_df.columns
        j_set = set(c for c in cols if c.endswith("_j"))
        paired_bases = set(c[:-2] for c in cols if c.endswith("_i") and c[:-2] + "_j" in j_set)

        select_exprs = []
        handled = set()
        for col_name in cols:
            if col_name in handled:
                continue
            if col_name.endswith("_i") and col_name[:-2] in paired_bases:
                j_col = col_name[:-2] + "_j"
                select_exprs.append(
                    F.when(needs_flip, F.col(j_col)).otherwise(F.col(col_name)).alias(col_name)
                )
                select_exprs.append(
                    F.when(needs_flip, F.col(col_name)).otherwise(F.col(j_col)).alias(j_col)
                )
                handled.add(col_name)
                handled.add(j_col)
            elif col_name not in handled:
                select_exprs.append(F.col(col_name))
                handled.add(col_name)

        return pairs_df.select(select_exprs)

    # --- TIER 3: Canonical Registry (The Hubs - Upsert) ---
    def _write_canonical_registry(self, manager, table_registry, singletons_df: DataFrame):
        # We only insert NEW Canonical IDs here. B3 (Golden Record Updates) handles updates later.
        registry_inserts = singletons_df.select(
            F.col("cluster_id").alias("canonical_id"),
            F.col("name").alias("canonical_name"),
            F.col("contact_address_country_code").alias("country_code"),
            F.lit("ACTIVE").alias("state"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at")
        ).dropDuplicates(["canonical_id"])

        if not manager._table_exists(table_registry):
            manager.create_table(table_registry, df=registry_inserts)
        else:
            manager.merge_table(
                table_name=table_registry, 
                df=registry_inserts, 
                key_columns=["canonical_id"]
            )