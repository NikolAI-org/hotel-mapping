import json
import os
import sys
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

# Ensure we can import custom modules (Standard Airflow setup)
sys.path.append('/opt/airflow')

from hotel_data.config.paths import (
    BASE_DELTA_PATH, 
    CATALOG_NAME, 
    SCHEMA_NAME, 
    TABLE_HOTELS_NAME, 
    TABLE_HOTELS_PAIRS_NAME
)
from hotel_data.delta.delta_table_manager import DeltaTableManager

# We assume you have or will define a constant for the final clusters table
TABLE_FINAL_CLUSTERS_NAME = "final_clusters" 

class PairScorer:
    def __init__(self, weights: dict, t_high: float, t_low: float):
        self.weights = weights
        self.t_high = t_high
        self.t_low = t_low
        self.total_weight = sum(weights.values()) if weights else 1.0

    def process(self, pairs_df):
        weighted_expr = "+".join([f"(coalesce({k}, 0) * {v})" for k, v in self.weights.items()])
        scored_df = pairs_df.withColumn("composite_score", F.expr(f"({weighted_expr}) / {self.total_weight}"))
        return scored_df.withColumn("classification", 
            F.when(F.col("composite_score") >= self.t_high, "MATCHED")
             .when(F.col("composite_score") >= self.t_low, "MANUAL_REVIEW")
             .otherwise("UNMATCHED")
        )

from pyspark.sql import functions as F
from pyspark.sql import Window

class IncrementalClusterResolver:

    def resolve(self, new_hotels_df, scored_pairs_df, existing_clusters_df):

        # 1️⃣ Filter MATCHED edges
        matches = scored_pairs_df \
            .filter(F.col("classification") == "MATCHED") \
            .select("uid_i", "uid_j")

        # 2️⃣ Build bidirectional edges using uid
        edges = matches.select(
            F.col("uid_i").alias("src"),
            F.col("uid_j").alias("dst")
        ).union(
            matches.select(
                F.col("uid_j").alias("src"),
                F.col("uid_i").alias("dst")
            )
        ).distinct()

        # 3️⃣ Build node set (existing clusters + new hotels) using uid
        existing_nodes = existing_clusters_df.select("uid", "cluster_id")

        new_nodes = new_hotels_df.select(
            "uid"
        ).withColumn("cluster_id", F.lit(None).cast("long"))

        nodes = existing_nodes.unionByName(new_nodes)

        # 4️⃣ Assign cluster IDs to new nodes
        max_cluster = existing_nodes.agg(
            F.max("cluster_id")
        ).collect()[0][0]

        max_cluster = max_cluster if max_cluster is not None else 0

        window = Window.orderBy("uid")

        nodes = nodes.withColumn(
            "cluster_id",
            F.when(
                F.col("cluster_id").isNull(),
                F.row_number().over(window) + max_cluster
            ).otherwise(F.col("cluster_id"))
        )

        # 5️⃣ Connected Components Iteration
        changed = True

        while changed:

            updated = nodes.alias("n") \
                .join(edges.alias("e"), F.col("n.uid") == F.col("e.src"), "left") \
                .join(nodes.alias("n2"), F.col("e.dst") == F.col("n2.uid"), "left") \
                .select(
                    F.col("n.uid"),
                    F.least(
                        F.col("n.cluster_id"),
                        F.coalesce(F.col("n2.cluster_id"), F.col("n.cluster_id"))
                    ).alias("cluster_id")
                )

            next_nodes = updated.groupBy("uid") \
                .agg(F.min("cluster_id").alias("cluster_id"))

            diff = next_nodes.subtract(nodes)
            changed = diff.count() > 0

            nodes = next_nodes

        # 6️⃣ Join cluster_id back to new provider records
        result = new_hotels_df.join(
            nodes.select("uid", "cluster_id"),
            "uid",
            "left"
        )

        return result

class EntityResolutionPipeline:
    def __init__(self, spark: SparkSession, config: dict):
        self.spark = spark
        self.scorer = PairScorer(config['weights'], config['t_high'], config['t_low'])
        self.resolver = IncrementalClusterResolver()

    def run(self, manager: DeltaTableManager, table_hotels: str, table_pairs: str,
            table_output: str, table_history: str, current_provider: str):

        # 1. Load Data
        new_hotels_df = manager.read_table(table_hotels) \
                               .filter(F.col("providerName") == current_provider)

        # 2. Load pairs involving this provider
        try:
            new_pairs_df = manager.read_table(table_pairs).filter(
                (F.col("providerName_i") == current_provider) |
                (F.col("providerName_j") == current_provider)
            )
            has_pairs = not new_pairs_df.isEmpty()
        except Exception:
            has_pairs = False

        # 3. Score and log history (only if pairs exist)
        scored_pairs = None
        if has_pairs:
            scored_pairs = self.scorer.process(new_pairs_df)

            history_df = scored_pairs.select(
                F.col("id_i").alias("hotel_id"),
                F.col("name_i").alias("hotel_name"),
                F.col("id_j").alias("compared_with_id"),
                F.col("name_j").alias("compared_with_name"),
                F.col("composite_score").alias("score"),
                F.col("classification").alias("status"),
                F.current_timestamp().alias("comparison_at")
            )
            manager.write_table(table_history, history_df, mode="append")

        # 4. Form clusters
        if not manager._table_exists(table_output):
            # FIRST RUN: initialize clusters with self IDs
            # output_df = new_hotels_df.withColumn("cluster_id", F.col("id"))
            # manager.create_table(
            #     table_output,
            #     df=output_df.select("id", "cluster_id", "name", "uid", "providerName"),
            # )
            window = Window.orderBy("id")

            output_df = new_hotels_df.withColumn(
                "cluster_id",
                F.row_number().over(window)
            )

            manager.create_table(
                table_output,
                df=output_df.select("id", "cluster_id", "name", "uid", "providerName"),
            )
        else:
            # INCREMENTAL RUN
            if has_pairs:
                existing_clusters_df = manager.read_table(table_output)
                output_df = self.resolver.resolve(
                    new_hotels_df, scored_pairs, existing_clusters_df
                )
            else:
                # No pairs for this provider: fall back to self clusters
                output_df = new_hotels_df.withColumn("cluster_id", F.col("id"))

            manager.merge_table(
                table_name=table_output,
                df=output_df.select("id", "cluster_id", "name", "uid", "providerName"),
                key_columns=["id"],
            )


def main():
    spark = SparkSession.builder.appName("HotelEntityResolution").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # These would typically come from Airflow DAG parameters
    current_provider = os.getenv('PROVIDER_NAME', 'hbose') 
    TABLE_HISTORY_NAME = "comparison_audit_log"

    config = {
        'weights': json.loads(os.getenv('WEIGHTS', '{"name_score": 0.7, "addr_score": 0.3}')),
        't_high': float(os.getenv('THRESHOLD_HIGH', 0.85)),
        't_low': float(os.getenv('THRESHOLD_LOW', 0.80))
    }

    manager = DeltaTableManager(spark, CATALOG_NAME, SCHEMA_NAME, BASE_DELTA_PATH)
    pipeline = EntityResolutionPipeline(spark, config)
    
    pipeline.run(
        manager=manager, 
        table_hotels=TABLE_HOTELS_NAME, 
        table_pairs=TABLE_HOTELS_PAIRS_NAME, 
        table_output=TABLE_FINAL_CLUSTERS_NAME,
        table_history=TABLE_HISTORY_NAME,
        current_provider=current_provider
    )

if __name__ == "__main__":
    main()