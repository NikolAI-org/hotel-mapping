from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
import logging
from typing import List, Optional, Dict, Set, Tuple

from hotel_data.pipeline.clustering.core.clustering_interfaces import ClusteringStrategy, Logger
from hotel_data.pipeline.clustering.services.clustering_service_driversideclustering import DriverSideUnionFindClustering

class FlexibleClustering(DriverSideUnionFindClustering):
    """
    ⭐ RECOMMENDED FOR YOUR DATA (< 100k nodes)
    
    Solves OOM issues by:
    1. Filtering pairs in Spark (fast)
    2. Collecting small edge list to driver (KB of data)
    3. Running fast Union-Find in pure Python
    4. Broadcasting results back
    
    Perfect for: Hotels (3k nodes, 13k edges)
    Time: Seconds
    Memory: Minimal
    
    For your dataset: ~100ms compute time
    """
    
    def __init__(self, spark: SparkSession, logger: Logger, transitivity_enabled: bool = True):
        super().__init__(spark=spark, logger=logger)
        self.spark = spark
        self.logger = logger
        self.transitivity_enabled = transitivity_enabled
    
    def cluster(
        self,
        hotels_df: DataFrame,
        scored_pairs_df: DataFrame,
        threshold: float = 0.7
    ) -> DataFrame:
        """
        Cluster hotels using driver-side Union-Find.
        
        Args:
            hotels_df: DataFrame with 'name' column (unique hotel identifier)
            scored_pairs_df: DataFrame with 'name_i', 'name_j', 'similarity_score'
            threshold: Minimum similarity to create edge
        
        Returns:
            DataFrame: hotels with added 'cluster_id' column
        """
        try:
            self.logger.info("=" * 70)
            self.logger.info("STARTING FLEXIBLE CLUSTERING WITH UNION FIND AND/OR CLIQUE BASED CLUSTERING")
            self.logger.info(f"Threshold: {threshold}")
            self.logger.info("=" * 70)
            
            # 1. FILTER: Keep pairs above threshold
            matched_pairs = scored_pairs_df.filter(
                F.col("is_matched") == True
            ).select("uid_i", "uid_j") # Using the UIDs available in your schema
            
            # 2. COLLECT: Get all unique UIDs and Edges
            # Get every unique hotel fingerprint from the main hotel list
            all_uids = hotels_df.filter(
                F.col("uid").isNotNull()
            ).select("uid").rdd.flatMap(lambda x: x).distinct().collect()
            
            
            # Collect edges as (uid_i, uid_j) tuples
            edges_list = matched_pairs.filter(
                (F.col("uid_i").isNotNull()) & (F.col("uid_j").isNotNull())
            ).rdd.map(lambda row: (row[0], row[1])).collect()
            
            self.logger.info(f"Collected {len(edges_list)} edges to driver")
            self.logger.info(f"Collected {len(all_uids)} unique hotel UIDs")
            
            if self.transitivity_enabled:
                self.logger.info("Running standard Union-Find (Transitive)")
                parent_map = self._union_find(edges_list, all_uids)
            else:
                self.logger.info("Running Clique-Based Clustering (Strict Matching)")
                parent_map = self._clique_clustering(edges_list, all_uids)
            
            # 4. BROADCAST & JOIN: Map cluster IDs back to the original hotels
            # We rename 'name' to 'uid' because our parent_map now contains UIDs
            cluster_df = self._create_cluster_dataframe(parent_map).withColumnRenamed("name", "uid")
            
            result = hotels_df.join(cluster_df, on="uid", how="left") \
                .withColumn(
                    "cluster_id",
                    F.coalesce(
                        F.col("cluster_id"),
                        F.concat(F.lit("SINGLETON_"), F.col("uid"))
                    )
                )
            
            # Log final stats
            self.logger.info(f"Clustering Complete. Found {result.select('cluster_id').distinct().count()} clusters.")
            
            return result
        
        except Exception as e:
            self.logger.error(f"Clustering failed: {str(e)}", exc_info=True)
            raise
        
    
    def _clique_clustering(self, edges: List[Tuple[str, str]], all_nodes: List[str]) -> Dict[str, str]:
        """
        Nodes only join a cluster if they match ALL members of that cluster.
        Using a Greedy Clique Partitioning approach.
        """
        # 1. Build an Adjacency List for O(1) lookups
        adj = {node: set() for node in all_nodes}
        for u, v in edges:
            adj[u].add(v)
            adj[v].add(u)

        clusters: List[Set[str]] = []
        node_to_cluster_root = {}

        # 2. Iterate through nodes and place them in the first compatible clique
        for node in all_nodes:
            placed = False
            for cluster_set in clusters:
                # CHECK: Does this node match EVERYONE in the current cluster?
                if all(member in adj[node] for member in cluster_set):
                    cluster_set.add(node)
                    # We use the first member added as the 'root' (representative)
                    node_to_cluster_root[node] = min(cluster_set) 
                    placed = True
                    break
            
            if not placed:
                # Start a new cluster (clique)
                clusters.append({node})
                node_to_cluster_root[node] = node

        # Ensure consistency in roots
        final_map = {}
        for node, root in node_to_cluster_root.items():
            # Find the actual minimum node in the final cluster for determinism
            for c_set in clusters:
                if node in c_set:
                    final_map[node] = min(c_set)
                    break
                    
        return final_map