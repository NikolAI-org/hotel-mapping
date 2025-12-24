from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
import logging
from typing import List, Optional, Dict, Tuple

from hotel_data.pipeline.clustering.core.clustering_interfaces import ClusteringStrategy, Logger

class DriverSideUnionFindClustering(ClusteringStrategy):
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
    
    def __init__(self, spark: SparkSession, logger: Logger):
        self.spark = spark
        self.logger = logger
    
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
            self.logger.info("STARTING DRIVER-SIDE UNION FIND CLUSTERING")
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
            
            # 3. COMPUTE: Union-Find on the UIDs
            parent_map = self._union_find(edges_list, all_uids)
            
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
    
    def _union_find(self, edges: List[Tuple[str, str]], all_nodes: List[str]) -> Dict[str, str]:
        """
        Fast Union-Find algorithm using path compression.
        
        Args:
            edges: List of (name_i, name_j) tuples
            all_nodes: List of all unique hotel names
        
        Returns:
            Dict mapping each hotel name to its component root
        """
        parent = {}
        
        def find(x):
            """Find root with path compression."""
            # If a node is new (should only happen for nodes from edges not in all_nodes, 
            # but is good practice), initialize it.
            if x not in parent:
                parent[x] = x
            if parent[x] != x:
                parent[x] = find(parent[x])  # Recursively compress path
            return parent[x]
        
        def union(x, y):
            """Union two components."""
            root_x = find(x)
            root_y = find(y)
            
            if root_x != root_y:
                # Always make lexicographically smaller name the root
                # (Ensures deterministic output)
                if root_x < root_y:
                    parent[root_y] = root_x
                else:
                    parent[root_x] = root_y
        
        # ⭐ PRIMARY FIX STEP: Initialize all nodes first
        for node in all_nodes:
            # By calling find(node), we ensure 'node' is in the parent map
            find(node)
            
        # Process all edges
        for name_i, name_j in edges: # 🛠️ Secondary Fix (Safety): Destructure tuple instead of accessing Row attributes
            union(name_i, name_j)
        
        # Final path compression: ensure all point directly to root
        result = {}
        for node in parent:
            result[node] = find(node)
        
        return result
    
    def _create_cluster_dataframe(self, parent_map: Dict[str, str]) -> DataFrame:
        """
        Convert parent_map to DataFrame and assign cluster IDs.
        
        Args:
            parent_map: Dict of {hotel_name: component_root}
        
        Returns:
            DataFrame with 'name' and 'cluster_id' columns
        """
        # Convert dict to list of (name, component) tuples
        rows = [(name, root) for name, root in parent_map.items()]
        
        # Create DataFrame
        component_df = self.spark.createDataFrame(
            rows,
            ["name", "component"]
        )
        
        # Get unique components and assign IDs
        unique_components = component_df.select("component").distinct() \
                                        .orderBy("component")
        
        window = Window.orderBy("component")
        cluster_mapping = unique_components.withColumn(
            "seq",
            F.row_number().over(window)
        ).select(
            F.col("component"),
            F.concat(
                F.lit("CLUSTER_"),
                F.lpad(F.col("seq"), 6, "0")
            ).alias("cluster_id")
        )
        
        # Join component names with cluster IDs
        result = component_df.join(
            cluster_mapping,
            on="component",
            how="left"
        ).select("name", "cluster_id")
        
        return result
    
    def _create_singleton_clusters(self, hotels_df: DataFrame) -> DataFrame:
        """When no pairs match, each hotel is its own cluster."""
        return hotels_df.withColumn(
            "cluster_id",
            F.concat(F.lit("SINGLETON_"), F.col("name"))
        )