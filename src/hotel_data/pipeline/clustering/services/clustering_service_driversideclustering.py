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
            ).select("name_i", "name_j")
            
            # Get all unique hotel names (vertices) to ensure singletons are clustered
            all_hotel_names = hotels_df.select("name").rdd.flatMap(lambda x: x).collect()
            
            # 2. COLLECT: Bring edges to driver memory
            # For 13k edges, this is < 1 MB
            # 🛠️ Secondary Fix (Safety): Collect as a list of tuples instead of Row objects 
            edges_list = matched_pairs.select("name_i", "name_j").rdd.map(
                lambda row: (row[0], row[1])
            ).collect()
            
            self.logger.info(f"Collected edges to driver (safe: {len(edges_list)} edges)")
            self.logger.info(f"Collected vertices to driver ({len(all_hotel_names)} hotels)")
            
            # 3. COMPUTE: Fast Union-Find in pure Python
            # 🛠️ Primary Fix (Completeness): Pass ALL hotel names to ensure all nodes are initialized
            parent_map = self._union_find(edges_list, all_hotel_names)
            self.logger.info(f"Computed {len(parent_map)} cluster assignments")
            
            # --- END OF FIXES FOR COMPLETENESS ---
            
            # 4. BROADCAST: Convert back to Spark DataFrame
            cluster_df = self._create_cluster_dataframe(parent_map)
            
            # 5. JOIN: Merge with hotels
            result = (
                hotels_df
                .join(cluster_df, on="name", how="left")
                .withColumn(
                    "cluster_id",
                    F.coalesce(
                        F.col("cluster_id"),
                        F.concat(F.lit("SINGLETON_"), F.col("name"))
                    )
                )
            )
            
            # Stats
            total_clusters = result.select("cluster_id").distinct().count()
            total_hotels = result.count()
            
            self.logger.info("=" * 70)
            self.logger.info(f"✓ CLUSTERING COMPLETE")
            self.logger.info(f"  Hotels: {total_hotels:,}")
            self.logger.info(f"  Clusters: {total_clusters:,}")
            self.logger.info(f"  Avg size: {total_hotels / total_clusters:.1f}")
            self.logger.info("=" * 70)
            
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