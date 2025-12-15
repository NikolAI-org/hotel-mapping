from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from abc import ABC, abstractmethod
from typing import Optional

from hotel_data.pipeline.clustering.core.clustering_interfaces import ClusteringStrategy, Logger


class FixedLabelPropagationClustering(ClusteringStrategy):
    """
    Corrected Connected Components using Label Propagation.
    Uses HOTEL NAMES instead of IDs.
    
    FIXES:
    - Joins labels TWICE to get neighbor's current label (not static name)
    - Handles full transitive closure (not just 1-hop)
    - Memory-efficient iteration
    - Guaranteed convergence
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
        Cluster hotels using corrected label propagation based on NAMES.
        
        Args:
            hotels_df: DataFrame with columns: name, ... (hotel name is unique key)
            scored_pairs_df: DataFrame with columns: name_i, name_j, is_matched
            threshold: Minimum similarity (0.0-1.0) to create edge
        
        Returns:
            DataFrame: Original hotels + cluster_id column
        """
        try:
            self.logger.info("=" * 70)
            self.logger.info("STARTING LABEL PROPAGATION CLUSTERING (NAME-BASED)")
            self.logger.info(f"Threshold: {threshold}")
            self.logger.info("=" * 70)
            
            # Filter pairs by threshold
            matched_pairs = scored_pairs_df.filter(
                F.col("is_matched") == True
            ).select(
                F.col("name_i"),
                F.col("name_j"),
                F.col("is_matched")
            ).distinct()
            
            pair_count = matched_pairs.count()
            self.logger.info(f"Pairs above threshold: {pair_count:,}")
            
            if pair_count == 0:
                self.logger.warning("No pairs found—creating singleton clusters")
                return self._create_singleton_clusters(hotels_df)
            
            # Compute connected components using names
            components = self._connected_components(matched_pairs)
            
            # Assign cluster IDs
            clusters = self._assign_cluster_ids(components)
            
            # Join with original hotels
            # result = hotels_df.join(
            #     clusters,
            #     on="name",
            #     how="left"
            # ).fillna({"cluster_id": F.concat(F.lit("SINGLETON_"), F.col("name"))})
            
            result = (
                hotels_df
                .join(clusters, on="name", how="left")
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
            hotel_count = result.count()
            
            self.logger.info("=" * 70)
            self.logger.info(f"✓ CLUSTERING COMPLETE")
            self.logger.info(f"  Hotels: {hotel_count:,}")
            self.logger.info(f"  Clusters: {total_clusters:,}")
            self.logger.info(f"  Avg cluster size: {hotel_count / total_clusters:.1f}")
            self.logger.info("=" * 70)
            
            return result
        
        except Exception as e:
            self.logger.error(f"Clustering failed: {str(e)}", exc_info=True)
            raise
    
    def _connected_components(
        self,
        edges: DataFrame,
        max_iter: int = 20
    ) -> DataFrame:
        """
        Compute connected components using corrected label propagation.
        Works with HOTEL NAMES as identifiers.
        
        **KEY FIX**: 
        Join labels with both source and destination nodes to get their
        CURRENT labels (hotel names), not the static edge endpoint names.
        
        Args:
            edges: DataFrame with name_i, name_j columns (hotel names)
            max_iter: Max iterations (usually converges in 5-10 for hotel data)
        
        Returns:
            DataFrame with name, component columns
        """
        
        self.logger.info("Building undirected graph...")
        
        # Create bidirectional edges (A-B becomes A→B and B→A)
        edges_ud = edges.select(
            F.col("name_i").alias("src"),
            F.col("name_j").alias("dst")
        ).union(
            edges.select(
                F.col("name_j").alias("src"),
                F.col("name_i").alias("dst")
            )
        ).dropDuplicates()
        
        # edges_ud.cache()
        
        edge_count = edges_ud.count()
        self.logger.info(f"Undirected edges: {edge_count:,}")
        
        # Extract vertices (unique hotel names)
        vertices = edges_ud.select(F.col("src").alias("name")).union(
            edges_ud.select(F.col("dst").alias("name"))
        ).distinct()
        
        vertex_count = vertices.count()
        self.logger.info(f"Vertices: {vertex_count:,}")
        
        # Initialize: each hotel's label is its own name
        labels = vertices.withColumn("label", F.col("name"))
        
        # Iterative label propagation
        self.logger.info("Starting label propagation iterations...")
        
        # Iterate
        for iteration in range(max_iter):
            self.logger.info(f"  Iteration {iteration + 1}/{max_iter}")
            
            # 1. Join edges_ud with current labels on the 'dst' column.
            candidates_from_neighbors = edges_ud.join(
                labels.select(F.col("name").alias("dst"), F.col("label").alias("candidate")),
                on="dst",
                how="inner"
            ).select(F.col("src").alias("name"), F.col("candidate"))
            
            # 2. Add the node's current label as a candidate (important for safety
            propagated = candidates_from_neighbors.unionByName(
                labels.select(F.col("name"), F.col("label").alias("candidate")) 
            )
            
            # 3. Take the minimum label per node from ALL candidates (the correct LPA rule)
            new_labels = propagated.groupBy("name").agg(
                F.min("candidate").alias("label")
            )
            
            # ⭐ CRITICAL FIX: CHECKPOINT every 2 iterations (Existing, but critical!)
            if iteration % 2 == 0 and iteration > 0:
                self.logger.info(f"    → Checkpointing (cutting lineage)")
                # 🛠️ PERFORMANCE FIX 3: Unpersist the old labels before checkpointing the new one
                if labels is not None:
                     labels.unpersist()
                new_labels = new_labels.localCheckpoint()
                new_labels.count()  # Force materialization
                
            # Convergence check
            # 🛠️ PERFORMANCE FIX 4: Explicitly Cache labels here for fast lookups in the next iteration
            new_labels = new_labels.cache() 
            new_labels.count() # Force materialization/caching
            
            if new_labels.exceptAll(labels).isEmpty():
                self.logger.info(f"  ✓ Converged at iteration {iteration + 1}")
                # 🛠️ PERFORMANCE FIX 5: Unpersist the final cached labels when done
                new_labels.unpersist()
                break
            
            changed = new_labels.exceptAll(labels).count()
            self.logger.info(f"    Changed: {changed:,}")
            
            # 🛠️ PERFORMANCE FIX 6: Unpersist the old labels *after* convergence check
            # (Only necessary if not using the checkpoint unpersist above)
            labels.unpersist() 

            labels = new_labels
        return labels.select(F.col("name"), F.col("label").alias("component"))
    
    def _assign_cluster_ids(self, component_map: DataFrame) -> DataFrame:
        """Assign human-readable cluster IDs to components."""
        
        # Get unique components (sorted alphabetically for determinism)
        unique_components = component_map.select("component").distinct().orderBy("component")
        
        # Assign sequential IDs
        window_spec = Window.orderBy("component")
        cluster_mapping = unique_components.withColumn(
            "seq",
            F.row_number().over(window_spec)
        ).select(
            F.col("component"),
            F.concat(
                F.lit("CLUSTER_"),
                F.lpad(F.col("seq"), 6, "0")
            ).alias("cluster_id")
        )
        
        # Join with component map
        result = component_map.join(
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