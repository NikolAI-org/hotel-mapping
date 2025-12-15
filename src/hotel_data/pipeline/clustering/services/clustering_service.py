from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
import logging

from hotel_data.pipeline.clustering.core.clustering_interfaces import ClusteringStrategy, Logger

class UnionFindHotelClustering(ClusteringStrategy):
    """
    Union-Find clustering using hotel NAMES instead of IDs.
    
    Clusters hotels based on name matches in paired_hotels data.
    """
    
    def __init__(self, spark: SparkSession, logger: Logger):
        self.spark = spark
        self.logger = logger
    
    def cluster(
        self,
        hotels_df: DataFrame,
        scored_pairs_df: DataFrame
    ) -> DataFrame:
        """
        Cluster hotels using name matching (not ID matching).
        
        Args:
            hotels_df: Hotel data with 'name' column
            paired_hotels_df: Pairs with 'name_i', 'name_j', 'is_matched'
        
        Returns:
            DataFrame with cluster_id assigned based on name
        """
        
        try:
            self.logger.info("Starting Union-Find Hotel Clustering by NAME")
            
            # ═══════════════════════════════════════════════════════════
            # STEP 1: Filter and Extract Name Edges
            # ═══════════════════════════════════════════════════════════
            
            matched_pairs = scored_pairs_df.filter(
                F.col("is_matched") == True
            )
            
            if matched_pairs.count() == 0:
                self.logger.warning("No matched pairs - creating singleton clusters")
                return self._create_identity_clusters_by_name(hotels_df)
            
            # Extract name_i and name_j as edges
            edges = matched_pairs.select(
                F.col("name_i"),
                F.col("name_j"),
                F.col("scoring_metadata")
            ).distinct()
            
            matched_count = edges.count()
            self.logger.info(f"Found {matched_count} unique name-based pairs")
            
            # ═══════════════════════════════════════════════════════════
            # STEP 2: Initialize Parent Map with Names
            # ═══════════════════════════════════════════════════════════
            
            # Extract unique names from both columns
            unique_names = edges.select(F.col("name_i").alias("name")).union(
                edges.select(F.col("name_j").alias("name"))
            ).distinct()
            
            total_names = unique_names.count()
            self.logger.info(f"Total unique hotel names in pairs: {total_names}")
            
            # Each name is initially its own parent (root)
            parent_map = unique_names.select(
                F.col("name").alias("hotel_name"),
                F.col("name").alias("parent")
            )
            
            parent_map.cache()
            
            # ═══════════════════════════════════════════════════════════
            # STEP 3: Union-Find Algorithm (Name-based)
            # ═══════════════════════════════════════════════════════════
            
            parent_map = self._union_find(edges, parent_map)
            
            # ═══════════════════════════════════════════════════════════
            # STEP 4: Assign Cluster IDs
            # ═══════════════════════════════════════════════════════════
            
            name_clusters = self._assign_cluster_ids(parent_map)
            
            # ═══════════════════════════════════════════════════════════
            # STEP 5: Join Back to Hotels
            # ═══════════════════════════════════════════════════════════
            
            result = hotels_df.join(
                name_clusters,
                hotels_df["name"] == name_clusters["hotel_name"],
                "left"
            )
            
            # Join with scoring_metadata aggregated from matched pairs
            scoring_metadata = matched_pairs.select(
                F.col("name_i"),
                F.col("name_j"),
                F.col("scoring_metadata")
            ).distinct()
            
            result = result.join(
                scoring_metadata,
                (result["name"] == scoring_metadata["name_i"]) | (result["name"] == scoring_metadata["name_j"]),
                "left"
            ).drop("name_i", "name_j")
            
            # Assign singleton clusters to hotels not in pairs
            result = result.withColumn(
                "cluster_id",
                F.when(
                    F.col("cluster_id").isNull(),
                    F.concat(F.lit("SINGLETON_"), F.col("name"))
                ).otherwise(F.col("cluster_id"))
            ).drop("hotel_name")
            
            unique_clusters = result.select("cluster_id").distinct().count()
            self.logger.info(f"✓ Clustering complete: {unique_clusters} clusters")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Clustering failed: {str(e)}")
            raise
    
    def _union_find(self, edges: DataFrame, parent_map: DataFrame) -> DataFrame:
        """
        Union-Find algorithm operating on hotel NAMES.
        """
        
        max_iterations = 50
        iteration = 0
        prev_root_count = 0
        
        while iteration < max_iterations:
            iteration += 1
            
            # Lookup parent for name_i
            updated = edges.join(
                parent_map.select(F.col("hotel_name"), F.col("parent").alias("parent_i")),
                edges["name_i"] == parent_map["hotel_name"],
                "left"
            ).select(F.col("name_i"), F.col("name_j"), F.col("parent_i"))
            
            # Lookup parent for name_j
            updated = updated.join(
                parent_map.select(F.col("hotel_name"), F.col("parent").alias("parent_j")),
                updated["name_j"] == parent_map["hotel_name"],
                "left"
            ).select(F.col("name_i"), F.col("name_j"), F.col("parent_i"), F.col("parent_j"))
            
            # Union: point to minimum parent
            updated = updated.withColumn(
                "min_parent",
                F.least(F.col("parent_i"), F.col("parent_j"))
            )
            
            # Collect updates
            updates_i = updated.select(
                F.col("parent_i").alias("hotel_name"),
                F.col("min_parent").alias("new_parent")
            ).distinct()
            
            updates_j = updated.select(
                F.col("parent_j").alias("hotel_name"),
                F.col("min_parent").alias("new_parent")
            ).distinct()
            
            all_updates = updates_i.union(updates_j).distinct()
            
            # Apply updates
            parent_map = parent_map.join(
                all_updates,
                parent_map["hotel_name"] == all_updates["hotel_name"],
                "left"
            ).select(
                parent_map["hotel_name"],
                F.when(
                    all_updates["new_parent"].isNotNull(),
                    F.least(parent_map["parent"], all_updates["new_parent"])
                ).otherwise(parent_map["parent"]).alias("parent")
            )
            
            # Check convergence
            roots = parent_map.filter(
                F.col("hotel_name") == F.col("parent")
            ).count()
            
            self.logger.info(f"Union-Find Iteration {iteration}: {roots} roots")
            
            if roots == prev_root_count:
                self.logger.info(f"✓ Converged at iteration {iteration}")
                break
            
            prev_root_count = roots
        
        if iteration == max_iterations:
            self.logger.warning(f"⚠ Did not converge after {max_iterations} iterations")
        
        return parent_map
    
    def _assign_cluster_ids(self, parent_map: DataFrame) -> DataFrame:
        """
        Assign sequential CLUSTER_XXXXXX IDs based on name roots.
        """
        
        roots = parent_map.filter(
            F.col("hotel_name") == F.col("parent")
        )
        
        root_count = roots.count()
        self.logger.info(f"Found {root_count} cluster roots")
        
        # Assign sequential IDs
        window_spec = Window.orderBy(F.col("hotel_name"))
        
        cluster_ids = roots.withColumn(
            "cluster_seq",
            F.row_number().over(window_spec)
        ).select(
            F.col("hotel_name").alias("root_name"),
            F.concat(
                F.lit("CLUSTER_"),
                F.lpad(F.col("cluster_seq"), 6, "0")
            ).alias("cluster_id")
        )
        
        # Map all names to cluster ID
        result = parent_map.join(
            cluster_ids,
            parent_map["parent"] == F.col("root_name"),
            "left"
        ).select(
            parent_map["hotel_name"],
            F.col("cluster_id")
        )
        
        return result
    
    def _create_identity_clusters_by_name(self, hotels_df: DataFrame) -> DataFrame:
        """
        Create singleton clusters when no pairs match.
        """
        
        self.logger.warning("Creating singleton clusters for each unique name")
        
        unique_names = hotels_df.select(F.col("name")).distinct()
        
        window_spec = Window.orderBy(F.col("name"))
        
        clusters = unique_names.withColumn(
            "cluster_seq",
            F.row_number().over(window_spec)
        ).select(
            F.col("name"),
            F.concat(
                F.lit("CLUSTER_"),
                F.lpad(F.col("cluster_seq"), 6, "0")
            ).alias("cluster_id")
        )
        
        return hotels_df.join(clusters, on="name", how="left")