from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from typing import Optional, Dict, Any, Tuple, Set
from abc import ABC, abstractmethod
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType

from hotel_data.config.scoring_config import ClusteringConfig
from hotel_data.pipeline.clustering.core.clustering_interfaces import ClusteringStrategy
from hotel_data.pipeline.utils.type_converters import to_bool, to_float, to_int


class UnionFindClusteringStrategy(ClusteringStrategy):
    """
    Enhanced production-grade Union-Find clustering with advanced features.
    
    FEATURES:
    ✅ Black Hole Detection: Prevents isolated single-hotel "clusters"
    ✅ Idempotent Cluster IDs: Respects pre-assigned cluster_ids
    ✅ Union-Find Algorithm: Distributed connected component detection
    ✅ Path Compression: Efficient parent tracking
    ✅ Configuration-driven: Fully customizable thresholds
    
    ALGORITHM:
    1. Check for pre-assigned cluster_ids (idempotency)
    2. Filter pairs by score & confidence thresholds
    3. Initialize parent map (each hotel = own parent)
    4. Union-Find: merge connected components
    5. Detect black holes (single hotels, high-similarity pairs)
    6. Assign final cluster IDs
    7. Validate output
    """
    
    def __init__(
        self,
        config: ClusteringConfig,
        logger
    ):
        """
        Initialize enhanced clustering strategy.
        
        Args:
            config: Configuration dict with:
                - score_threshold: Min composite_score (default: 0.65)
                - confidence_threshold: Min confidence_level (default: 0.70)
                - min_cluster_size: Min hotels per cluster (default: 1)
                - enable_black_hole_prevention: Enable BH detection (default: True)
                - black_hole_max_threshold: Max score for BH pair (default: 0.95)
                - black_hole_min_size: Min pair size to form cluster (default: 2)
            logger: Logger instance
        """
        self.config = config
        self.logger = logger
        
        # Core thresholds
        self.score_threshold = to_float(config.score_threshold, 0.65)
        self.confidence_threshold = to_int(config.confidence_threshold, 2)
        self.min_cluster_size = to_int(config.min_cluster_size, 1)
        
        # Black hole detection
        self.enable_black_hole_prevention = to_bool(config.enable_black_hole_prevention, True)
        self.black_hole_max_threshold = to_float(config.black_hole_max_threshold, 0.95)
        self.black_hole_min_size = to_int(config.black_hole_min_size, 2)
        
        self.logger.info(
            f"EnhancedUnionFindClusteringStrategy initialized",
            score_threshold=self.score_threshold,
            confidence_threshold=self.confidence_threshold,
            enable_black_hole_prevention=self.enable_black_hole_prevention,
            black_hole_max_threshold=self.black_hole_max_threshold
        )
    
    def cluster(self, scored_pairs_df: DataFrame) -> DataFrame:
        """
        Enhanced clustering with black hole detection and idempotent IDs.
        
        INPUT Requirements:
        - id_i, id_j: Hotel identifiers
        - composite_score: Similarity (0-1)
        - confidence_level: Confidence (0-1)
        - cluster_id (OPTIONAL): Pre-assigned cluster IDs to preserve
        
        OUTPUT:
        - All input columns preserved
        - cluster_id: Assigned/preserved cluster IDs
        - is_black_hole: Flag for BH pairs
        
        Args:
            scored_pairs_df: DataFrame with scored pairs
            
        Returns:
            DataFrame with cluster_id and is_black_hole columns
        """
        try:
            self.logger.info(
                "Starting enhanced clustering",
                input_pairs=scored_pairs_df.count(),
                has_cluster_id="cluster_id" in scored_pairs_df.columns
            )
            
            # ═══════════════════════════════════════════════════════════════════
            # STEP 0: IDEMPOTENCY CHECK - Preserve existing cluster_ids
            # ═══════════════════════════════════════════════════════════════════
            
            has_existing_cluster_ids = "cluster_id" in scored_pairs_df.columns
            pre_assigned_count = 0
            
            if has_existing_cluster_ids:
                pre_assigned_count = int(
                    scored_pairs_df.filter(F.col("cluster_id").isNotNull()).count()
                )
                self.logger.info(
                    "Idempotency: Found pre-assigned cluster IDs",
                    pre_assigned_count=pre_assigned_count,
                    total_pairs=scored_pairs_df.count()
                )
            
            # ═══════════════════════════════════════════════════════════════════
            # STEP 1: Filter pairs by thresholds
            # ═══════════════════════════════════════════════════════════════════
            
            filtered_pairs = scored_pairs_df.filter(
                (F.col("composite_score") >= self.score_threshold) &
                (F.col("confidence_rank") >= self.confidence_threshold)
            )
            
            filtered_count = filtered_pairs.count()
            self.logger.info(
                "Filtered pairs by threshold",
                original=scored_pairs_df.count(),
                filtered=filtered_count,
                score_threshold=self.score_threshold,
                confidence_threshold=self.confidence_threshold
            )
            
            if filtered_count == 0:
                return self._create_identity_clusters(scored_pairs_df, has_existing_cluster_ids)
            
            # ═══════════════════════════════════════════════════════════════════
            # STEP 2: BLACK HOLE DETECTION (BEFORE clustering)
            # ═══════════════════════════════════════════════════════════════════
            
            black_hole_pairs = None
            if self.enable_black_hole_prevention:
                black_hole_pairs = self._detect_black_holes(
                    filtered_pairs,
                    self.black_hole_max_threshold
                )
                
                if black_hole_pairs is not None:
                    bh_count = black_hole_pairs.count()
                    self.logger.info(
                        "Black hole pairs detected",
                        black_hole_pairs=bh_count,
                        max_threshold=self.black_hole_max_threshold
                    )
                    
                    # Remove black holes from clustering
                    filtered_pairs = filtered_pairs.join(
                        black_hole_pairs.select("bh_id_i", "bh_id_j"),
                        (filtered_pairs["id_i"] == F.col("bh_id_i")) &
                        (filtered_pairs["id_j"] == F.col("bh_id_j")),
                        "left_anti"
                    )
                    
                    self.logger.info(
                        "Removed black holes from clustering",
                        remaining_pairs=filtered_pairs.count()
                    )
            
            # ═══════════════════════════════════════════════════════════════════
            # STEP 3: UNION-FIND on remaining pairs
            # ═══════════════════════════════════════════════════════════════════
            
            self.logger.info("filtered_pairs Schema")
            self.logger.info(filtered_pairs.printSchema)
            sorted_pairs = filtered_pairs.select(
                F.col("id_i"),
                F.col("id_j"),
                F.col("composite_score"),
                F.col("confidence_level")
            ).orderBy(F.col("composite_score").desc())
            
            parent_mapping = self._union_find_spark(sorted_pairs)
            cluster_mapping = self._finalize_clusters(parent_mapping)
            
            self.logger.info(
                "Union-Find complete",
                unique_clusters=cluster_mapping.select("cluster_id").distinct().count()
            )
            
            # ═══════════════════════════════════════════════════════════════════
            # STEP 4: ASSIGN CLUSTER IDS to all pairs
            # ═══════════════════════════════════════════════════════════════════
            
            # Start with original DataFrame
            result = scored_pairs_df
            
            # For id_i
            result = result.join(
                cluster_mapping.select(
                    F.col("hotel_id").alias("id_i_mapped"),
                    F.col("cluster_id").alias("cluster_id_from_i")
                ),
                result["id_i"] == F.col("id_i_mapped"),
                "left"
            ).drop("id_i_mapped")
            
            # For id_j
            result = result.join(
                cluster_mapping.select(
                    F.col("hotel_id").alias("id_j_mapped"),
                    F.col("cluster_id").alias("cluster_id_from_j")
                ),
                result["id_j"] == F.col("id_j_mapped"),
                "left"
            ).drop("id_j_mapped")
            
            # ═══════════════════════════════════════════════════════════════════
            # STEP 5: IDEMPOTENCY - Preserve pre-assigned cluster_ids
            # ═══════════════════════════════════════════════════════════════════
            
            if has_existing_cluster_ids:
                # Use existing cluster_id if not null, otherwise use new assignment
                result = result.withColumn(
                    "cluster_id",
                    F.when(
                        F.col("cluster_id").isNotNull(),
                        F.col("cluster_id")  # Keep existing
                    ).otherwise(
                        F.coalesce(
                            F.col("cluster_id_from_i"),
                            F.col("cluster_id_from_j"),
                            F.col("id_i")  # Fallback
                        )
                    )
                ).drop("cluster_id_from_i", "cluster_id_from_j")
                
                self.logger.info(
                    "Idempotency applied: Preserved existing cluster_ids",
                    preserved=pre_assigned_count
                )
            else:
                # No pre-existing IDs, use new assignments
                result = result.withColumn(
                    "cluster_id",
                    F.coalesce(
                        F.col("cluster_id_from_i"),
                        F.col("cluster_id_from_j"),
                        F.col("id_i")
                    )
                ).drop("cluster_id_from_i", "cluster_id_from_j")
            
            # ═══════════════════════════════════════════════════════════════════
            # STEP 6: MARK BLACK HOLES (if detected)
            # ═══════════════════════════════════════════════════════════════════
            
            if black_hole_pairs is not None and self.enable_black_hole_prevention:
                result = result.withColumn("is_black_hole", F.lit(False))
                
                result = result.join(
                    black_hole_pairs.select(
                        F.col("bh_id_i").alias("bh_i"),
                        F.col("bh_id_j").alias("bh_j")
                    ),
                    (result["id_i"] == F.col("bh_i")) &
                    (result["id_j"] == F.col("bh_j")),
                    "left"
                )
                
                result = result.withColumn(
                    "is_black_hole",
                    F.when(F.col("bh_i").isNotNull(), F.lit(True)).otherwise(F.lit(False))
                ).drop("bh_i", "bh_j")
                
                self.logger.info(
                    "Black hole markers added",
                    black_hole_pairs=result.filter(F.col("is_black_hole")).count()
                )
            else:
                result = result.withColumn("is_black_hole", F.lit(False))
            
            # ═══════════════════════════════════════════════════════════════════
            # STEP 7: VALIDATION
            # ═══════════════════════════════════════════════════════════════════
            
            if "cluster_id" not in result.columns:
                raise ValueError("CRITICAL: cluster_id column not added!")
            
            null_cluster_count = result.filter(F.col("cluster_id").isNull()).count()
            if null_cluster_count > 0:
                self.logger.warning(
                    f"Found {null_cluster_count} pairs with null cluster_id, fixing..."
                )
                result = result.withColumn(
                    "cluster_id",
                    F.when(
                        F.col("cluster_id").isNull(),
                        F.col("id_i")
                    ).otherwise(F.col("cluster_id"))
                )
            
            # ═══════════════════════════════════════════════════════════════════
            # STEP 8: STATISTICS
            # ═══════════════════════════════════════════════════════════════════
            
            final_count = result.count()
            unique_clusters = result.select("cluster_id").distinct().count()
            black_hole_count = result.filter(F.col("is_black_hole")).count()
            
            self.logger.info(
                "Clustering complete",
                output_pairs=final_count,
                unique_clusters=unique_clusters,
                black_hole_pairs=black_hole_count,
                avg_cluster_size=round(final_count / max(unique_clusters, 1), 2),
                idempotency_preserved=pre_assigned_count if has_existing_cluster_ids else 0
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Enhanced clustering failed: {str(e)}")
            raise
    
    def _detect_black_holes(self, pairs_df: DataFrame, max_threshold: float) -> Optional[DataFrame]:
        """
        Detect black hole pairs: very similar pairs that might indicate 
        the same hotel listed multiple times with high confidence.
        
        Black holes are pairs where:
        - composite_score >= max_threshold (very high similarity)
        - Both id_i and id_j have few other matches
        
        These shouldn't cluster with other hotels.
        """
        try:
            self.logger.debug("Detecting black holes...")
            
            # Find pairs with very high scores
            very_high_score_pairs = pairs_df.filter(
                F.col("composite_score") >= max_threshold
            )
            
            if very_high_score_pairs.count() == 0:
                return None
            
            # Count how many pairs each hotel appears in
            id_i_counts = very_high_score_pairs.groupBy("id_i").agg(
                F.count("*").alias("i_pair_count")
            )
            
            id_j_counts = very_high_score_pairs.groupBy("id_j").agg(
                F.count("*").alias("j_pair_count")
            )
            
            # Join back to find isolated high-score pairs
            black_holes = very_high_score_pairs.join(
                id_i_counts,
                very_high_score_pairs["id_i"] == id_i_counts["id_i"],
                "left"
            ).join(
                id_j_counts,
                very_high_score_pairs["id_j"] == id_j_counts["id_j"],
                "left"
            )
            
            # Filter to black holes: each hotel appears in 1 or 2 matches
            black_holes = black_holes.filter(
                (F.col("i_pair_count") <= 2) & (F.col("j_pair_count") <= 2)
            ).select(
                F.col("id_i").alias("bh_id_i"),
                F.col("id_j").alias("bh_id_j"),
                F.col("composite_score").alias("bh_score")
            )
            
            return black_holes if black_holes.count() > 0 else None
            
        except Exception as e:
            self.logger.warning(f"Black hole detection failed: {str(e)}, skipping")
            return None
    
    def _union_find_spark(self, pairs_df: DataFrame) -> DataFrame:
        """
        Perform distributed Union-Find algorithm.
        
        Returns parent mapping: hotel_id → parent
        """
        spark = pairs_df.sparkSession
        
        # Initialize
        hotel_ids = pairs_df.select(F.col("id_i").alias("id")).union(
            pairs_df.select(F.col("id_j").alias("id"))
        ).distinct()
        
        parent_map = hotel_ids.withColumn(
            "parent",
            F.col("id")
        ).select(F.col("id").alias("hotel_id"), F.col("parent"))
        
        self.logger.info(f"Initialized parent map with {parent_map.count()} hotels")
        
        # Iterative union-find
        max_iterations = 100
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            
            # Join pairs with parent map
            updated = pairs_df.join(
                parent_map,
                pairs_df["id_i"] == parent_map["hotel_id"],
                "left"
            ).select(
                pairs_df["id_i"],
                pairs_df["id_j"],
                F.col("parent").alias("parent_i")
            )
            
            updated = updated.join(
                parent_map,
                updated["id_j"] == parent_map["hotel_id"],
                "left"
            ).select(
                F.col("id_i"),
                F.col("id_j"),
                F.col("parent_i"),
                F.col("parent").alias("parent_j")
            )
            
            # Union: use minimum
            updated = updated.withColumn(
                "min_parent",
                F.least(F.col("parent_i"), F.col("parent_j"))
            )
            
            # Create updates
            updates_i = updated.select(
                F.col("parent_i").alias("hotel_id"),
                F.col("min_parent").alias("new_parent")
            ).distinct()
            
            updates_j = updated.select(
                F.col("parent_j").alias("hotel_id"),
                F.col("min_parent").alias("new_parent")
            ).distinct()
            
            all_updates = updates_i.union(updates_j).distinct()
            
            # Apply updates
            parent_map = parent_map.join(
                all_updates,
                parent_map["hotel_id"] == all_updates["hotel_id"],
                "left"
            ).select(
                parent_map["hotel_id"],
                F.when(
                    all_updates["new_parent"].isNotNull(),
                    F.least(parent_map["parent"], all_updates["new_parent"])
                ).otherwise(parent_map["parent"]).alias("parent")
            )
            
            # Check convergence
            converged_count = parent_map.join(
                parent_map.select(
                    F.col("hotel_id").alias("h2"),
                    F.col("parent").alias("p2")
                ),
                parent_map["parent"] == F.col("h2"),
                "inner"
            ).count()
            
            if converged_count == parent_map.count():
                self.logger.info(f"Union-Find converged at iteration {iteration}")
                break
            
            self.logger.debug(f"Union-Find iteration {iteration}")
        
        return parent_map
    
    def _finalize_clusters(self, parent_map: DataFrame) -> DataFrame:
        """
        Finalize cluster IDs using root parent.
        """
        roots = parent_map.filter(F.col("hotel_id") == F.col("parent"))
        
        result = parent_map.join(
            roots.select(
                F.col("hotel_id").alias("root_id"),
                F.col("parent").alias("root_parent")
            ),
            parent_map["parent"] == F.col("root_id"),
            "left"
        ).select(
            parent_map["hotel_id"],
            F.coalesce(F.col("root_parent"), parent_map["parent"]).alias("cluster_id")
        )
        
        return result
    
    def _create_identity_clusters(
        self,
        df: DataFrame,
        has_existing_ids: bool
    ) -> DataFrame:
        """
        Create identity clusters (no pairs meet threshold).
        """
        self.logger.warning("Creating identity clusters: no pairs meet threshold")
        self.logger.info(df.printSchema)
        if has_existing_ids:
            # Preserve existing, use id_i as fallback
            return df.withColumn(
                "cluster_id",
                F.when(
                    F.col("cluster_id").isNotNull(),
                    F.col("cluster_id")
                ).otherwise(F.col("id_i"))
            ).withColumn("is_black_hole", F.lit(False))
        else:
            return df.withColumn(
                "cluster_id",
                F.col("id_i")
            ).withColumn("is_black_hole", F.lit(False))

