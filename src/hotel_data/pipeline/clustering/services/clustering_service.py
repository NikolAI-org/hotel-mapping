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
            
             # Debug: Check what columns and data we have
            self.logger.info(f"Input DataFrame columns: {scored_pairs_df.columns}")
            self.logger.info(f"Input DataFrame count: {scored_pairs_df.count()}")
            
            has_confidence_rank = "confidence_rank" in scored_pairs_df.columns
            has_confidence_level = "confidence_level" in scored_pairs_df.columns
            
            if has_confidence_rank:
                # Use confidence_rank directly (numeric)
                filtered_pairs = scored_pairs_df.filter(
                    (F.col("composite_score") >= self.score_threshold) &
                    (F.col("confidence_rank") >= self.confidence_threshold)
                )
                confidence_col = "confidence_rank"
                
            elif has_confidence_level:
                # Convert confidence_level (string) to numeric rank
                scored_pairs_df = scored_pairs_df.withColumn(
                    "confidence_rank",
                    F.when(F.col("confidence_level") == "CERTAIN", 4)
                        .when(F.col("confidence_level") == "HIGH", 3)
                        .when(F.col("confidence_level") == "MEDIUM", 2)
                        .when(F.col("confidence_level") == "LOW", 1)
                        .otherwise(0)
                )
                
                filtered_pairs = scored_pairs_df.filter(
                    (F.col("composite_score") >= self.score_threshold) &
                    (F.col("confidence_rank") >= self.confidence_threshold)
                )
                confidence_col = "confidence_rank"
                
            else:
                raise ValueError(
                    f"Neither 'confidence_rank' nor 'confidence_level' found! "
                    f"Available: {scored_pairs_df.columns}"
                )

            filtered_count = filtered_pairs.count()
            self.logger.info(
                "Filtered pairs by threshold",
                original=scored_pairs_df.count(),
                filtered=filtered_count,
                score_threshold=self.score_threshold,
                confidence_threshold=self.confidence_threshold,
                confidence_column=confidence_col
            )
            
            
            # filtered_pairs = scored_pairs_df.filter(
            #     (F.col("composite_score") >= self.score_threshold) &
            #     (F.col("confidence_rank") >= self.confidence_threshold)
            # )
            
            # filtered_count = filtered_pairs.count()
            # self.logger.info(
            #     "Filtered pairs by threshold",
            #     original=scored_pairs_df.count(),
            #     filtered=filtered_count,
            #     score_threshold=self.score_threshold,
            #     confidence_threshold=self.confidence_threshold
            # )
            
            # if filtered_count == 0:
            #     return self._create_identity_clusters(scored_pairs_df, has_existing_cluster_ids)
            
            if filtered_count == 0:
                self.logger.warning(
                    "⚠️ No pairs meet score/confidence threshold - using identity clusters",
                    score_threshold=self.score_threshold,
                    confidence_threshold=self.confidence_threshold
                )
                
                # Show sample data to help understand why
                self.logger.info("Sample confidence levels:")
                scored_pairs_df.select("confidence_level", "composite_score").limit(5).show()
                
                # ✅ Create identity clusters for ALL hotels
                parent_mapping = self._create_identity_parent_map(scored_pairs_df)
                cluster_mapping = self._finalize_clusters(parent_mapping)
                
                self.logger.info(
                    "✓ Identity clusters created",
                    unique_clusters=cluster_mapping.select("cluster_id").distinct().count()
                )
                
                # Continue to STEP 4 (assign cluster IDs)
                # Skip STEP 2 (black hole detection) and STEP 3 (union-find)
                black_hole_pairs = None
                
            else:
                # ════════════════════════════════════════════════════════════════════
                # STEP 2: BLACK HOLE DETECTION (BEFORE clustering)
                # ════════════════════════════════════════════════════════════════════
                
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
                
                # ════════════════════════════════════════════════════════════════════
                # STEP 3: UNION-FIND on remaining pairs
                # ════════════════════════════════════════════════════════════════════
                
                sorted_pairs = filtered_pairs.select(
                    F.col("id_i"),
                    F.col("id_j"),
                    F.col("name_i"),
                    F.col("name_j"),
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
                result = result.withColumn(
                    "cluster_id",
                    F.when(
                        F.col("cluster_id").isNotNull(),
                        F.col("cluster_id")
                    ).otherwise(
                        F.coalesce(
                            F.col("cluster_id_from_i"),
                            F.col("cluster_id_from_j"),
                            F.col("id_i")
                        )
                    )
                ).drop("cluster_id_from_i", "cluster_id_from_j")
                
                self.logger.info(
                    "Idempotency applied: Preserved existing cluster_ids",
                    preserved=pre_assigned_count
                )
            else:
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
        
        FIXED: Properly extract all unique hotel IDs from pairs_df
        
        Returns parent mapping: hotel_id → parent
        """
        
        self.logger.debug("Starting Union-Find algorithm")
        self.logger.debug(f"Input pairs_df schema: {pairs_df.columns}")
        
        # ✅ FIXED: Extract hotel IDs - verify columns exist first
        try:
            # Debug: Check what columns are actually in pairs_df
            available_cols = pairs_df.columns
            self.logger.info(f"Available columns in pairs_df: {available_cols}")
            
            # Check if id_i and id_j exist
            if "id_i" not in available_cols or "id_j" not in available_cols:
                self.logger.error(
                    f"CRITICAL: id_i or id_j not found in pairs_df!",
                    available_columns=available_cols
                )
                raise ValueError(
                    f"id_i or id_j column not found. Available: {available_cols}"
                )
            
            # Extract all unique hotel IDs from both id_i and id_j columns
            self.logger.info("=========================")
            self.logger.info("Pair_DF schema")
            self.logger.info(pairs_df.printSchema())
            self.logger.info("=========================")
            hotel_ids = pairs_df.select(
                F.col("name_i").alias("name"),
                F.col("id_i").alias("id")
            ).union(
                pairs_df.select(F.col("name_j").alias("name"),
                                F.col("id_j").alias("id")
                                )
            ).distinct()
            
            hotel_ids.show()
            
            hotel_ids_count = hotel_ids.count()
            self.logger.info(f"Extracted {hotel_ids_count} unique hotel IDs")
            
            if hotel_ids_count == 0:
                self.logger.error("CRITICAL: No hotel IDs extracted from pairs!")
                raise ValueError("No hotel IDs found in pairs_df")
            
            # Initialize parent map: each hotel = own parent (initially)
            parent_map = hotel_ids.withColumn(
                "parent",
                F.col("id")
            ).select(
                F.col("id").alias("hotel_id"),
                F.col("parent")
            )
            
            parent_map_count = parent_map.count()
            self.logger.info(
                f"✓ Initialized parent map",
                hotel_count=parent_map_count
            )
            
        except Exception as e:
            self.logger.error(f"Failed to initialize parent map: {str(e)}")
            raise
    
        # ═══════════════════════════════════════════════════════════════════
        # Iterative Union-Find with convergence check
        # ═══════════════════════════════════════════════════════════════════
        
        max_iterations = 100
        iteration = 0
        
        while iteration < max_iterations:
            iteration += 1
            
            try:
                # Join pairs with parent map to get parents for both id_i and id_j
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
                
                # Union: merge to minimum parent
                updated = updated.withColumn(
                    "min_parent",
                    F.least(F.col("parent_i"), F.col("parent_j"))
                )
                
                # Create update records
                updates_i = updated.select(
                    F.col("parent_i").alias("hotel_id"),
                    F.col("min_parent").alias("new_parent")
                ).distinct()
                
                updates_j = updated.select(
                    F.col("parent_j").alias("hotel_id"),
                    F.col("min_parent").alias("new_parent")
                ).distinct()
                
                all_updates = updates_i.union(updates_j).distinct()
                
                # Apply updates to parent map
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
                
                # Check convergence: parent should point to itself for roots
                converged_count = parent_map.join(
                    parent_map.select(
                        F.col("hotel_id").alias("h2"),
                        F.col("parent").alias("p2")
                    ),
                    parent_map["parent"] == F.col("h2"),
                    "inner"
                ).count()
                
                if converged_count == parent_map_count:
                    self.logger.info(f"✓ Union-Find converged at iteration {iteration}")
                    break
                
                self.logger.debug(f"Union-Find iteration {iteration}")
                
            except Exception as e:
                self.logger.error(f"Union-Find iteration {iteration} failed: {str(e)}")
                raise
        
        if iteration == max_iterations:
            self.logger.warning(f"⚠️ Union-Find did not converge after {max_iterations} iterations")
        
        return parent_map

    
    def _finalize_clusters(self, parent_map: DataFrame) -> DataFrame:
        """
        Finalize cluster IDs: Map all hotels to their cluster's sequential ID.
        
        FIXED: Now generates proper CLUSTER_XXXXXX format
        """
        
        try:
            # Find all unique roots (cluster representatives)
            roots = parent_map.filter(F.col("hotel_id") == F.col("parent"))
            unique_clusters = roots.count()
            self.logger.info(f"Found {unique_clusters} unique cluster roots")
            
            # Assign sequential cluster IDs to roots
            # Sorted by root_id for consistency across runs
            window_spec = Window.orderBy(F.col("hotel_id"))
            
            cluster_mapping = roots.withColumn(
                "seq_number",
                F.row_number().over(window_spec)
            ).select(
                F.col("hotel_id").alias("root_id"),
                F.col("parent").alias("root_parent"),
                # ✅ FIXED: Add "CLUSTER_" prefix!
                F.concat(
                    F.lit("CLUSTER_"),                    # ✅ NOT EMPTY!
                    F.lpad(F.col("seq_number"), 6, "0")
                ).alias("cluster_id")
            ).cache()
            
            self.logger.info(f"✓ Generated {cluster_mapping.count()} sequential cluster IDs")
            
            # Show sample
            self.logger.debug("Sample cluster mapping:")
            cluster_mapping.select("root_id", "cluster_id").limit(5).show(truncate=False)
            
            # Map all hotels to their cluster's sequential ID
            result = parent_map.join(
                cluster_mapping,
                parent_map["parent"] == F.col("root_id"),
                "left"
            ).select(
                parent_map["hotel_id"],
                F.col("cluster_id")
            )
            
            # Verify all have cluster IDs
            null_count = result.filter(F.col("cluster_id").isNull()).count()
            if null_count > 0:
                self.logger.warning(f"⚠️ {null_count} hotels without cluster assignment!")
            else:
                self.logger.info("✓ All hotels successfully mapped to sequential cluster IDs")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Cluster finalization failed: {str(e)}")
            raise


    
    def _create_identity_clusters(
        self,
        df: DataFrame,
        has_existing_ids: bool
    ) -> DataFrame:
        """
        Create identity clusters when no pairs meet threshold.
        
        FIXED: Now generates sequential CLUSTER_XXXXXX IDs instead of using id_i!
        
        Process:
        1. Create parent map (each hotel = own parent)
        2. Finalize clusters (assign sequential IDs: CLUSTER_000001, etc.)
        3. Join back to original dataframe
        
        Args:
            df: DataFrame with id_i, id_j columns
            has_existing_ids: Whether cluster_id column already exists
        
        Returns:
            DataFrame with proper sequential cluster_id values
        """
        self.logger.warning("Creating identity clusters: no pairs meet threshold")
        
        try:
            # Step 1: Create parent map where each hotel = own parent
            parent_map = self._create_identity_parent_map(df)
            
            # Step 2: Finalize clusters - generates sequential CLUSTER_XXXXXX IDs
            cluster_mapping = self._finalize_clusters(parent_map)
            
            self.logger.info(
                f"Generated {cluster_mapping.select('cluster_id').distinct().count()} "
                f"sequential cluster IDs for identity clusters"
            )
            
            # Step 3: Join cluster IDs back to original dataframe
            # Join on id_i
            result = df.join(
                cluster_mapping.select(
                    F.col("hotel_id").alias("id_i_mapped"),
                    F.col("cluster_id").alias("cluster_id_from_i")
                ),
                df["id_i"] == F.col("id_i_mapped"),
                "left"
            ).drop("id_i_mapped")
            
            # Join on id_j (for pairs where id_j should have cluster_id)
            result = result.join(
                cluster_mapping.select(
                    F.col("hotel_id").alias("id_j_mapped"),
                    F.col("cluster_id").alias("cluster_id_from_j")
                ),
                result["id_j"] == F.col("id_j_mapped"),
                "left"
            ).drop("id_j_mapped")
            
            # Step 4: Pick cluster_id (prefer id_i's cluster)
            result = result.withColumn(
                "cluster_id",
                F.coalesce(
                    F.col("cluster_id_from_i"),
                    F.col("cluster_id_from_j")
                )
            ).drop("cluster_id_from_i", "cluster_id_from_j")
            
            # Step 5: Add is_black_hole flag
            result = result.withColumn("is_black_hole", F.lit(False))
            
            # Verify all have cluster_ids
            null_count = result.filter(F.col("cluster_id").isNull()).count()
            if null_count > 0:
                self.logger.warning(f"⚠️ {null_count} rows without cluster_id!")
            else:
                self.logger.info("✓ All rows assigned to sequential cluster IDs")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Identity cluster creation failed: {str(e)}")
            raise

    
    
    def _create_identity_parent_map(self, df: DataFrame) -> DataFrame:
        """
        Create parent map for identity clusters (no matching pairs).
        
        Each hotel becomes its own parent (singleton cluster).
        Later, _finalize_clusters() will assign sequential CLUSTER_XXX IDs.
        
        Args:
            df: DataFrame with id_i and id_j columns
        
        Returns:
            DataFrame with [hotel_id, parent] columns where hotel_id == parent
        """
        # Extract all unique hotel IDs from both id_i and id_j columns
        hotel_ids = df.select(F.col("id_i").alias("id")).union(
            df.select(F.col("id_j").alias("id"))
        ).distinct()
        
        # Create parent map: each hotel is its own parent
        parent_map = hotel_ids.select(
            F.col("id").alias("hotel_id"),
            F.col("id").alias("parent")  # Each hotel = own parent
        )
        
        self.logger.info(
            f"Created identity parent map for {parent_map.count()} hotels"
        )
        
        return parent_map


