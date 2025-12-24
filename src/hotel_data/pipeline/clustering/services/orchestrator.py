# pipeline/orchestrator.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, Tuple
from hotel_data.infrastructure.core.table_io import TableIO
from hotel_data.pipeline.clustering.core.clustering_interfaces import (
    Logger,
    ScoringStrategy,
    ClusteringStrategy,
    MetadataRecorder,
)

class HotelClusteringOrchestrator:
    """
    Orchestrator for the entire hotel clustering pipeline
    
    Coordinates all services to process hotels and create clusters:
    1. Score pairs (similarity calculation)
    2. Detect conflicts (transitive property violations)
    3. Create clusters (group similar hotels)
    4. Record metadata (track statistics)
    5. Write results (save to storage)
    """
    
    def __init__(
        self,
        scorer: ScoringStrategy,
        # conflict_detector: ConflictDetectionStrategy,
        clusterer: ClusteringStrategy,
        metadata_recorder: MetadataRecorder,
        writer: TableIO,
        logger: Logger
    ):
        """
        Initialize the orchestrator with all services
        
        Args:
            scorer: ScoringStrategy (e.g., CompositeScoringStrategy)
            conflict_detector: ConflictDetectionStrategy
            clusterer: ClusteringStrategy
            metadata_recorder: MetadataRecorder
            writer: ClusterWriter
            logger: Logger
        """
        self.scorer = scorer
        # self.conflict_detector = conflict_detector
        self.clusterer = clusterer
        self.metadata_recorder = metadata_recorder
        self.writer = writer
        self.logger = logger
        
        # Track state
        self._is_initialized = True
        
        self.logger.info(
            "HotelClusteringOrchestrator initialized",
            scorer=type(scorer).__name__,
            # conflict_detector=type(conflict_detector).__name__,
            clusterer=type(clusterer).__name__,
            metadata_recorder=type(metadata_recorder).__name__,
            writer=type(writer).__name__
        )
    
    # ════════════════════════════════════════════════════════════════════════
    # MAIN ENTRY POINTS
    # ════════════════════════════════════════════════════════════════════════
    
    def run_batch(
        self,
        hotels_df: DataFrame,
        pairs_df: DataFrame
    ) -> Dict[str, Any]:
        """
        Run batch clustering pipeline
        
        Processes hotels and pairs through entire pipeline:
        1. Score pairs
        2. Create clusters
        3. Write Results with metadata
        
        Args:
            hotels_df: DataFrame with hotel data
                - id (primary key)
                - name, address, phone, email, etc.
            
            pairs_df: DataFrame with hotel pairs to compare
                - id_i, id_j
                - 8 signal columns (geo_distance_km, name_score_sbert, etc.)
        
        Returns:
            Dict with results:
            {
                'scored_pairs': DataFrame (with composite_score, etc.),
                'conflicts': DataFrame (pairs with conflicts),
                'clusters': DataFrame (cluster assignments),
                'metadata': Dict (statistics),
                'status': 'SUCCESS' or 'FAILED'
            }
        """
        try:
            self.logger.info(
                "Starting batch clustering pipeline",
                hotel_count=hotels_df.count(),
                pair_count=pairs_df.count()
            )
            
            # ═════════════════════════════════════════════════════════════
            # PHASE 1: SCORE PAIRS
            # ═════════════════════════════════════════════════════════════
            
            self.logger.info("PHASE 1: Scoring pairs...")
            scored_pairs_df = self._phase_score(pairs_df)
            self.logger.debug(f"Scored {scored_pairs_df.count()} pairs")
            
            
            # ═════════════════════════════════════════════════════════════
            # PHASE 2: CREATE CLUSTERS
            # ═════════════════════════════════════════════════════════════
            
            # self.logger.info("PHASE 3: Creating clusters...")
            clusters_df = self._phase_cluster(hotels_df, scored_pairs_df)
            self.logger.debug(f"Created {clusters_df.count()} clusters")

            
            # ═════════════════════════════════════════════════════════════
            # PHASE 3: WRITE RESULTS
            # ═════════════════════════════════════════════════════════════
            
            self.logger.info("PHASE 3: Writing results...")
            self._phase_write(scored_pairs_df, clusters_df, None)

            
            # ═════════════════════════════════════════════════════════════
            # SUCCESS
            # ═════════════════════════════════════════════════════════════
            
            # self.logger.info(
            #     "Batch clustering pipeline completed",
            #     status="SUCCESS",
            #     clusters=clusters_df.count(),
            #     conflicts=conflicts_df.filter("has_conflict").count()
            # )
            
            return {
                'scored_pairs': scored_pairs_df,
                # 'conflicts': conflicts_df,
                'clusters': clusters_df,
                # 'metadata': metadata,
                'status': 'SUCCESS'
            }
        
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            return {
                'scored_pairs': None,
                'conflicts': None,
                'clusters': None,
                'metadata': None,
                'status': 'FAILED',
                'error': str(e)
            }
    
    def run_streaming(self, pairs_stream):
        """
        Run streaming clustering pipeline
        
        TODO: Implement streaming mode
        Processes incoming pairs in real-time batches
        """
        raise NotImplementedError("Streaming mode not yet implemented")
    
    # ════════════════════════════════════════════════════════════════════════
    # PIPELINE PHASES (Private Methods)
    # ════════════════════════════════════════════════════════════════════════
    
    def _phase_score(self, pairs_df: DataFrame) -> DataFrame:
        """
        PHASE 1: Score all pairs
        
        Input: pairs_df with 8 signals
        Output: scored_pairs_df with composite_score, confidence_level, etc.
        """
        self.logger.debug(
            "Scoring pairs",
            input_rows=pairs_df.count()
        )
        
        scored_df = self.scorer.score(pairs_df)
        
        score_count = scored_df.count()
        
        self.logger.info(
            "Pairs scored",
            total=score_count
            # high_conf=scored_df.filter("confidence_level = 'HIGH'").count(),
            # medium_conf=scored_df.filter("confidence_level = 'MEDIUM'").count(),
            # low_conf=scored_df.filter("confidence_level = 'LOW'").count(),
            # uncertain=scored_df.filter("confidence_level = 'UNCERTAIN'").count()
        )
        
        return scored_df
    
    # def _phase_detect_conflicts(self, scored_pairs_df: DataFrame) -> DataFrame:
    #     """
    #     PHASE 2: Detect conflicts in scored pairs
        
    #     Input: scored_pairs_df
    #     Output: pairs with conflict flags (has_conflict, conflict_reason)
    #     """
    #     self.logger.debug("Detecting conflicts in pairs")
        
    #     conflicts_df = self.conflict_detector.detect_conflicts(scored_pairs_df)
        
    #     conflicts_count = conflicts_df.filter("has_conflict").count()
        
    #     self.logger.info(
    #         "Conflict detection complete",
    #         conflicts_found=conflicts_count,
    #         valid_pairs=conflicts_df.filter("not has_conflict").count()
    #     )
        
    #     return conflicts_df
    
    def _phase_cluster(self, hotels_df: DataFrame, scored_pairs_df: DataFrame) -> DataFrame:
        """
        PHASE: Create clusters from scored pairs
        
        Input: scored_pairs_df
        Output: clusters_df with cluster assignments (id, cluster_id, is_representative)
        """
        self.logger.debug("Creating clusters")

        # 1. FILTER: Only keep pairs that are flagged as matches
        #    (Assumes you have an 'is_match' or 'match_status' column from the scorer)
        #    Adjust the filter condition based on your exact column name
        if "is_matched" in scored_pairs_df.columns:
            valid_edges_df = scored_pairs_df.filter(F.col("is_matched") == True)
        elif "match_score" in scored_pairs_df.columns:
            # Fallback if binary flag is missing: Use a strict threshold
            valid_edges_df = scored_pairs_df.filter(F.col("match_score") > 0.85)
        else:
            # Safety valve: If we can't filter, we shouldn't run.
            raise ValueError("Cannot filter pairs! Missing 'is_matched' or 'match_score' column.")

        self.logger.info(
            f"Filtering edges for clustering: {scored_pairs_df.count()} total -> {valid_edges_df.count()} matches"
        )

        # 2. CLUSTER: Run connected components on the sparse graph

        clusters_df = self.clusterer.cluster(hotels_df=hotels_df, scored_pairs_df=scored_pairs_df)
        clusters_df = clusters_df.cache()
        
        
        cluster_count = clusters_df.select("cluster_id").distinct().count()
        
        self.logger.info(
            "Clustering complete",
            total_hotels=clusters_df.count(),
            total_clusters=cluster_count,
            avg_cluster_size=clusters_df.count() / max(cluster_count, 1)
        )
        
        return clusters_df
    
    # def _phase_metadata(
    #     self,
    #     clusters_df,  # Actually: scored pairs WITH cluster_id
    #     scored_pairs_df,  # Scored pairs
    #     conflicts_df  # Conflicts
    # ):
    #     """
    #     PHASE 4: Record metadata and statistics
        
    #     With validation to handle missing cluster_id
    #     """
    #     self.logger.debug("Recording metadata...")
    #     metadata = self.metadata_recorder.get_metrics(
    #         clusters_df=clusters_df,
    #         scored_pairs_df=scored_pairs_df,
    #         conflicts_df=conflicts_df
    #     )
    #     return metadata


    
    # def _phase_conflict_resolution(
    #     self,
    #     conflicts_df: DataFrame
    # ) -> DataFrame:
    #     """
    #     PHASE 3: Resolve detected conflicts
        
    #     NOTE: Currently DISABLED - returns non-conflict pairs only
        
    #     Future Implementation:
    #     - Analyze conflicting pairs
    #     - Choose winning pair based on scores
    #     - Merge cluster assignments
    #     - Return resolved pairs
        
    #     Args:
    #         conflicts_df: DataFrame with has_conflict column
        
    #     Returns:
    #         DataFrame with conflicts resolved (currently just non-conflict rows)
    #     """
    #     self.logger.info("Conflict resolution phase (currently disabled)")
        
    #     # For now, just return pairs without conflicts
    #     try:
    #         if "has_conflict" in conflicts_df.columns:
    #             self.conflict_detector.resolve_conflicts(conflicts_df)
    #             resolved_df = conflicts_df.filter("NOT has_conflict")
    #             conflict_count = conflicts_df.filter("has_conflict").count()
    #             self.logger.info(
    #                 f"Skipped {conflict_count} conflict pairs, returning clean pairs"
    #             )
    #             return resolved_df
    #         else:
    #             # No conflict column, return as-is
    #             self.logger.warning("has_conflict column not found in conflicts_df")
    #             return conflicts_df
        
    #     except Exception as e:
    #         self.logger.error(f"Conflict resolution failed: {str(e)}")
    #         raise



    
    def _phase_write(
        self,
        scored_pairs_df,
        clusters_df,
        metadata: Dict[str, Any] | None
    ) -> None:
        """
        PHASE: Write results to storage
        
        Single write point for all outputs:
        - 02_scored_pairs: Scored pairs with composite scores
        - 06_final_clusters: Final clusters with assignments
        - 07_metadata: Pipeline statistics and metadata
        
        All writes use unified TableIO interface
        """
        self.logger.debug("Writing results to storage")
        
        try:
            # 1. CRITICAL FIX: Join Cluster IDs back to the Scored Pairs
            #    We join on 'id_i' (Hotel A) to see which cluster the pair belongs to.
            #    (You could also join on id_j, but usually joining on the anchor 'id_i' is sufficient for analytics)

            # Rename columns in clusters_df to avoid collision before joining
            cluster_info = clusters_df.select(
                F.col("id").alias("cluster_node_id"),
                F.col("cluster_id"),
                #F.col("is_representative")
            )

            # Join: scored_pairs.id_i == clusters.id
            enriched_pairs_df = scored_pairs_df.join(
                cluster_info,
                scored_pairs_df.id_i == cluster_info.cluster_node_id,
                "left"
            )

            # 2. Write the Tables (You seemed to be missing the actual table writes!)
            self.writer.write(enriched_pairs_df, "hotel_pairs_clustered")
            self.writer.write(clusters_df, "hotel_clusters")

            self.metadata_recorder.record_metadata(enriched_pairs_df, clusters_df,metadata=None)
        except Exception as e:
            self.logger.error(f"Write failed: {str(e)}")
            raise


    
    # ════════════════════════════════════════════════════════════════════════
    # UTILITY METHODS
    # ════════════════════════════════════════════════════════════════════════
    
    def get_status(self) -> str:
        """Get orchestrator status"""
        return "INITIALIZED" if self._is_initialized else "NOT_INITIALIZED"
    
    def health_check(self) -> bool:
        """Verify all services are ready"""
        try:
            assert self.scorer is not None, "Scorer not initialized"
            # assert self.conflict_detector is not None, "Conflict detector not initialized"
            assert self.clusterer is not None, "Clusterer not initialized"
            assert self.metadata_recorder is not None, "Metadata recorder not initialized"
            assert self.writer is not None, "Writer not initialized"
            assert self.logger is not None, "Logger not initialized"
            
            self.logger.info("Health check PASSED - all services ready")
            return True
        
        except AssertionError as e:
            self.logger.error(f"Health check FAILED: {str(e)}")
            return False
