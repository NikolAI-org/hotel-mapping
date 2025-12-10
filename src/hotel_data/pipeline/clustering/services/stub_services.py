# services/stub_services.py

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import lit, col
from hotel_data.pipeline.clustering.core.clustering_interfaces import (
    ConflictDetectionStrategy,
    ClusteringStrategy,
    MetadataRecorder,
    ClusterWriter
)
from typing import Dict, Any

# ═══════════════════════════════════════════════════════════════════════════
# STUB: Conflict Detection
# ═══════════════════════════════════════════════════════════════════════════

class StubConflictDetector(ConflictDetectionStrategy):
    """
    Stub implementation of conflict detector
    
    Current behavior: Reports NO conflicts (pass-through)
    TODO: Replace with real TransitiveConflictDetector
    """
    
    def __init__(self, logger, **kwargs):
        self.logger = logger
        self.conflicts_found = 0
    
    def detect_conflicts(self, scored_pairs_df: DataFrame) -> DataFrame:
        """
        Detect conflicts in scored pairs
        
        STUB: Currently finds no conflicts (all pairs valid)
        """
        self.logger.info("Detecting conflicts (STUB - no conflicts found)")
        
        # Add conflict columns (all False = no conflicts)
        result_df = (scored_pairs_df
            .withColumn("has_conflict", lit(False))
            .withColumn("conflict_reason", lit(""))
            .withColumn("conflicting_pair", lit(None))
        )
        
        self.conflicts_found = 0
        self.logger.info(f"Conflicts detected: {self.conflicts_found}")
        
        return result_df
    
    def resolve_conflicts(self, pairs_with_conflicts_df: DataFrame) -> DataFrame:
        """
        Resolve detected conflicts
        
        STUB: Currently does nothing (all pairs already valid)
        """
        self.logger.info("Resolving conflicts (STUB - nothing to resolve)")
        return pairs_with_conflicts_df


# ═══════════════════════════════════════════════════════════════════════════
# STUB: Clustering Strategy
# ═══════════════════════════════════════════════════════════════════════════

class StubClusterer(ClusteringStrategy):
    """
    Stub implementation of clustering strategy
    
    Current behavior: Each pair becomes a separate cluster
    TODO: Replace with real UnionFindClusteringStrategy
    """
    
    def __init__(self, logger, **kwargs):
        self.logger = logger
        self.clusters_created = 0
    
    def cluster(self, scored_pairs_df: DataFrame) -> DataFrame:
        """
        Create clusters from scored pairs
        
        STUB: Each unique hotel ID gets assigned its own cluster_id
        """
        self.logger.info("Creating clusters (STUB - one cluster per hotel)")
        
        # Get all unique hotel IDs
        hotel_ids = (
            scored_pairs_df
            .select(F.col("id_i").alias("id"))
            .unionByName(
                scored_pairs_df.select(F.col("id_j").alias("id"))
            )
            .distinct()
        )
        
        # Assign each hotel to its own cluster (cluster_id = hotel_id)
        clusters_df = (
            hotel_ids
            .withColumn("cluster_id", col("id"))
            .withColumn("cluster_size", lit(1))
            .withColumn("is_representative", lit(True))
        )
        
        self.clusters_created = clusters_df.count()
        self.logger.info(f"Clusters created: {self.clusters_created}")
        
        return clusters_df


# ═══════════════════════════════════════════════════════════════════════════
# STUB: Metadata Recorder
# ═══════════════════════════════════════════════════════════════════════════

class StubMetadataRecorder(MetadataRecorder):
    """
    Stub implementation of metadata recorder
    
    Current behavior: Collects basic statistics
    TODO: Replace with real ComprehensiveMetadataRecorder
    """
    
    def __init__(self, logger, **kwargs):
        self.logger = logger
        self.metrics = {}
    
    def record_metadata(
        self,
        clusters_df: DataFrame,
        scored_pairs_df: DataFrame,
        conflicts_df: DataFrame
    ) -> Dict[str, Any]:
        """Record metadata about clustering"""
        from datetime import datetime
        
        self.logger.info("Recording metadata (STUB)")
        
        # Collect counts
        total_clusters = clusters_df.count()
        total_pairs = scored_pairs_df.count()
        
        # Count by confidence level
        confidence_counts = (
            scored_pairs_df
            .groupBy("confidence_level")
            .count()
            .collect()
        )
        
        high_conf_pairs = sum(
            row["count"] for row in confidence_counts
            if row["confidence_level"] == "HIGH"
        )
        
        conflicts_detected = (
            conflicts_df
            .filter(col("has_conflict") == True)
            .count()
        )
        
        # Store metrics
        self.metrics = {
            'total_hotels': clusters_df.count(),
            'total_pairs_scored': total_pairs,
            'high_confidence_pairs': high_conf_pairs,
            'medium_confidence_pairs': sum(
                row["count"] for row in confidence_counts
                if row["confidence_level"] == "MEDIUM"
            ),
            'low_confidence_pairs': sum(
                row["count"] for row in confidence_counts
                if row["confidence_level"] == "LOW"
            ),
            'clusters_created': total_clusters,
            'conflicts_detected': conflicts_detected,
            'conflicts_resolved': 0,
            'processing_timestamp': datetime.now().isoformat(),
            'version': 'v1.0',
            'status': 'COMPLETED'
        }
        
        self.logger.info(f"Metadata recorded: {len(self.metrics)} metrics")
        return self.metrics
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get collected metrics"""
        return self.metrics


# ═══════════════════════════════════════════════════════════════════════════
# STUB: Cluster Writer
# ═══════════════════════════════════════════════════════════════════════════

class StubClusterWriter(ClusterWriter):
    """
    Stub implementation of cluster writer
    
    Current behavior: Just logs the write operations
    TODO: Replace with real DeltaLakeWriter (writes to Delta Lake)
    """
    
    def __init__(self, logger, **kwargs):
        self.logger = logger
        self.write_count = 0
    
    def write_clusters(self, clusters_df: DataFrame, path: str | None = None) -> None:
        """Write cluster assignments"""
        self.logger.info(
            "Writing clusters (STUB - no actual write)",
            rows=clusters_df.count(),
            path=path or "default_path"
        )
        self.write_count += 1
    
    def write_scored_pairs(self, scored_pairs_df: DataFrame, path: str | None = None) -> None:
        """Write scored pairs"""
        self.logger.info(
            "Writing scored pairs (STUB - no actual write)",
            rows=scored_pairs_df.count(),
            path=path or "default_path"
        )
        self.write_count += 1
    
    def write_metadata(self, metadata: Dict[str, Any], path: str | None = None) -> None:
        """Write metadata"""
        self.logger.info(
            "Writing metadata (STUB - no actual write)",
            metadata_keys=len(metadata),
            path=path or "default_path"
        )
        self.write_count += 1
