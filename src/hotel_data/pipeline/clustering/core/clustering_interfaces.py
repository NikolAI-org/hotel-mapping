from abc import ABC, abstractmethod
from typing import Any, Dict
from pyspark.sql import DataFrame

from hotel_data.config.scoring_config import HotelClusteringConfig

class ScoringStrategy(ABC):
    """Abstract interface for scoring hotel pairs"""
    
    @abstractmethod
    def score(self, pairs_df: DataFrame) -> DataFrame:
        """
        Score hotel pairs
        
        Args:
            pairs_df: DataFrame with columns:
                - id_i, id_j
                - geo_distance_km, name_score_sbert, ...
                - (8 signal columns total)
        
        Returns:
            DataFrame with added columns:
                - composite_score: float [0, 1]
                - confidence_level: str (HIGH|MEDIUM|LOW|UNCERTAIN)
                - individual_scores: map
                - meets_exclusion_rules: bool
                - score_components: struct
                - signal_quality: struct
                - scoring_version: str
                - scoring_timestamp: timestamp
        """
        pass

# ═══════════════════════════════════════════════════════════════════════════
# CONFLICT DETECTION STRATEGY (NEW - Add this)
# ═══════════════════════════════════════════════════════════════════════════

class ConflictDetectionStrategy(ABC):
    """
    Interface for detecting conflicts in clustering
    
    A conflict occurs when:
    - Hotel A matches with Hotel B
    - Hotel B matches with Hotel C
    - But Hotel A does NOT match Hotel C (transitive property violated)
    """
    
    @abstractmethod
    def detect_conflicts(self, scored_pairs_df: DataFrame) -> DataFrame:
        """
        Detect conflicts in scored pairs
        
        Args:
            scored_pairs_df: DataFrame with columns:
                - id_i, id_j
                - composite_score
                - confidence_level
                - meets_exclusion_rules
        
        Returns:
            DataFrame with columns:
                - id_i, id_j
                - has_conflict (boolean)
                - conflict_reason (string)
                - conflicting_pair (struct)
        """
        pass

# ═══════════════════════════════════════════════════════════════════════════
# CLUSTERING STRATEGY (NEW - Add this)
# ═══════════════════════════════════════════════════════════════════════════

class ClusteringStrategy(ABC):
    """Interface for clustering strategies"""
    
    @abstractmethod
    def cluster(self, scored_pairs_df: DataFrame) -> DataFrame:
        """
        Create clusters from scored pairs
        
        Args:
            scored_pairs_df: DataFrame with id_i, id_j, composite_score, etc.
        
        Returns:
            DataFrame with columns:
                - id (hotel id)
                - cluster_id (assigned cluster)
                - cluster_size (number of hotels in cluster)
                - is_representative (boolean - is this the representative?)
        """
        pass


# ═══════════════════════════════════════════════════════════════════════════
# METADATA RECORDER (NEW - Add this)
# ═══════════════════════════════════════════════════════════════════════════

class MetadataRecorder(ABC):
    """Interface for recording clustering metadata"""
    
    @abstractmethod
    def record_metadata(
        self,
        clusters_df: DataFrame,
        scored_pairs_df: DataFrame,
        conflicts_df: DataFrame
    ) -> Dict[str, Any]:
        """
        Record metadata about the clustering process
        
        Returns:
            {
                'total_hotels': int,
                'total_pairs_scored': int,
                'high_confidence_pairs': int,
                'clusters_created': int,
                'conflicts_detected': int,
                'conflicts_resolved': int,
                'processing_timestamp': str,
                'version': str
            }
        """
        pass
    
    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]:
        """Get collected metrics"""
        pass


# ═══════════════════════════════════════════════════════════════════════════
# CLUSTER WRITER (NEW - Add this)
# ═══════════════════════════════════════════════════════════════════════════

class ClusterWriter(ABC):
    """Interface for writing clustering results"""
    
    @abstractmethod
    def write_clusters(self, clusters_df: DataFrame, path: str | None = None) -> None:
        """Write cluster assignments to storage"""
        pass
    
    @abstractmethod
    def write_scored_pairs(self, scored_pairs_df: DataFrame, path: str | None = None) -> None:
        """Write scored pairs to storage"""
        pass
    
    @abstractmethod
    def write_metadata(self, metadata: Dict[str, Any], path: str | None = None) -> None:
        """Write metadata to storage"""
        pass

# ═══════════════════════════════════════════════════════════════════════════
# LOGGING INTERFACE (Already exists - shown for reference)
# ═══════════════════════════════════════════════════════════════════════════


class Logger(ABC):
    """
    Abstract Logger interface
    
    All loggers must implement this interface
    """
    
    @abstractmethod
    def log(self, level: str, message: str, **kwargs) -> None:
        """
        Log a message at specified level
        
        Args:
            level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            message: Message to log
            **kwargs: Additional context (will be logged as JSON)
        """
        pass
    
    @abstractmethod
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message"""
        pass
    
    @abstractmethod
    def info(self, message: str, **kwargs) -> None:
        """Log info message"""
        pass
    
    @abstractmethod
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message"""
        pass
    
    @abstractmethod
    def error(self, message: str, **kwargs) -> None:
        """Log error message"""
        pass
    
    @abstractmethod
    def critical(self, message: str, **kwargs) -> None:
        """Log critical message"""
        pass
    

class ConfigProvider(ABC):
    """Interface for configuration providers"""
    
    @abstractmethod
    def get_config(self) -> HotelClusteringConfig:
        """Get configuration"""
        pass
    