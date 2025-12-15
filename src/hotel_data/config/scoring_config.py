from dataclasses import dataclass
from typing import Dict, Optional

@dataclass
class ScoringConfig:
    """Configuration for scoring strategy"""
    
    weights: Dict[str, float]
    # Example:
    # {
    #   'geo_distance_km': 0.15,
    #   'name_score_sbert': 0.25,
    #   ...
    # }
    
    thresholds: Dict[str, float]
    # Example:
    # {
    #   'high_confidence': 0.85,
    #   'medium_confidence': 0.70,
    #   'low_confidence': 0.55
    # }
    
    comparators: Dict[str, float]
    
    exclusion_rules: Dict[str, float]
    # Example:
    # {
    #   'max_geo_distance_km': 5.0,
    #   'min_country_match': 1.0,
    #   'min_normalized_name_score': 0.3
    # }
    
    def validate(self) -> bool:
        """
        Validate scoring config with detailed error reporting
        """
        # ════════════════════════════════════════════════════════════════
        # CHECK 1: Weights sum to ~1.0 (with tolerance)
        # ════════════════════════════════════════════════════════════════
        
        total_weight = sum(self.weights.values())
        tolerance = 0.01  # Allow ±1% deviation
        
        if not (1.0 - tolerance <= total_weight <= 1.0 + tolerance):
            raise ValueError(
                f"Weights must sum to 1.0 (±{tolerance}), got {total_weight}. "
                f"Weights: {self.weights}"
            )
        
        # ════════════════════════════════════════════════════════════════
        # CHECK 2: All weights are positive
        # ════════════════════════════════════════════════════════════════
        
        for signal_name, weight in self.weights.items():
            if weight < 0:
                raise ValueError(
                    f"Weight for '{signal_name}' is negative: {weight}"
                )
            if weight > 1.0:
                raise ValueError(
                    f"Weight for '{signal_name}' exceeds 1.0: {weight}"
                )
        
        # ════════════════════════════════════════════════════════════════
        # CHECK 3: Thresholds are in correct order (high > medium > low)
        # ════════════════════════════════════════════════════════════════
        
        high = self.thresholds.get('high_confidence', 0.85)
        medium = self.thresholds.get('medium_confidence', 0.70)
        low = self.thresholds.get('low_confidence', 0.55)
        
        if not (high > medium > low):
            raise ValueError(
                f"Threshold order incorrect: "
                f"high({high}) > medium({medium}) > low({low}). "
                f"Require: high > medium > low"
            )
        
        # ════════════════════════════════════════════════════════════════
        # CHECK 4: All thresholds in valid range
        # ════════════════════════════════════════════════════════════════
        
        for threshold_name, threshold_value in self.thresholds.items():
            if not (0.0 <= threshold_value <= 1.0):
                raise ValueError(
                    f"Threshold '{threshold_name}' outside [0, 1]: {threshold_value}"
                )
        
        # ════════════════════════════════════════════════════════════════
        # CHECK 5: Exclusion rules have valid values
        # ════════════════════════════════════════════════════════════════
        
        if self.exclusion_rules:
            for rule_name, rule_value in self.exclusion_rules.items():
                if rule_value < 0:
                    raise ValueError(
                        f"Exclusion rule '{rule_name}' has negative value: {rule_value}"
                    )
        
        return True

@dataclass
class StorageConfig:
    """Configuration for storage (MinIO, Delta Lake)"""
    
    # MinIO configuration
    minio_endpoint: str          # e.g., "http://minio:9000"
    minio_access_key: str        # e.g., "minioadmin"
    minio_secret_key: str        # e.g., "minioadmin"
    minio_bucket: str            # e.g., "hotel-data"
    
    # Delta Lake configuration
    delta_path: str              # e.g., "s3a://hotel-data/delta"
    
    def validate(self) -> bool:
        """Validate storage config"""
        if not self.minio_endpoint or not self.delta_path:
            raise ValueError("Missing required storage config")
        return True


@dataclass
class ClusteringConfig:
    """Configuration for clustering strategy"""
    
    min_score_threshold: float   # e.g., 0.70 (minimum composite score to cluster)
    max_cluster_size: Optional[int]  # e.g., None (no limit)
    enable_black_hole_prevention: bool  # e.g., True
    black_hole_max_threshold: float     # e.g., 0.95
    score_threshold: float
    confidence_threshold: float
    min_cluster_size: int
    black_hole_min_size: int
    
    
    def validate(self) -> bool:
        """Validate clustering config"""
        if not (0.0 <= self.min_score_threshold <= 1.0):
            raise ValueError("min_score_threshold must be in [0, 1]")
        return True    

@dataclass
class StreamingConfig:
    """Configuration for streaming pipeline"""
    
    enabled: bool                # Whether streaming is enabled
    checkpoint_path: str         # e.g., "s3a://hotel-data/checkpoints"
    trigger_interval: str        # e.g., "5 minutes"
    max_batch_size: int          # e.g., 10000
    
    def validate(self) -> bool:
        """Validate streaming config"""
        if self.enabled and not self.checkpoint_path:
            raise ValueError("checkpoint_path required when streaming enabled")
        return True


@dataclass
class LoggingConfig:
    """Configuration for logging"""
    level: str                 # e.g., "INFO", "DEBUG", "ERROR"
    format: str                # e.g., "%(asctime)s - %(name)s - %(message)s"
    output_file: Optional[str] # e.g., "/var/log/hotel-clustering.log"
    
    def validate(self) -> bool:
        """Validate logging config"""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.level not in valid_levels:
            raise ValueError(f"Invalid log level: {self.level}")
        return True

    @classmethod
    def from_dict(cls, config_dict: Dict) -> "LoggingConfig":
        """Create LoggingConfig from dictionary"""
        return cls(
            level=config_dict["level"],
            format=config_dict["format"],
            output_file=config_dict.get("output_file"),
        )

@dataclass
class DeltaConfig:
    catalog_name: str
    schema_name: str
    base_path: str
    
    def validate(self) -> bool:
        """Validate streaming config"""
        if not self.catalog_name and not self.schema_name and not self.base_path:
            raise ValueError("Catlog name, Schema name and/or base path is not defined for delta lake.")
        return True

@dataclass
class HotelClusteringConfig:
    """
    Master configuration for entire hotel clustering system
    
    Contains all sub-configurations for different components
    """
    
    storage: StorageConfig
    scoring: ScoringConfig
    clustering: ClusteringConfig
    streaming: StreamingConfig
    logging: LoggingConfig
    delta_lake: DeltaConfig
    
    def validate(self) -> bool:
        """Validate entire configuration"""
        self.storage.validate()
        self.scoring.validate()
        self.clustering.validate()
        self.streaming.validate()
        self.logging.validate()
        
        print("✅ HotelClusteringConfig validated successfully")
        return True
    
    @classmethod
    def from_dict(cls, config_dict: Dict) -> "HotelClusteringConfig":
        """
        Create HotelClusteringConfig from dictionary
        
        Used by ConfigLoader when parsing YAML
        """
        return cls(
            storage=StorageConfig(**config_dict["storage"]),
            scoring=ScoringConfig(**config_dict["scoring"]),
            clustering=ClusteringConfig(**config_dict["clustering"]),
            streaming=StreamingConfig(**config_dict["streaming"]),
            logging=LoggingConfig.from_dict(config_dict["logging"]),
            delta_lake=DeltaConfig(**config_dict["delta_lake"]),
        )

