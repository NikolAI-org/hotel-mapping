from dataclasses import dataclass
from typing import Dict, Optional

@dataclass
class ScoringConfig:
    """Configuration for scoring strategy"""
    
    thresholds: Dict[str, float]
    
    comparators: Dict[str, float]
    
    def validate(self) -> bool:
        """
        Validate scoring config with detailed error reporting
        """
        
        # ════════════════════════════════════════════════════════════════
        # CHECK 4: All thresholds in valid range
        # ════════════════════════════════════════════════════════════════
        
        for threshold_name, threshold_value in self.thresholds.items():
            if not (-1.0 <= threshold_value <= 1.0):
                raise ValueError(
                    f"Threshold '{threshold_name}' outside [-1, 1]: {threshold_value}"
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
    algorithm: str
    
    
    def validate(self) -> bool:
        """Validate clustering config"""
        if not self.algorithm:
            raise ValueError("algorithm must be specified")
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

