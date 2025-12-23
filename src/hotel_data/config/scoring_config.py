from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class ComparisonOperator(str, Enum):
    """Valid comparison operators"""
    GTE = "gte"
    LTE = "lte"
    GT = "gt"
    LT = "lt"


class LogicalOperator(str, Enum):
    """Valid logical operators"""
    AND = "AND"
    OR = "OR"


@dataclass
class ConditionConfig:
    """Represents a single condition within a condition group"""
    
    threshold: float
    comparator: ComparisonOperator = ComparisonOperator.GTE
    
    def validate(self) -> bool:
        """Validate individual condition"""
        if not (-1.0 <= self.threshold <= 1.0):
            raise ValueError(
                f"Condition threshold outside [-1, 1]: {self.threshold}"
            )
        
        if self.comparator not in ComparisonOperator:
            raise ValueError(
                f"Invalid comparator: {self.comparator}. "
                f"Must be one of: {[op.value for op in ComparisonOperator]}"
            )
        
        return True


@dataclass
class ConditionGroupConfig:
    """
    Represents a group of conditions combined with AND/OR operator.
    NOW SUPPORTS NESTING: can contain either conditions OR nested condition_groups.
    
    Backward compatible: old configs with only 'conditions' still work!
    """
    
    operator: LogicalOperator = LogicalOperator.AND
    conditions: Optional[Dict[str, ConditionConfig]] = field(default_factory=dict)
    condition_groups: Optional[List['ConditionGroupConfig']] = field(default_factory=list)
    
    def validate(self) -> bool:
        """Validate condition group - can have conditions OR nested groups, not both"""
        if self.operator not in LogicalOperator:
            raise ValueError(
                f"Invalid group operator: {self.operator}. "
                f"Must be one of: {[op.value for op in LogicalOperator]}"
            )
        
        has_conditions = self.conditions and len(self.conditions) > 0
        has_nested_groups = self.condition_groups and len(self.condition_groups) > 0
        
        # Must have either conditions OR nested groups (not both, not neither)
        if has_conditions and has_nested_groups:
            raise ValueError(
                "ConditionGroup cannot have BOTH 'conditions' and 'condition_groups'"
            )
        
        if not (has_conditions or has_nested_groups):
            raise ValueError(
                "ConditionGroup must have either 'conditions' or 'condition_groups'"
            )
        
        # Validate conditions if present
        if has_conditions and self.conditions is not None:
            for signal_name, condition_config in self.conditions.items():
                if not condition_config.validate():
                    raise ValueError(
                        f"Invalid condition for signal '{signal_name}'"
                    )
        
        # Recursively validate nested groups if present
        if has_nested_groups and self.condition_groups is not None:
            for i, nested_group in enumerate(self.condition_groups):
                if not nested_group.validate():
                    raise ValueError(
                        f"Nested condition group {i} is invalid"
                    )
        
        return True


@dataclass
class ScoringConfig:
    """Configuration for scoring strategy - now supports nested conditions"""
    
    condition_groups: Optional[List[ConditionGroupConfig]] = field(default_factory=list)
    group_operator: LogicalOperator = LogicalOperator.AND  # How to combine top-level groups
    
    def validate(self) -> bool:
        """Validate scoring config with condition_groups (flat or nested)"""
        
        if not self.condition_groups:
            raise ValueError("condition_groups cannot be empty")
        
        deserialized_groups = []
        
        for i, group_data in enumerate(self.condition_groups):
            if isinstance(group_data, dict):
                try:
                    deserialized_group = self._deserialize_condition_group(group_data)
                    deserialized_groups.append(deserialized_group)
                
                except Exception as e:
                    raise ValueError(
                        f"Failed to deserialize condition_groups[{i}]: {str(e)}"
                    )
            
            elif isinstance(group_data, ConditionGroupConfig):
                if not group_data.validate():
                    raise ValueError(f"Condition group {i} is invalid")
                deserialized_groups.append(group_data)
            
            else:
                raise ValueError(
                    f"condition_groups[{i}] must be dict or ConditionGroupConfig, "
                    f"got {type(group_data)}"
                )
        
        self.condition_groups = deserialized_groups
        
        # Convert group_operator if string
        if isinstance(self.group_operator, str):
            try:
                self.group_operator = LogicalOperator[self.group_operator.upper()]
            except KeyError:
                raise ValueError(
                    f"Invalid group_operator: {self.group_operator}. "
                    f"Must be one of: {[op.value for op in LogicalOperator]}"
                )
        
        return True
    
    @staticmethod
    def _deserialize_condition_group(group_data: dict) -> ConditionGroupConfig:
        """
        Recursively deserialize a condition group from dict.
        Supports both flat (conditions only) and nested (condition_groups) structures.
        """
        operator_str = group_data.get("operator", "AND")
        operator = (
            LogicalOperator[operator_str.upper()]
            if isinstance(operator_str, str)
            else operator_str
        )
        
        conditions = None
        nested_groups = None
        
        # Parse conditions if present
        if "conditions" in group_data and group_data["conditions"]:
            conditions = {}
            conditions_dict = group_data.get("conditions", {})
            
            for signal_name, cond_data in conditions_dict.items():
                if isinstance(cond_data, dict):
                    threshold = cond_data.get("threshold")
                    if threshold is None:
                        raise ValueError(
                            f"Condition for '{signal_name}' missing threshold"
                        )
                    
                    comparator_str = cond_data.get("comparator", "gte")
                    try:
                        comparator = (
                            ComparisonOperator(comparator_str.lower())
                            if isinstance(comparator_str, str)
                            else comparator_str
                        )
                    except ValueError:
                        raise ValueError(
                            f"Invalid comparator '{comparator_str}' for '{signal_name}'. "
                            f"Must be one of: {[op.value for op in ComparisonOperator]}"
                        )
                    
                    conditions[signal_name] = ConditionConfig(
                        threshold=threshold,
                        comparator=comparator
                    )
                else:
                    conditions[signal_name] = cond_data
        
        # Parse nested condition_groups if present (NEW FEATURE)
        if "condition_groups" in group_data and group_data["condition_groups"]:
            nested_groups = []
            for nested_group_data in group_data["condition_groups"]:
                if isinstance(nested_group_data, dict):
                    # Recursively parse nested group
                    nested_groups.append(
                        ScoringConfig._deserialize_condition_group(nested_group_data)
                    )
                elif isinstance(nested_group_data, ConditionGroupConfig):
                    nested_groups.append(nested_group_data)
                else:
                    raise ValueError(
                        f"condition_groups item must be dict or ConditionGroupConfig, "
                        f"got {type(nested_group_data)}"
                    )
        
        group_config = ConditionGroupConfig(
            operator=operator,
            conditions=conditions or {},
            condition_groups=nested_groups or []
        )
        
        if not group_config.validate():
            raise ValueError("Deserialized condition group failed validation")
        
        return group_config



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

