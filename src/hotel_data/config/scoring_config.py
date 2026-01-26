from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Union, Any


from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Union, Any

class ScoringConstants:
    """
    Centralized configuration for scoring rewards and penalties.
    Change values here to affect name_utils.py logic.
    """

    # Reward when both sets are empty (e.g., just stop words) in Jaccard
    BOTH_EMPTY_SCORE = 0.5

    # Score when one side is empty in Jaccard
    ONE_SIDE_EMPTY_SCORE = 0.0

    # Score when reconstructed strings (LCS/Levenshtein) are empty
    RECONSTRUCTION_EMPTY_SCORE = 0.0

    # Penalty applied when numeric tokens mismatch (e.g. "OYO 1" vs "OYO 2")
    NUMERIC_MISMATCH_PENALTY = 0.7

    # --- NEW FIX: SINGLE LETTER BUG CHECK ("V" vs "Shivaji") ---
    # If the cleaned string is tiny (< 3 chars), we require an EXACT match.
    # "V" vs "V" -> Match.
    # "V" vs "Shivaji" -> Mismatch.
    MIN_CHARS_REQUIRED_FOR_MATCHING = 3

    MIN_CHARS_MISSING_SCORE = 0


class ComparisonOperator(str, Enum):
    """Valid comparison operators for signals"""
    GTE = "gte"
    LTE = "lte"
    GT = "gt"
    LT = "lt"

class LogicalOperator(str, Enum):
    """Valid logical operators for groups"""
    AND = "AND"
    OR = "OR"

@dataclass
class RuleConfig:
    """
    Unified configuration node.
    It can represent either:
      1. A LEAF (Signal): Has 'signal', 'threshold', 'comparator'
      2. A NODE (Group):  Has 'operator', 'rules'
    """
    
    # --- Leaf Properties ---
    signal: Optional[str] = None
    threshold: Optional[float] = None
    comparator: ComparisonOperator = ComparisonOperator.GTE
    
    # --- Node Properties ---
    operator: Optional[LogicalOperator] = None
    # We keep Optional here for flexibility, but default is empty list
    rules: Optional[List['RuleConfig']] = field(default_factory=list)

    @property
    def is_group(self) -> bool:
        """True if this is a Logic Group (AND/OR), False if it's a Signal Leaf"""
        return self.operator is not None

    def validate(self) -> bool:
        """Validate rule consistency"""
        
        # CASE 1: Logic Group (AND / OR)
        if self.operator:
            # A group should not have leaf properties
            if self.signal is not None or self.threshold is not None:
                raise ValueError(
                    f"Rule cannot be both a Group (operator={self.operator}) "
                    f"and a Signal ({self.signal})"
                )
            
            # FIX: Safely handle None by defaulting to empty list for iteration
            # This satisfies the type checker and prevents runtime errors
            safe_rules = self.rules or []
            
            if not safe_rules:
                 # Warning: Empty groups technically do nothing, but aren't fatal errors.
                 pass
            
            # Recursively validate children
            for i, child in enumerate(safe_rules):
                try:
                    child.validate()
                except ValueError as e:
                    raise ValueError(
                        f"Invalid child rule at index {i} inside {self.operator} group: {str(e)}"
                    )

        # CASE 2: Signal Leaf
        elif self.signal:
            if self.threshold is None:
                raise ValueError(f"Signal '{self.signal}' is missing 'threshold'")
            
            # Validate threshold range (assuming standard -1.0 to 1.0 scoring)
            if not (-1.0 <= self.threshold <= 1.0):
                # You can comment this out if your scores are not normalized
                pass 
                
            if self.comparator not in ComparisonOperator:
                raise ValueError(f"Invalid comparator for '{self.signal}': {self.comparator}")

        # CASE 3: Invalid Empty Config
        else:
            raise ValueError("RuleConfig must have either 'operator' (Group) or 'signal' (Leaf)")
            
        return True

@dataclass
class ScoringConfig:
    """
    Master Scoring Configuration.
    Holds a single root 'match_logic' which is a RuleConfig tree.
    """
    
    match_logic: RuleConfig
    
    # Legacy fields
    condition_groups: Optional[List[Any]] = field(default_factory=list) 
    group_operator: Optional[LogicalOperator] = LogicalOperator.AND
    
    def __post_init__(self):
        """
        Auto-convert dictionary input to RuleConfig objects.
        This fixes the AttributeError when initializing directly from dicts.
        """
        if isinstance(self.match_logic, dict):
            self.match_logic = self._deserialize_rule(self.match_logic)

    def validate(self) -> bool:
        return self.match_logic.validate()

    @classmethod
    def from_dict(cls, config_dict: Dict) -> "ScoringConfig":
        """
        Parses the dictionary.
        Prioritizes the new 'match_logic' block.
        """
        # 1. Check for new Unified Structure
        if "match_logic" in config_dict:
            root_rule = cls._deserialize_rule(config_dict["match_logic"])
            return cls(match_logic=root_rule)
            
        # 2. Safety Valve for Legacy Configs
        elif "condition_groups" in config_dict:
            raise ValueError(
                "Legacy 'condition_groups' format detected. Please update YAML to use 'match_logic' "
                "with unified nested structure."
            )
        else:
            raise ValueError("ScoringConfig missing 'match_logic' block.")

    @staticmethod
    def _deserialize_rule(data: Dict) -> RuleConfig:
        """Recursively parses a dict into a RuleConfig"""
        
        # A) Is it a Group?
        if "operator" in data:
            op_str = data["operator"].upper()
            try:
                operator = LogicalOperator[op_str]
            except KeyError:
                 raise ValueError(f"Invalid operator: {op_str}")
            
            raw_rules = data.get("rules", [])
            parsed_rules = []
            for r in raw_rules:
                parsed_rules.append(ScoringConfig._deserialize_rule(r))
                
            return RuleConfig(operator=operator, rules=parsed_rules)
            
        # B) Is it a Leaf?
        elif "signal" in data:
            comp_str = data.get("comparator", "gte").lower()
            try:
                comparator = ComparisonOperator(comp_str)
            except ValueError:
                # Handle symbolic comparators if necessary
                symbol_map = {">=": "gte", "<=": "lte", ">": "gt", "<": "lt"}
                if comp_str in symbol_map:
                    comparator = ComparisonOperator(symbol_map[comp_str])
                else:
                    raise ValueError(f"Unknown comparator: {comp_str}")

            return RuleConfig(
                signal=data["signal"],
                threshold=float(data["threshold"]),
                comparator=comparator
            )
            
        else:
            raise ValueError(f"Rule block must contain 'operator' or 'signal'. Got keys: {list(data.keys())}")

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
    transitivity: bool = True
    
    
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
