# config/config_loader.py

import yaml
from pathlib import Path
from typing import Dict, Any

from hotel_data.config.scoring_config import ConditionConfig, ConditionGroupConfig, HotelClusteringConfig, ScoringConfig

class ConfigLoader:
    """
    Loads and parses configuration from YAML files
    """
    
    @staticmethod
    def load_from_yaml(config_path: str) -> HotelClusteringConfig:
        """
        Load configuration from YAML file
        
        Args:
            config_path: Path to config.yaml file
        
        Returns:
            HotelClusteringConfig instance
        
        Raises:
            FileNotFoundError: If config file not found
            ValueError: If config validation fails
        """
        base_dir = Path(__file__).resolve().parent
        print(f"=========== Base Directory: {base_dir}")
        abs_path = (base_dir / config_path).resolve()
        
        config_file = Path(abs_path)
        
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {abs_path}")
        
        # Load YAML
        with open(config_file, 'r') as f:
            config_dict = yaml.safe_load(f)
            
        
        # Create config object
        config = HotelClusteringConfig.from_dict(config_dict)
        
        # Validate
        config.validate()
        
        return config
    
    @staticmethod
    def load_from_dict(config_dict: Dict[str, Any]) -> HotelClusteringConfig:
        """Load configuration from dictionary"""
        config = HotelClusteringConfig.from_dict(config_dict)
        config.validate()
        return config

