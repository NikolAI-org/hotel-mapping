# infrastructure/logging/logger.py

import logging
import json
from datetime import datetime
from hotel_data.pipeline.clustering.core.clustering_interfaces import Logger

class ConsoleLogger(Logger):
    """
    Concrete Logger implementation that outputs to console (stdout)
    
    Uses Python's built-in logging module
    """
    
    LEVEL_MAP = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL
    }
    
    def __init__(self, name: str = "HotelClustering", level: str = "INFO"):
        """
        Initialize ConsoleLogger
        
        Args:
            name: Logger name
            level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self.LEVEL_MAP.get(level, logging.INFO))
        
        # Create console handler
        handler = logging.StreamHandler()
        handler.setLevel(self.LEVEL_MAP.get(level, logging.INFO))
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        # Add handler to logger
        if not self.logger.handlers:  # Avoid adding multiple handlers
            self.logger.addHandler(handler)
    
    def log(self, level: str, message: str, **kwargs) -> None:
        """
        Log message at specified level
        
        Args:
            level: Logging level string
            message: Message to log
            **kwargs: Additional context (logged as JSON)
        """
        log_level = self.LEVEL_MAP.get(level.upper(), logging.INFO)
        
        # Build full message with context
        if kwargs:
            context_str = json.dumps(kwargs, default=str)
            full_message = f"{message} | Context: {context_str}"
        else:
            full_message = message
        
        self.logger.log(log_level, full_message)
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message"""
        self.log("DEBUG", message, **kwargs)
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message"""
        self.log("INFO", message, **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message"""
        self.log("WARNING", message, **kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message"""
        self.log("ERROR", message, **kwargs)
    
    def critical(self, message: str, **kwargs) -> None:
        """Log critical message"""
        self.log("CRITICAL", message, **kwargs)


# Usage examples:
# ───────────────

logger = ConsoleLogger(level="INFO")

# Simple logging
logger.info("Starting pipeline")

# Logging with context
logger.info(
    "Scoring completed",
    pairs_count=1000,
    average_score=0.75,
    high_confidence_count=750
)

# Error logging
logger.error(
    "Validation failed",
    error_code="VALIDATION_001",
    invalid_rows=5,
    total_rows=1000
)
