from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Dict, Any, Optional, List


class TableIO(ABC):
    """
    Base interface for all output writers
    
    Abstracts away Delta Lake implementation details
    Allows decorators to add pipeline-specific behavior
    """
    
    @abstractmethod
    def write(
        self,
        df: DataFrame,
        location: str,
        metadata: Dict[str, Any] | None = None
    ) -> Dict[str, Any]:
        """
        Write DataFrame to output
        
        Args:
            df: DataFrame to write
            location: Table name or location identifier
            metadata: Optional metadata to track
        
        Returns:
            Write result with path, rows, timestamp
        """
        pass
    
    @abstractmethod
    def read(
        self,
        location: str,
        version: str | None = None
    ) -> DataFrame | None:
        """
        Read from output
        
        Args:
            location: Table name or location identifier
            version: Optional specific version (timestamp)
        
        Returns:
            DataFrame
        """
        pass


class VersionedWriter(TableIO):
    """Interface for writers supporting versioning"""
    
    @abstractmethod
    def list_versions(self, location: str) -> List[str]:
        """List available versions for location"""
        pass
    
    @abstractmethod
    def cleanup_old_versions(self, location: str, keep_recent: int) -> int:
        """Remove old versions, keeping recent ones"""
        pass


class MetadataTracker(TableIO):
    """Interface for writers tracking metadata"""
    
    @abstractmethod
    def get_metadata_index(self) -> DataFrame | None:
        """Get index of all writes"""
        pass
    
    @abstractmethod
    def track_write(
        self,
        location: str,
        metadata: Dict[str, Any]
    ) -> None:
        """Track a write"""
        pass


class ReportingWriter(TableIO):
    """Interface for writers generating reports"""
    
    @abstractmethod
    def generate_report(self) -> str:
        """Generate report of all outputs"""
        pass
