from hotel_data.infrastructure.core.table_io import (
    TableIO, VersionedWriter, ReportingWriter
)
from pyspark.sql import DataFrame
from typing import Dict, Any


class DebugIO(VersionedWriter, ReportingWriter):
    """
    Decorator: Adds debugging capabilities
    
    Works with ANY OutputWriter implementation:
    - DeltaWriter
    - Any other OutputWriter
    - Can be chained with other decorators
    """
    
    def __init__(self, base_writer: TableIO, logger):
        self.base_writer = base_writer
        self.logger = logger
        self.write_history = []
    
    def write(
        self,
        df: DataFrame,
        location: str,
        metadata: Dict[str, Any] | None = None
    ) -> Dict[str, Any]:
        """Write with debug information"""
        
        # Enhance metadata
        debug_metadata = {
            **(metadata or {}),
            'stage_name': location,
            'row_count': df.count(),
            'columns_count': len(df.columns),
        }
        
        # Call base writer
        result = self.base_writer.write(df, location, debug_metadata)
        
        # Track for reporting
        self.write_history.append({
            'location': location,
            'result': result,
            'metadata': debug_metadata
        })
        
        return result
    
    def read(self, location: str, version: str | None = None):
        return self.base_writer.read(location, version)
    
    def list_versions(self, location: str) -> list:
        if isinstance(self.base_writer, VersionedWriter):
            return self.base_writer.list_versions(location)
        return []
    
    def cleanup_old_versions(self, location: str, keep_recent: int) -> int:
        if isinstance(self.base_writer, VersionedWriter):
            return self.base_writer.cleanup_old_versions(location, keep_recent)
        return 0
    
    def generate_report(self) -> str:
        """Generate debug report"""
        
        report = "═" * 80 + "\n"
        report += "DEBUG OUTPUT REPORT\n"
        report += "═" * 80 + "\n"
        
        for entry in self.write_history:
            report += f"\nLocation: {entry['location']}\n"
            report += f"  Timestamp: {entry['result']['timestamp']}\n"
            report += f"  Rows: {entry['result']['rows']}\n"
            report += f"  Columns: {entry['result']['columns']}\n"
        
        report += "\n" + "═" * 80 + "\n"
        
        return report
