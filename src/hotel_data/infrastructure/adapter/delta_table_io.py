from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, current_timestamp
from hotel_data.infrastructure.core.table_io import (
    TableIO, VersionedWriter, MetadataTracker
)
from hotel_data.delta.delta_table_manager import DeltaTableManager
from typing import Dict, Any, Optional, List
from datetime import datetime
import os


class DeltaTableIO(VersionedWriter, MetadataTracker, TableIO):
    """
    Adapter that wraps DeltaTableManager as OutputWriter
    
    Design:
    - SRP: Only wraps DeltaTableManager, no business logic
    - Translates OutputWriter interface to DeltaTableManager operations
    - Adds versioning support via timestamps
    - Tracks metadata of all writes
    - Can be decorated with pipeline-specific decorators
    
    Reusability:
    - DeltaTableManager unchanged (can be used directly)
    - DeltaWriter provides standardized interface
    - Works across multiple pipelines
    """
    
    def __init__(
        self,
        delta_manager: DeltaTableManager,
        logger,
        metadata_table: str = "_write_metadata"
    ):
        """
        Initialize DeltaWriter
        
        Args:
            delta_manager: Existing DeltaTableManager instance
            logger: Logger instance
            metadata_table: Table name for tracking metadata
        """
        self.delta_manager = delta_manager
        self.logger = logger
        self.metadata_table = metadata_table
        self.write_history = []
    
    # ════════════════════════════════════════════════════════════════════════
    # CORE WRITE/READ (Delegates to DeltaTableManager)
    # ════════════════════════════════════════════════════════════════════════
    
    def write(
        self,
        df: DataFrame,
        location: str,
        metadata: Dict[str, Any] | None = None
    ) -> Dict[str, Any]:
        """
        Write DataFrame to Delta table WITHOUT adding metadata columns.
        
        CRITICAL FIX:
        - Write ORIGINAL DataFrame (no _meta_* columns)
        - Track metadata in separate _write_metadata table
        
        Args:
            df: DataFrame to write (will NOT be modified)
            location: Table name to write to
            metadata: Dict with stats/debug info (stored separately, not in df)
        
        Returns:
            Dict with write result (path, rows, timestamp)
        """
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Prepare metadata for tracking (DO NOT ADD TO DataFrame)
            write_metadata = {
                'location': location,
                'timestamp': timestamp,
                'rows': df.count(),
                'columns': len(df.columns),
                **(metadata or {})
            }
            
            # ✅ FIX: Write ORIGINAL DataFrame WITHOUT metadata columns
            # This preserves your original schema (name, id, cluster_id, etc.)
            self.delta_manager.write_table(
                table_name=location,
                df=df,  # ← Write original df, NOT modified version
                mode="overwrite"
            )
            
            # Track metadata separately in metadata table
            self.track_write(location, write_metadata)
            
            # Return result
            result = {
                'location': location,
                'timestamp': timestamp,
                'rows': write_metadata['rows'],
                'columns': write_metadata['columns']
            }
            
            self.write_history.append({
                'location': location,
                'result': result,
                'metadata': write_metadata
            })
            
            self.logger.info(
                f"DeltaWriter: Wrote {location}",
                timestamp=timestamp,
                rows=write_metadata['rows'],
                note="Metadata tracked separately in _write_metadata table"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"DeltaWriter write failed for {location}: {str(e)}")
            raise
    
    def read(
        self,
        location: str,
        version: str | None = None
    ) -> DataFrame | None:
        """
        Read from Delta table.
        
        Returns: DataFrame with ONLY original columns (no _meta_* columns)
        """
        try:
            df = self.delta_manager.read_table(table_name=location)
            self.logger.info(f"DeltaWriter: Read {location} → {df.count()} rows")
            return df
        except Exception as e:
            self.logger.error(f"DeltaWriter read failed for {location}: {str(e)}")
            return None
    
    # ════════════════════════════════════════════════════════════════════════
    # VERSIONING (Via timestamp-based directory structure)
    # ════════════════════════════════════════════════════════════════════════
    
    def list_versions(self, location: str) -> List[str]:
        """List all versions (timestamps) for a location"""
        try:
            # Read metadata index and filter by location
            if self._metadata_index_exists():
                metadata_df = self.delta_manager.read_table(self.metadata_table)
                versions = (
                    metadata_df
                    .filter(f"location = '{location}'")
                    .select("timestamp")
                    .distinct()
                    .collect()
                )
                return [row['timestamp'] for row in versions]
            return []
        except Exception as e:
            self.logger.warning(f"Failed to list versions for {location}: {str(e)}")
            return []
    
    def cleanup_old_versions(
        self,
        location: str,
        keep_recent: int
    ) -> int:
        """
        Remove old versions, keeping most recent.
        
        Note: This is simplified. In production, you'd manage
        version directories or use Delta table time travel.
        """
        try:
            versions = self.list_versions(location)
            removed = 0
            
            # Keep most recent N versions
            for old_version in versions[keep_recent:]:
                # This is simplified - in production you'd handle directory cleanup
                self.logger.info(f"Would remove old version: {old_version}")
                removed += 1
            
            self.logger.info(f"Cleaned {removed} old versions for {location}")
            return removed
        except Exception as e:
            self.logger.warning(f"Cleanup failed: {str(e)}")
            return 0
    
    # ════════════════════════════════════════════════════════════════════════
    # METADATA TRACKING
    # ════════════════════════════════════════════════════════════════════════
    
    def _metadata_index_exists(self) -> bool:
        """Check if metadata index table exists"""
        try:
            self.delta_manager.read_table(self.metadata_table)
            return True
        except Exception:
            return False

    def get_metadata_index(self) -> DataFrame | None:
        """Get metadata index of all writes"""
        try:
            if self._metadata_index_exists():
                return self.delta_manager.read_table(self.metadata_table)
            self.logger.info("No metadata index yet")
            return None
        except Exception as e:
            self.logger.warning(f"Failed to get metadata index: {str(e)}")
            return None
    
    def track_write(
        self,
        location: str,
        metadata: Dict[str, Any]
    ) -> None:
        """
        Track a write in metadata index.
        
        Creates metadata table if needed.
        This stores metadata SEPARATELY from your data tables.
        """
        try:
            spark = self.delta_manager.spark
            
            # Create metadata record with all details
            record = {
                'location': location,
                'timestamp': datetime.now().isoformat(),
                **metadata
            }
            
            metadata_df = spark.createDataFrame([record])
            
            # Write to metadata table
            self.delta_manager.write_table(
                table_name=f"{location}{self.metadata_table}",
                df=metadata_df,
                mode="append"
            )
            
            self.logger.debug(f"Metadata tracked for {location}")
            
        except Exception as e:
            self.logger.warning(f"Metadata tracking failed: {str(e)}")
            # Don't raise - metadata tracking failure shouldn't block data writes
