# infrastructure/delta_lake/debug_writer.py

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, from_unixtime, unix_timestamp
)
from pyspark.sql.types import StructType
from typing import Dict, Any, Optional
import os
from datetime import datetime

class DeltaLakeDebugWriter:
    """
    Manages debug output to Delta Lake for each pipeline stage
    
    Features:
    - Saves DataFrames at each stage
    - Tracks metadata (timestamp, row count, schema)
    - Manages versioning
    - Provides cleanup and analysis utilities
    """
    
    def __init__(
        self,
        spark: SparkSession,
        logger,
        base_path: str = "delta_lake/debug",
        enable_debug: bool = True
    ):
        """
        Initialize debug writer
        
        Args:
            spark: SparkSession
            logger: Logger instance
            base_path: Base path for debug outputs (default: delta_lake/debug)
            enable_debug: Enable/disable debug output (for quick toggle)
        """
        self.spark = spark
        self.logger = logger
        self.base_path = base_path
        self.enable_debug = enable_debug
        
        # Create base directory
        if enable_debug:
            os.makedirs(base_path, exist_ok=True)
        
        self.logger.info(
            f"DeltaLakeDebugWriter initialized",
            base_path=base_path,
            enabled=enable_debug
        )
    
    # ════════════════════════════════════════════════════════════════════════
    # MAIN WRITE METHODS
    # ════════════════════════════════════════════════════════════════════════
    
    def write_stage(
        self,
        df: DataFrame,
        stage_name: str,
        stage_number: int,
        metadata: Dict[str, Any] | None = None
    ) -> None:
        """
        Write DataFrame for a pipeline stage
        
        Args:
            df: DataFrame to write
            stage_name: Name of stage (e.g., "raw_pairs", "scored_pairs")
            stage_number: Stage number (e.g., 1, 2, 3)
            metadata: Optional metadata to include
        """
        
        if not self.enable_debug:
            return
        
        try:
            # ═══════════════════════════════════════════════════════════════
            # Build path
            # ═══════════════════════════════════════════════════════════════
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            stage_path = (
                f"{self.base_path}/{stage_number:02d}_{stage_name}/{timestamp}"
            )
            
            self.logger.info(
                f"Writing debug output: {stage_name}",
                path=stage_path,
                rows=df.count()
            )
            
            # ═══════════════════════════════════════════════════════════════
            # Add metadata columns
            # ═══════════════════════════════════════════════════════════════
            
            df_with_metadata = self._add_metadata_columns(
                df,
                stage_name=stage_name,
                stage_number=stage_number,
                metadata=metadata
            )
            
            # ═══════════════════════════════════════════════════════════════
            # Write to Delta Lake
            # ═══════════════════════════════════════════════════════════════
            
            df_with_metadata.write \
                .format("delta") \
                .mode("overwrite") \
                .save(stage_path)
            
            # ═══════════════════════════════════════════════════════════════
            # Log write summary
            # ═══════════════════════════════════════════════════════════════
            
            summary = self._get_write_summary(df_with_metadata, stage_path)
            self.logger.info(f"Debug output written", **summary)
            
            # ═══════════════════════════════════════════════════════════════
            # Update metadata index
            # ═══════════════════════════════════════════════════════════════
            
            self._update_metadata_index(stage_name, stage_number, summary)
            
        except Exception as e:
            self.logger.error(
                f"Failed to write debug output for {stage_name}",
                error=str(e)
            )
            # Don't fail pipeline on debug write error
            pass
    
    # ════════════════════════════════════════════════════════════════════════
    # HELPER METHODS
    # ════════════════════════════════════════════════════════════════════════
    
    def _add_metadata_columns(
        self,
        df: DataFrame,
        stage_name: str,
        stage_number: int,
        metadata: Dict[str, Any] | None = None
    ) -> DataFrame:
        """
        Add metadata columns to DataFrame
        
        Columns added:
        - _debug_stage_name: Name of stage
        - _debug_stage_number: Number of stage
        - _debug_timestamp: When written
        - _debug_row_count: Total rows
        - _debug_metadata: Additional metadata
        """
        
        result = (
            df
            .withColumn("_debug_stage_name", lit(stage_name))
            .withColumn("_debug_stage_number", lit(stage_number))
            .withColumn("_debug_timestamp", current_timestamp())
        )
        
        # Add custom metadata if provided
        if metadata:
            for key, value in metadata.items():
                if isinstance(value, (int, float, str, bool)):
                    result = result.withColumn(f"_debug_{key}", lit(value))
        
        return result
    
    def _get_write_summary(
        self,
        df: DataFrame,
        path: str
    ) -> Dict[str, Any]:
        """Get summary of written data"""
        
        row_count = df.count()
        schema = df.schema
        
        return {
            'path': path,
            'rows': row_count,
            'columns': len(schema.fields),
            'column_names': [field.name for field in schema.fields][:5]  # First 5
        }
    
    def _update_metadata_index(
        self,
        stage_name: str,
        stage_number: int,
        summary: Dict[str, Any]
    ) -> None:
        """
        Update metadata index for tracking all outputs
        
        Creates a metadata table that tracks all written stages
        """
        
        try:
            metadata_path = f"{self.base_path}/_metadata_index"
            
            metadata_df = self.spark.createDataFrame([
                {
                    'stage_name': stage_name,
                    'stage_number': stage_number,
                    'output_path': summary['path'],
                    'row_count': summary['rows'],
                    'column_count': summary['columns'],
                    'timestamp': datetime.now().isoformat()
                }
            ])
            
            # Append to metadata table
            metadata_df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(metadata_path)
            
        except Exception as e:
            self.logger.warning(f"Failed to update metadata index: {str(e)}")
    
    # ════════════════════════════════════════════════════════════════════════
    # UTILITY METHODS
    # ════════════════════════════════════════════════════════════════════════
    
    def read_stage(
        self,
        stage_name: str,
        stage_number: int,
        timestamp: str | None = None
    ) -> DataFrame:
        """
        Read a previously written stage
        
        Args:
            stage_name: Name of stage (e.g., "scored_pairs")
            stage_number: Number of stage (e.g., 2)
            timestamp: Optional specific timestamp (latest if not provided)
        
        Returns:
            DataFrame from Delta Lake
        """
        
        stage_dir = f"{self.base_path}/{stage_number:02d}_{stage_name}"
        
        if not os.path.exists(stage_dir):
            self.logger.warning(f"Stage directory not found: {stage_dir}")
            return self.spark.createDataFrame([], StructType([]))
        
        if timestamp:
            path = f"{stage_dir}/{timestamp}"
        else:
            # Get latest timestamp
            timestamps = sorted(os.listdir(stage_dir), reverse=True)
            if not timestamps:
                self.logger.warning(f"No timestamps found in {stage_dir}")
                return self.spark.createDataFrame([], StructType([]))
            path = f"{stage_dir}/{timestamps}"
        
        try:
            df = self.spark.read.format("delta").load(path)
            self.logger.info(f"Loaded stage: {stage_name} from {path}")
            return df
        except Exception as e:
            self.logger.error(f"Failed to load stage: {str(e)}")
            return self.spark.createDataFrame([], StructType([]))
    
    def get_metadata_index(self) -> DataFrame:
        """Get metadata index of all written stages"""
        
        try:
            metadata_path = f"{self.base_path}/_metadata_index"
            return self.spark.read.format("delta").load(metadata_path)
        except Exception as e:
            self.logger.warning(f"Failed to load metadata index: {str(e)}")
            return self.spark.createDataFrame([], StructType([]))
    
    def list_stages(self) -> Dict[int, list]:
        """
        List all available stages and timestamps
        
        Returns:
            Dict: {stage_number: [timestamps]}
        """
        
        stages = {}
        
        try:
            for item in os.listdir(self.base_path):
                if item.startswith('_'):
                    continue
                
                # Parse stage number and name
                parts = item.split('_', 1)
                if len(parts) == 2 and parts[0].isdigit():
                    stage_number = int(parts[0])
                    item_path = f"{self.base_path}/{item}"
                    
                    if os.path.isdir(item_path):
                        timestamps = sorted(
                            [t for t in os.listdir(item_path) if not t.startswith('_')],
                            reverse=True
                        )
                        stages[stage_number] = timestamps
        
        except Exception as e:
            self.logger.warning(f"Failed to list stages: {str(e)}")
        
        return stages
    
    def cleanup_old_outputs(self, keep_recent: int = 5) -> None:
        """
        Clean up old debug outputs, keeping only most recent
        
        Args:
            keep_recent: Number of recent outputs to keep per stage
        """
        
        if not self.enable_debug:
            return
        
        self.logger.info(f"Cleaning up old outputs (keeping {keep_recent} recent)")
        
        try:
            import shutil
            
            stages = self.list_stages()
            
            for stage_number, timestamps in stages.items():
                if len(timestamps) > keep_recent:
                    # Remove old ones
                    for old_timestamp in timestamps[keep_recent:]:
                        stage_name = None
                        
                        # Find stage name
                        for item in os.listdir(self.base_path):
                            if item.startswith(f"{stage_number:02d}_"):
                                stage_name = item[3:]  # Remove "XX_"
                                break
                        
                        if stage_name:
                            old_path = (
                                f"{self.base_path}/{stage_number:02d}_{stage_name}/{old_timestamp}"
                            )
                            
                            try:
                                shutil.rmtree(old_path)
                                self.logger.info(f"Removed old output: {old_path}")
                            except Exception as e:
                                self.logger.warning(f"Failed to remove {old_path}: {str(e)}")
        
        except Exception as e:
            self.logger.warning(f"Cleanup failed: {str(e)}")
    
    def generate_debug_report(self) -> str:
        """
        Generate a debug report of all outputs
        
        Returns:
            String report for logging/display
        """
        
        stages = self.list_stages()
        
        report = "═" * 80 + "\n"
        report += "DEBUG OUTPUT REPORT\n"
        report += "═" * 80 + "\n"
        
        for stage_number in sorted(stages.keys()):
            timestamps = stages[stage_number]
            report += f"\nStage {stage_number}: {len(timestamps)} outputs\n"
            
            for i, timestamp in enumerate(timestamps[:3]):  # Show first 3
                report += f"  {i+1}. {timestamp}\n"
            
            if len(timestamps) > 3:
                report += f"  ... and {len(timestamps) - 3} more\n"
        
        report += "\n" + "═" * 80 + "\n"
        
        return report
