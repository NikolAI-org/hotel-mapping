from pyspark.sql import DataFrame, SparkSession, Row, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from typing import Dict, Any, Optional
from datetime import datetime
from abc import ABC, abstractmethod
from hotel_data.infrastructure.core.table_io import TableIO
from hotel_data.pipeline.clustering.core.clustering_interfaces import MetadataRecorder



class ComprehensiveMetadataRecorder(MetadataRecorder):
    """
    Comprehensive metadata recorder that captures:
    - Cluster statistics (count, size distribution, density)
    - Score statistics (min, max, mean, distribution)
    - Data quality metrics
    - Performance metrics
    - Processing timestamps
    """
    
    def __init__(self, writer: TableIO, spark: SparkSession, logger):
        """
        Initialize MetadataRecorder
        
        Args:
            writer: TableIO instance for writing metadata
            logger: Logger instance
            config: Optional configuration dict
        """
        self.writer = writer
        self.logger = logger
        # self.config = config or {}
        self.spark = spark
        self.processing_start_time = datetime.now()
    
    def record_metadata(self, 
        scored_pairs_df: DataFrame,
        clusters_df: DataFrame,
        metadata: Dict[str, Any] | None) -> None:
        """Write metadata to storage as structured data"""
        try:
            from pyspark.sql import Row
            from pyspark.sql.types import StructType, StructField, StringType
            
            # ════════════════════════════════════════════════════════════════════
            # WRITE 1: Scored Pairs
            # ════════════════════════════════════════════════════════════════════
            
            self.logger.debug(f"Writing {scored_pairs_df.count()} scored pairs...")
            self.writer.write(
                scored_pairs_df,
                "02_scored_pairs"
            )
            
            # ════════════════════════════════════════════════════════════════════
            # WRITE 2: Final Clusters
            # ════════════════════════════════════════════════════════════════════
            
            self.logger.debug(f"Writing {clusters_df.count()} clusters...")
            self.writer.write(
                clusters_df,
                "06_final_clusters"
            )
            self.logger.info("Final Cluster Schema")
            self.logger.info(clusters_df.printSchema())
            
            
        except Exception as e:
            self.logger.error(f"Failed to write metadata: {str(e)}")
            raise
