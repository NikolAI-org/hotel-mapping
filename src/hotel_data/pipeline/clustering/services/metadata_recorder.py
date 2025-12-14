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
    
    def get_metrics(
        self,
        clusters_df: DataFrame,
        scored_pairs_df: DataFrame,
        conflicts_df: DataFrame
    ) -> Dict[str, Any]:
        """
        Record comprehensive metadata from pipeline execution
        
        Args:
            clustersdf: Clustered hotel pairs DataFrame
            scored_pairs_df: Scored pairs DataFrame
            conflicts_df: Conflicts DataFrame
            
        Returns:
            Dict with all pipeline metadata
        """
        self.spark = clusters_df.sparkSession
        metadata = {}
        
        try:
            self.logger.info("Recording comprehensive metadata...")
            
            # Phase 1: Cluster Metrics
            metadata.update(self._collect_cluster_metrics(clusters_df))
            
            # Phase 2: Score Metrics
            metadata.update(self._collect_score_metrics(scored_pairs_df))
            
            # Phase 3: Conflict Metrics
            metadata.update(self._collect_conflict_metrics(conflicts_df))
            
            # Phase 4: Data Quality Metrics
            metadata.update(self._collect_data_quality_metrics(
                clusters_df, scored_pairs_df, conflicts_df
            ))
            
            # Phase 5: Processing Metrics
            metadata.update(self._collect_processing_metrics())
            
            # Phase 6: Write metadata to storage
            # self.get_metrics(metadata)
            
            self.logger.info(
                f"✓ Metadata recorded successfully",
                metrics_count=len(metadata),
                timestamp=metadata.get('processing_timestamp')
            )
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Metadata recording failed: {str(e)}")
            return {
                'total_clusters': 0,
                'total_pairs': 0,
                'error': str(e)
            }
    
    def record_metadata(self, 
        scored_pairs_df: DataFrame,
        clusters_df: DataFrame,
        metadata: Dict[str, Any]) -> None:
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
                "02_scored_pairs",
                metadata={
                    'phase': 1,
                    'description': 'Scored pairs with composite scores',
                    'row_count': int(scored_pairs_df.count())
                }
            )
            
            # ════════════════════════════════════════════════════════════════════
            # WRITE 2: Final Clusters
            # ════════════════════════════════════════════════════════════════════
            
            self.logger.debug(f"Writing {clusters_df.count()} clusters...")
            self.logger.info(f"06_final_clusters schema")
            self.logger.info(clusters_df.printSchema)
            self.writer.write(
                clusters_df,
                "06_final_clusters",
                metadata={
                    'phase': 4,
                    'description': 'Final clusters with assignments',
                    'row_count': int(clusters_df.count()),
                    **metadata  # Include all metadata
                }
            )
            
            # ════════════════════════════════════════════════════════════════════
            # WRITE 3: Metadata
            # ════════════════════════════════════════════════════════════════════
            
            # Convert metadata to DataFrame rows
            metadata_rows = [
                Row(key=str(k), value=str(v), metric_type=self._get_metric_type(k))
                for k, v in metadata.items()
            ]
            
            if not metadata_rows:
                self.logger.warning("No metadata to write")
                return
            
            # Create schema
            schema = StructType([
                StructField("key", StringType(), False),
                StructField("value", StringType(), False),
                StructField("metric_type", StringType(), True)
            ])
            
            # Create DataFrame
            metadata_df = self.spark.createDataFrame(metadata_rows, schema)
            
            # Write using writer
            self.logger.debug(f"Writing {len(metadata_rows)} metadata entries...")
            self.writer.write(
                metadata_df,
                "07_metadata",
                metadata={
                    'phase': 'metadata_recording',
                    'description': 'Pipeline metadata and statistics',
                    'entry_count': len(metadata_rows),
                    'timestamp': datetime.now().isoformat()
                }
            )
            
            self.logger.info(f"✓ Metadata written to storage: {len(metadata_rows)} entries")
            
        except Exception as e:
            self.logger.error(f"Failed to write metadata: {str(e)}")
            raise
    
    def _collect_cluster_metrics(self, clustersdf: DataFrame) -> Dict[str, Any]:
        """Collect cluster-related metrics"""
        try:
            if "cluster_id" not in clustersdf.columns:
                return {
                    'total_clusters': 0,
                    'total_hotels': 0,
                    'avg_cluster_size': 0.0,
                    'max_cluster_size': 0,
                    'min_cluster_size': 0,
                    'cluster_distribution': {}
                }
            
            total_hotels = clustersdf.count()
            
            # Get cluster statistics
            cluster_stats = clustersdf.groupBy("cluster_id").agg(
                F.count("*").alias("size"),
                F.collect_list("id_i").alias("hotel_ids")
            )
            
            total_clusters = cluster_stats.count()
            
            # Calculate size distribution
            size_stats = cluster_stats.agg(
                F.avg("size").alias("avg_size"),
                F.max("size").alias("max_size"),
                F.min("size").alias("min_size"),
                F.stddev("size").alias("stddev_size")
            ).first()
            
            # Defensive handling
            if size_stats is None:
                avg_size = 0.0
                max_size = 0
                min_size = 0
                stddev_size = 0.0
            else:
                avg_size = float(size_stats.avg_size or 0.0)
                max_size = int(size_stats.max_size or 0)
                min_size = int(size_stats.min_size or 0)
                stddev_size = float(size_stats.stddev_size or 0.0)
            
            # Percentile distribution
            percentile_stats = cluster_stats.agg(
                F.percentile_approx("size", 0.25).alias("p25"),
                F.percentile_approx("size", 0.50).alias("p50"),
                F.percentile_approx("size", 0.75).alias("p75"),
                F.percentile_approx("size", 0.95).alias("p95")
            ).first()
            
            # Defensive handling
            if percentile_stats is None:
                p25 = 0
                p50 = 0
                p75 = 0
                p95 = 0
            else:
                p25 = int(percentile_stats.p25 or 0)
                p50 = int(percentile_stats.p50 or 0)
                p75 = int(percentile_stats.p75 or 0)
                p95 = int(percentile_stats.p95 or 0)
            
            return {
                'total_clusters': int(total_clusters),
                'total_hotels': int(total_hotels),
                'avg_cluster_size': round(float(avg_size), 6),
                'max_cluster_size': int(max_size),
                'min_cluster_size': int(min_size),
                'stddev_cluster_size': round(float(stddev_size), 6),
                'cluster_density': round(
                    (total_clusters / max(total_hotels, 1)) * 100, 2
                ),
                'cluster_percentiles': {
                    'p25': int(p25),
                    'p50': int(p50),
                    'p75': int(p75),
                    'p95': int(p95),
                }
            }
        except Exception as e:
            self.logger.warning(f"Cluster metrics collection failed: {str(e)}")
            return {'total_clusters': 0, 'total_hotels': 0}
    
    def _collect_score_metrics(self, scored_pairs_df: DataFrame) -> Dict[str, Any]:
        """Collect score distribution metrics"""
        try:
            if "composite_score" not in scored_pairs_df.columns:
                return {
                    'avg_composite_score': 0.0,
                    'min_composite_score': 0.0,
                    'max_composite_score': 0.0
                }
            
            score_stats = scored_pairs_df.agg(
                F.avg("composite_score").alias("avg_score"),
                F.min("composite_score").alias("min_score"),
                F.max("composite_score").alias("max_score"),
                F.stddev("composite_score").alias("stddev_score"),
                F.percentile_approx("composite_score", 0.50).alias("median_score")
            ).first()
            
            if score_stats is None:
                avg_score = 0.0
                min_score = 0.0
                max_score = 0.0
                stddev_score = 0.0
                median_score = 0.0
            else:
                avg_score = float(score_stats.avg_score or 0.0)
                min_score = float(score_stats.min_score or 0.0)
                max_score = float(score_stats.max_score or 0.0)
                stddev_score = float(score_stats.stddev_score or 0.0)
                median_score = float(score_stats.median_score or 0.0)
            
            # Confidence level distribution
            conf_dist = {}
            if "confidence_level" in scored_pairs_df.columns:
                conf_counts = scored_pairs_df.groupBy("confidence_level").count().collect()
                for row in conf_counts:
                    conf_dist[row["confidence_level"]] = int(row["count"])
            
            return {
                'avg_composite_score': round(float(avg_score), 6),
                'min_composite_score': float(min_score),
                'max_composite_score': float(max_score),
                'median_composite_score': float(median_score),
                'stddev_composite_score': round(float(stddev_score), 6),
                'confidence_distribution': conf_dist
            }
        except Exception as e:
            self.logger.warning(f"Score metrics collection failed: {str(e)}")
            return {
                'avg_composite_score': 0.0,
                'min_composite_score': 0.0,
                'max_composite_score': 0.0
            }
    
    def _collect_conflict_metrics(self, conflicts_df: DataFrame) -> Dict[str, Any]:
        """Collect conflict detection metrics"""
        try:
            total_pairs = conflicts_df.count()
            conflicts_count = 0
            
            if "has_conflict" in conflicts_df.columns:
                conflicts_count = int(
                    conflicts_df.filter(F.col("has_conflict") == True).count()
                )
            
            conflict_percentage = (conflicts_count / max(total_pairs, 1)) * 100 if total_pairs > 0 else 0
            
            return {
                'total_conflict_checks': int(total_pairs),
                'conflicts_detected': int(conflicts_count),
                'conflict_percentage': round(conflict_percentage, 2),
                'clean_pairs': int(total_pairs - conflicts_count)
            }
        except Exception as e:
            self.logger.warning(f"Conflict metrics collection failed: {str(e)}")
            return {
                'total_conflict_checks': 0,
                'conflicts_detected': 0,
                'clean_pairs': 0
            }
    
    def _collect_data_quality_metrics(
        self,
        clustersdf: DataFrame,
        scored_pairs_df: DataFrame,
        conflicts_df: DataFrame
    ) -> Dict[str, Any]:
        """Collect data quality indicators"""
        try:
            quality_metrics = {}
            
            # Null check for critical columns in clusters
            if "cluster_id" in clustersdf.columns:
                null_count = int(
                    clustersdf.filter(F.col("cluster_id").isNull()).count()
                )
                quality_metrics['null_cluster_ids'] = null_count
            
            # Null check for composite_score
            if "composite_score" in scored_pairs_df.columns:
                null_scores = int(
                    scored_pairs_df.filter(F.col("composite_score").isNull()).count()
                )
                quality_metrics['null_scores'] = null_scores
            
            # Duplicate check
            total_rows = scored_pairs_df.count()
            unique_pairs = scored_pairs_df.select("id_i", "id_j").distinct().count()
            quality_metrics['duplicate_pairs'] = int(total_rows - unique_pairs)
            
            return quality_metrics
            
        except Exception as e:
            self.logger.warning(f"Data quality metrics collection failed: {str(e)}")
            return {}
    
    def _collect_processing_metrics(self) -> Dict[str, Any]:
        """Collect processing and timing metrics"""
        elapsed_time = (datetime.now() - self.processing_start_time).total_seconds()
        
        return {
            'processing_timestamp': datetime.now().isoformat(),
            'processing_start_time': self.processing_start_time.isoformat(),
            'processing_duration_seconds': round(elapsed_time, 2),
            'pipeline_version': '1.0.0',
            'recorder_type': 'ComprehensiveMetadataRecorder'
        }
    
    @staticmethod
    def _get_metric_type(key: str) -> str:
        """Classify metric type based on key name"""
        if 'cluster' in key.lower():
            return 'cluster_metric'
        elif 'score' in key.lower():
            return 'score_metric'
        elif 'conflict' in key.lower():
            return 'conflict_metric'
        elif 'quality' in key.lower() or 'null' in key.lower():
            return 'quality_metric'
        elif 'processing' in key.lower() or 'timestamp' in key.lower():
            return 'processing_metric'
        else:
            return 'other'
