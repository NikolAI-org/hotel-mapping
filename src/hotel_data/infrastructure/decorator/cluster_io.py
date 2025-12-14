from hotel_data.infrastructure.core.table_io import TableIO
from pyspark.sql import DataFrame, functions as F
from typing import Dict, Any

class ClusterIO(TableIO):
    """
    Decorator: Adds clustering-specific formatting
    Works with ANY OutputWriter implementation
    """

    def __init__(self, base_writer: TableIO, logger):
        self.base_writer = base_writer
        self.logger = logger

    def write(
        self,
        df: DataFrame,
        location: str,
        metadata: Dict[str, Any] | None = None
    ) -> Dict[str, Any]:
        """Write with cluster-specific formatting"""
        
        try:
            # ✅ FIX: Validate cluster_id exists
            if "cluster_id" not in df.columns:
                self.logger.warning(
                    f"⚠ cluster_id not found in DataFrame!",
                    available_columns=df.columns
                )
                # Write as-is without formatting
                return self.base_writer.write(df, location, metadata)
            
            # Calculate statistics
            stats = self._calculate_stats(df)
            
            # Enhance metadata
            cluster_metadata = {
                **(metadata or {}),
                'total_clusters': stats['total_clusters'],
                'total_hotels': stats['total_hotels'],
                'avg_cluster_size': stats['avg_cluster_size'],
                'largest_cluster': stats['largest_cluster']
            }
            
            # Format clusters
            df_formatted = self._format_clusters(df)
            
            # Call base writer
            result = self.base_writer.write(
                df_formatted,
                location,
                cluster_metadata
            )
            
            self.logger.info(
                f"✓ ClusterWriter: {stats['total_clusters']} clusters, "
                f"{stats['total_hotels']} hotels"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"ClusterIO.write() failed: {str(e)}")
            # Fallback: write original DataFrame
            return self.base_writer.write(df, location, metadata)

    def read(self, location: str, version: str | None = None):
        return self.base_writer.read(location, version)

    def _calculate_stats(self, df: DataFrame) -> Dict[str, Any]:
        """Calculate cluster statistics"""
        try:
            # ✅ FIX: Check if cluster_id exists before aggregating
            if "cluster_id" not in df.columns:
                return {
                    'total_clusters': 0,
                    'total_hotels': df.count(),
                    'avg_cluster_size': 0,
                    'largest_cluster': 0
                }
            
            cluster_stats = df.groupBy("cluster_id").agg(
                F.count("*").alias("size"),
                F.avg("composite_score").alias("avg_score") if "composite_score" in df.columns else F.lit(0)
            )
            
            total_clusters = cluster_stats.count()
            total_hotels = df.count()
            
            size_stats = cluster_stats.select(
                F.avg("size").alias("avg_size"),
                F.max("size").alias("max_size")
            ).collect()
            
            if not size_stats:
                avg_size = 0.0
                max_size = 0
            else:
                row = size_stats[0]
                avg_size = float(row.avg_size) if row.avg_size is not None else 0.0
                max_size = int(row.max_size) if row.max_size is not None else 0
            
            return {
                'total_clusters': total_clusters,
                'total_hotels': total_hotels,
                'avg_cluster_size': avg_size,
                'largest_cluster': max_size
            }
            
        except Exception as e:
            self.logger.warning(f"Failed to calculate stats: {str(e)}")
            return {
                'total_clusters': 0,
                'total_hotels': df.count() if df else 0,
                'avg_cluster_size': 0,
                'largest_cluster': 0
            }

    def _format_clusters(self, df: DataFrame) -> DataFrame:
        """Format cluster output - ✅ FIXED to handle missing cluster_id"""
        
        try:
            # Build list of columns to select
            columns_to_select = []
            
            # ✅ FIX: Only select cluster_id if it exists
            if "cluster_id" in df.columns:
                columns_to_select.append("cluster_id")
            
            # Add other required columns if they exist
            for col in ["id_i", "id_j", "name_i","name_j","composite_score", "confidence_level", "is_black_hole"]:
                if col in df.columns:
                    columns_to_select.append(col)
            
            # If cluster_id not in columns, add all available columns
            if "cluster_id" not in columns_to_select:
                self.logger.warning("cluster_id not available, returning all columns")
                return df
            
            # Select and sort
            result = df.select(columns_to_select)
            
            # Only sort by cluster_id if it exists
            if "cluster_id" in result.columns:
                result = result.sort("cluster_id")
                if "composite_score" in result.columns:
                    result = result.orderBy(
                        F.col("cluster_id"),
                        F.col("composite_score").desc()
                    )
            
            return result
            
        except Exception as e:
            self.logger.error(f"_format_clusters failed: {str(e)}, returning original df")
            return df
