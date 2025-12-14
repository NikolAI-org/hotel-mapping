from pyspark.sql import DataFrame, SparkSession, functions as F, Window
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, 
    TimestampType, DoubleType
)
from typing import Dict, Any, Optional, Tuple
from datetime import datetime

from hotel_data.infrastructure.core.table_io import TableIO


class GoldenRecordTransformer:
    """
    Transforms clustering output (06_final_clusters) and hotel data
    into Golden Records matching the exact specification
    """
    
    def __init__(self, logger, spark: SparkSession, writer: TableIO):
        """Initialize transformer"""
        self.logger = logger
        self.spark = spark
        self.writer = writer
        self.timestamp = datetime.now()
    
    def build_from_delta(
        self,
        hotels_df: DataFrame,
        clusters_df: DataFrame,
        scored_pairs_df: Optional[DataFrame] = None
    ) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Build Golden Records from Delta sources
        
        Args:
            hotels_df: Original hotels table from delta/hotels
            clusters_df: Final clusters from delta/06_final_clusters
            scored_pairs_df: Optional scored pairs from delta/02_scored_pairs
            
        Returns:
            (golden_records_df, validation_stats)
        """
        self.logger.info("Building Golden Records from Delta sources...")
        
        try:
            # Step 1: Validate inputs
            self._validate_inputs(hotels_df, clusters_df)
            
            # Step 2: Extract cluster members with hotel data
            cluster_members = self._extract_cluster_members(hotels_df, clusters_df)
            
            # Step 3: Identify cluster masters
            cluster_masters = self._identify_cluster_masters(cluster_members)
            
            # Step 4: Aggregate cluster data
            aggregated = self._aggregate_cluster_data(cluster_members)
            
            # Step 5: Build Golden Records
            golden_records = self._build_golden_records(cluster_masters, aggregated)
            
            # Step 6: Validate output
            stats = self._validate_output(golden_records)
            
            self.logger.info(
                "✓ Golden Records built successfully",
                record_count=golden_records.count(),
                validation_passed=stats['validation_passed']
            )
            
            return golden_records, stats
            
        except Exception as e:
            self.logger.error(f"Golden Record building failed: {str(e)}")
            raise
    
    def _validate_inputs(self, hotels_df: DataFrame, clusters_df: DataFrame) -> None:
        """Validate input DataFrames have required columns"""
        self.logger.debug("Validating input schemas...")
        
        # Check hotels table
        required_hotel_cols = {
            'providerId', 'name', 'geoCode_lat', 'geoCode_long', 'combined_address', 'id'
        }
        hotels_cols = set(hotels_df.columns)
        missing_hotel = required_hotel_cols - hotels_cols
        
        if missing_hotel:
            raise ValueError(f"hotels_df missing columns: {missing_hotel}")
        
        # Check clusters table
        required_cluster_cols = {'cluster_id', 'id_i', 'id_j', 'name_i',"name_j",'composite_score'}
        clusters_cols = set(clusters_df.columns)

        missing_cluster = required_cluster_cols - clusters_cols
        if missing_cluster:
            raise ValueError(f"clusters_df missing columns: {missing_cluster}")
        
        self.logger.debug("✓ Input validation passed")
    
    def _extract_cluster_members(
        self,
        hotels_df: DataFrame,
        clusters_df: DataFrame
    ) -> DataFrame:
        """
        Extract all hotels in each cluster with their attributes
        From clusters, we get id_i and id_j. We need to:
        1. Expand both to get all members
        2. Join with hotels table to get attributes
        """
        self.logger.debug("Extracting cluster members...")

        # Filter out black holes if present
        clean_clusters = clusters_df
        if "is_black_hole" in clusters_df.columns:
            clean_clusters = clusters_df.filter(F.col("is_black_hole") != True)

        # Get both id_i and id_j as members
        from_id_i = clean_clusters.select(
            F.col("cluster_id"),
            F.col("id_i").alias("hotel_id"),
            F.col("name_i").alias("cluster_name"),
            F.col("composite_score").alias("pair_score")
        )

        from_id_j = clean_clusters.select(
            F.col("cluster_id"),
            F.col("id_j").alias("hotel_id"),
            F.col("name_j").alias("cluster_name"),
            F.col("composite_score").alias("pair_score")
        )

        # Combine and deduplicate
        all_members = from_id_i.union(from_id_j).distinct()

        # Join with hotels to get attributes
        # CORRECTED: Keep hotel_id (which is the actual source ID from clusters)
        # and alias the hotel table's id to something else if needed
        members_with_data = all_members.join(
            hotels_df.select(
                F.col("id").alias("hotel_pk_id"),  # CHANGED: Different name to avoid confusion
                "providerId",
                F.col("name").alias("hotel_name"),
                "starRating",
                "geoCode_lat",
                "geoCode_long",
                "combined_address",
                "contact_phones",
                "contact_emails"
            ),
            on=F.col("hotel_id") == F.col("hotel_pk_id"),
            how="left"
        ).drop("hotel_pk_id")  # Drop the redundant PK after join

        member_count = members_with_data.count()
        self.logger.debug(f"Extracted {member_count} cluster members")
        return members_with_data

    
    def _identify_cluster_masters(
        self,
        cluster_members: DataFrame
    ) -> DataFrame:
        """
        Identify the master (representative) hotel for each cluster
        Master = highest average composite_score within cluster
        """
        self.logger.debug("Identifying cluster masters...")

        # Calculate average score per hotel per cluster
        # FIX: Use 'hotel_name' instead of 'name'
        avg_scores = cluster_members.groupBy("cluster_id", "hotel_id").agg(
            F.avg("pair_score").alias("avg_score"),
            F.first("providerId").alias("provider_id"),
            F.first("hotel_name").alias("primary_name"),  # CHANGED: Reference 'hotel_name'
            F.first("starRating").alias("primary_star_rating"),
            F.first("geoCode_lat").alias("lat"),
            F.first("geoCode_long").alias("lon"),
            F.first("combined_address").alias("primary_address"),
            F.first("contact_phones").alias("phones"),
            F.first("contact_emails").alias("emails")
        )

        # Select highest-scoring member as master
        masters = avg_scores.withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("cluster_id")
                .orderBy(F.col("avg_score").desc())
            )
        ).filter(F.col("rank") == 1).drop("rank", "avg_score")

        master_count = masters.count()
        self.logger.debug(f"Identified {master_count} cluster masters")
        return masters

    
    def _aggregate_cluster_data(
        self,
        cluster_members: DataFrame
    ) -> DataFrame:
        """
        Aggregate all data within each cluster
        Collects:
        - All original_ids (hotel_id from source clusters)
        - All provider_ids
        - All names
        - All phones
        - All emails
        - Average latitude/longitude (centroid)
        """
        self.logger.debug("Aggregating cluster data...")

        # Remove rows with null hotel_id (failed joins)
        valid_members = cluster_members.filter(F.col("hotel_id").isNotNull())

        aggregated = valid_members.groupBy("cluster_id").agg(
            # Collect IDs - hotel_id is the original source ID from clusters
            F.collect_set(F.col("hotel_id")).alias("all_source_ids"),
            F.collect_set(F.col("providerId")).alias("all_provider_ids"),
            # Collect attributes
            F.collect_set(F.col("hotel_name")).alias("all_names_reported"),
            F.collect_set(F.col("contact_phones")).alias("all_phones"),
            F.collect_set(F.col("contact_emails")).alias("all_emails"),
            F.collect_set(F.col("combined_address")).alias("all_address"),
            # Calculate centroid
            F.avg(F.col("geoCode_lat")).alias("centroid_lat"),
            F.avg(F.col("geoCode_long")).alias("centroid_lon"),
            # Stats
            F.count(F.col("hotel_id")).alias("cluster_size")
        ).withColumn(
            # Remove nulls from arrays
            "all_source_ids",
            F.array_remove(F.col("all_source_ids"), None)
        ).withColumn(
            "all_provider_ids",
            F.array_remove(F.col("all_provider_ids"), None)
        ).withColumn(
            "all_names_reported",
            F.array_remove(F.col("all_names_reported"), None)
        ).withColumn(
            "all_phones",
            F.array_remove(F.col("all_phones"), None)
        ).withColumn(
            "all_emails",
            F.array_remove(F.col("all_emails"), None)
        )

        agg_count = aggregated.count()
        self.logger.debug(f"Aggregated {agg_count} clusters")
        return aggregated
    
    def _build_golden_records(
        self,
        cluster_masters: DataFrame,
        aggregated: DataFrame
    ) -> DataFrame:
        """
        Build final Golden Records with exact schema
        
        Joins master data with aggregated data
        """
        self.logger.debug("Building final Golden Records...")
        
        # Join masters with aggregated data
        combined = cluster_masters.join(
            aggregated,
            on="cluster_id",
            how="left"
        )
        
        # Build final schema with all 12 fields
        golden_records = combined.select(
            # Primary key (cluster_id as unique_hotel_id)
            F.col("cluster_id").cast(StringType()).alias("unique_hotel_id"),
            
            # Primary fields from master
            F.col("primary_name"),
            F.col("primary_star_rating").cast(StringType()),
            
            # Centroid geocode struct
            F.struct(
                F.col("centroid_lat").alias("latitude"),
                F.col("centroid_lon").alias("longitude")
            ).alias("primary_geocode"),
            
            F.col("primary_address"),
            
            # Aggregated arrays
            F.col("all_source_ids"),
            F.col("all_provider_ids"),
            F.col("all_names_reported"),
            F.col("all_phones"),
            F.col("all_emails"),
            
            # Timestamp
            F.lit(self.timestamp).cast(TimestampType()).alias("golden_record_timestamp")
        )
        
        record_count = golden_records.count()
        self.logger.debug(f"Built {record_count} Golden Records")
        
        return golden_records
    
    def _validate_output(
        self,
        golden_records: DataFrame
    ) -> Dict[str, Any]:
        """
        Validate Golden Records output
        
        Checks:
        1. All required columns present
        2. NOT NULL constraints
        3. Array non-empty for required fields
        4. Valid geocode values
        """
        self.logger.debug("Validating Golden Records...")
        
        issues = []
        stats = {
            'total_records': golden_records.count(),
            'validation_passed': True,
            'issues': []
        }
        
        # Check 1: All required columns
        required_cols = {
            'unique_hotel_id', 'primary_name', 'primary_star_rating',
            'primary_geocode', 'primary_address', 'all_source_ids',
            'all_provider_ids', 'all_names_reported', 'all_phones',
            'all_emails', 'golden_record_timestamp'
        }
        
        actual_cols = set(golden_records.columns)
        missing_cols = required_cols - actual_cols
        
        if missing_cols:
            issues.append(f"Missing columns: {missing_cols}")
        
        # Check 2: NOT NULL fields
        # not_null_fields = [
        #     'unique_hotel_id',
        #     'all_source_ids',
        #     'all_provider_ids',
        #     'golden_record_timestamp'
        # ]
        not_null_fields = [
            'unique_hotel_id',
            'golden_record_timestamp'
        ]
        
        for field in not_null_fields:
            if field in actual_cols:
                null_count = golden_records.filter(
                    F.col(field).isNull()
                ).count()
                
                if null_count > 0:
                    issues.append(f"Nulls in NOT NULL field '{field}': {null_count}")
                    stats[f'null_count_{field}'] = null_count
        
        # Check 3: Arrays not empty (for NOT NULL arrays)
        for array_field in ['all_source_ids', 'all_provider_ids']:
            if array_field in actual_cols:
                empty_count = golden_records.filter(
                    F.size(F.col(array_field)) == 0
                ).count()
                
                if empty_count > 0:
                    issues.append(f"Empty arrays in '{array_field}': {empty_count}")
                    stats[f'empty_count_{array_field}'] = empty_count
        
        # Check 4: Valid geocodes
        if 'primary_geocode' in actual_cols:
            invalid_geo = golden_records.filter(
                (F.col("primary_geocode.latitude") < -90) |
                (F.col("primary_geocode.latitude") > 90) |
                (F.col("primary_geocode.longitude") < -180) |
                (F.col("primary_geocode.longitude") > 180) |
                F.col("primary_geocode.latitude").isNull() |
                F.col("primary_geocode.longitude").isNull()
            ).count()
            
            if invalid_geo > 0:
                issues.append(f"Invalid or null geocodes: {invalid_geo}")
                stats['invalid_geocodes'] = invalid_geo
        
        # Check 5: Data quality metrics
        # Check 5: Data quality metrics

        row_avg = golden_records.select(
            F.avg(F.size(F.col("all_source_ids"))).alias("avg_size")
        ).first()   # returns a Row object

        stats['avg_cluster_size'] = float(row_avg['avg_size']) if row_avg and row_avg['avg_size'] is not None else 0.0


        row_max = golden_records.select(
            F.max(F.size(F.col("all_source_ids"))).alias("max_size")
        ).first()

        stats['max_cluster_size'] = int(row_max['max_size']) if row_max and row_max['max_size'] is not None else 0


        row_providers = golden_records.select(
            F.avg(F.size(F.col("all_provider_ids"))).alias("avg_providers")
        ).first()

        stats['providers_distribution'] = float(row_providers['avg_providers']) if row_providers and row_providers['avg_providers'] is not None else 0.0

        
        if issues:
            stats['validation_passed'] = False
            stats['issues'] = issues
            for issue in issues:
                self.logger.warning(f"Validation issue: {issue}")
        else:
            self.logger.info("✓ All validation checks passed!")
        
        return stats
    
    def write_to_delta(
        self,
        golden_records: DataFrame,
        location: str,
        mode: str = "overwrite"
    ) -> None:
        """
        Write Golden Records to Delta Lake
        
        Args:
            golden_records: Golden Records DataFrame
            location: Delta table location path
            mode: Write mode (overwrite, append, ignore, error)
        """
        self.logger.info(f"Writing Golden Records to {location}...")
        
        try:

            self.writer.write(
                golden_records,
                "09_golden_records"
            )
            # golden_records.write \
            #     .format("delta") \
            #     .mode(mode) \
            #     .option("mergeSchema", "true") \
            #     .save(location)
            
            self.logger.info(
                f"✓ Golden Records written successfully",
                location=location,
                record_count=golden_records.count()
            )
            
        except Exception as e:
            self.logger.error(f"Failed to write Golden Records: {str(e)}")
            raise
    
    def create_managed_table(
        self,
        spark: SparkSession,
        location: str,
        table_name: str = "hotel_golden_records"
    ) -> None:
        """
        Register Golden Records as managed Hive table
        
        Args:
            spark: SparkSession
            location: Delta location path
            table_name: Table name
        """
        self.logger.info(f"Registering table: {table_name}...")
        
        try:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name}
                USING DELTA
                LOCATION '{location}'
            """)
            
            # Create indexes for common queries
            spark.sql(f"""
                ALTER TABLE {table_name}
                SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
            """)
            
            self.logger.info(f"✓ Table {table_name} registered successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to create managed table: {str(e)}")
            raise
