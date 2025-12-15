from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, IntegerType, ArrayType, TimestampType

from hotel_data.config.config_loader import ConfigLoader
from hotel_data.config.paths import BASE_DELTA_PATH, CATALOG_NAME, DERBY_HOME, SCHEMA_NAME, WAREHOUSE_DIR
from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.pipeline.clustering.infrastructure.dependency_container import DependencyContainer

# Initialize Spark Session
spark = (
        SparkSession.builder.appName("HotelsPipelineWrite")
        .config("spark.jars.packages", ",".join([
            "io.delta:delta-spark_2.13:4.0.0",
            "org.apache.hadoop:hadoop-aws:3.4.1",
        ]))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        # ---- S3/MinIO config ----
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.1.4:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ---- Hive metastore (Derby) for local prod-like testing ----
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:{DERBY_HOME}/metastore_db;create=true",
        )
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionDriverName",
            "org.apache.derby.jdbc.EmbeddedDriver",
        )
        .config("spark.hadoop.datanucleus.autoCreateSchema", "true")
        .config("spark.hadoop.datanucleus.fixedDatastore", "true")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "4g")
        .enableHiveSupport()
        .getOrCreate()
    )

config = ConfigLoader.load_from_yaml('config.yaml')
Path(DERBY_HOME).mkdir(parents=True, exist_ok=True)
DependencyContainer.configure(config, spark)

writer = DependencyContainer.get_output_writer()

manager = DeltaTableManager(
        spark=spark,
        catalog_name=CATALOG_NAME,
        schema_name=SCHEMA_NAME,
        base_path=BASE_DELTA_PATH,
    )
    
final_cluster_df = manager.read_table("06_final_clusters")
    
scored_pair_df = manager.read_table("02_scored_pairs")

## ============================================================================
# STEP 1: Aggregate pairing information for each hotel name
# ============================================================================

# Get pairing details for each hotel (aggregating by name_i and name_j)
pairing_details = scored_pair_df.groupBy("name_i").agg(
    F.collect_list(F.struct(
        F.col("name_j").alias("paired_hotel_name"),
        F.col("is_matched").alias("match_status"),
        F.col("scoring_metadata").alias("scoring_metadata")
    )).alias("paired_with"),
    F.count("*").alias("total_paired_hotels")
).select(
    F.col("name_i").alias("hotel_name"),
    F.col("total_paired_hotels"),
    F.col("paired_with")
)

# ============================================================================
# STEP 2: Extract condition pass/fail details for matched vs unmatched
# ============================================================================

# Get all condition columns (those ending with "_passed")
condition_columns = [col for col in scored_pair_df.columns if col.endswith("_passed")]

# Create a condition summary for each pair
condition_summary = scored_pair_df.select(
    F.col("name_i"),
    F.col("name_j"),
    F.col("is_matched"),
    F.col("match_status"),
    F.struct(
        *[F.col(col).alias(col.replace("_passed", "")) for col in condition_columns]
    ).alias("condition_results")
)

# Aggregate conditions by hotel name_i
conditions_by_hotel = condition_summary.groupBy("name_i").agg(
    F.collect_list(F.struct(
        F.col("name_j").alias("paired_hotel_name"),
        F.col("is_matched").alias("match_status"),
        F.col("match_status").alias("match_reason"),
        F.col("condition_results").alias("condition_results")
    )).alias("pairing_conditions")
).select(
    F.col("name_i").alias("hotel_name"),
    F.col("pairing_conditions")
)

# ============================================================================
# STEP 3: Join pairing details with conditions
# ============================================================================

hotel_insights = pairing_details.join(
    conditions_by_hotel,
    on="hotel_name",
    how="left"
).select(
    F.col("hotel_name"),
    F.col("total_paired_hotels").alias("num_paired_hotels"),
    F.col("pairing_conditions").alias("pairing_details")
)

# ============================================================================
# STEP 4: Join with Final Cluster
# ============================================================================

final_cluster_with_insights = final_cluster_df.join(
    hotel_insights,
    on=F.col("name") == F.col("hotel_name"),
    how="left"
).drop("hotel_name")

# Reorder columns - Final Cluster schema first, then insights
final_cluster_columns = final_cluster_df.columns
final_cluster_with_insights = final_cluster_with_insights.select(
    *final_cluster_columns,
    F.col("num_paired_hotels"),
    F.col("pairing_details")
)

# ============================================================================
# STEP 5: Display results
# ============================================================================

print("=" * 100)
print("ENRICHED FINAL CLUSTER WITH PAIRING INSIGHTS")
print("=" * 100)
final_cluster_with_insights.show(truncate=False)

print("\n" + "=" * 100)
print("SCHEMA OF ENRICHED FINAL CLUSTER")
print("=" * 100)
final_cluster_with_insights.printSchema()

# ============================================================================
# Optional: Save the enriched table
# ============================================================================
# final_cluster_with_insights.write.mode("overwrite").parquet("path/to/output/final_cluster_enriched")

# ============================================================================
# Optional: Flatten the insights for easier viewing
# ============================================================================

print("\n" + "=" * 100)
print("SAMPLE INSIGHTS (Expanded View)")
print("=" * 100)

# Explode the pairing_details to see each pair separately
sample_view = final_cluster_with_insights.select(
    F.col("name"),
    F.col("num_paired_hotels"),
    F.explode(F.col("pairing_details")).alias("pair_info")
).select(
    F.col("name").alias("hotel_name"),
    F.col("num_paired_hotels"),
    F.col("pair_info.paired_hotel_name").alias("paired_with_hotel"),
    F.col("pair_info.match_status").alias("is_matched"),
    F.col("pair_info.match_reason").alias("match_reason"),
    F.col("pair_info.condition_results").alias("conditions")
)

sample_view.show(truncate=False)

final_cluster_with_insights.printSchema()

writer.write(final_cluster_with_insights, "final_cluster_insights")