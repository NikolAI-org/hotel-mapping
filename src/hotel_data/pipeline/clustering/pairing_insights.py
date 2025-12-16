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
        .config("spark.hadoop.fs.s3a.endpoint", "http://172.16.16.152:9000")
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


metric_schema = StructType([
    StructField("actual", DoubleType(), True),
    StructField("threshold", DoubleType(), True),
    StructField("comparator", StringType(), True),
    StructField("passed", BooleanType(), True)
])

# 2. DEFINE THE FULL METADATA SCHEMA
# This maps each key specifically so Spark preserves the names
full_metadata_schema = StructType([
    StructField("geo_distance_km", metric_schema, True),
    StructField("name_score_jaccard_lcs", metric_schema, True),
    StructField("normalized_name_score_sbert", metric_schema, True),
    StructField("star_ratings_score", metric_schema, True),
    StructField("address_line1_score", metric_schema, True),
    StructField("postal_code_match", metric_schema, True),
    StructField("country_match", metric_schema, True),
    StructField("address_sbert_score", metric_schema, True),
    StructField("phone_match_score", metric_schema, True),
    StructField("email_match_score", metric_schema, True),
    StructField("fax_match_score", metric_schema, True)
])

# ============================================================================
# STEP 1 & 2: Parse and Aggregate
# ============================================================================

# Convert the string column 'scoring_metadata' into a structured object
pairing_summary = scored_pair_df.select(
    F.col("name_i").alias("hotel_name"),
    F.struct(
        F.col("name_j").alias("paired_hotel_name"),
        F.col("is_matched"),
        F.col("match_status").alias("match_reason"),
        # Use from_json with the full schema to keep all keys/labels
        F.from_json(F.col("scoring_metadata"), full_metadata_schema).alias("scoring_metrics")
    ).alias("pair_info")
)

# Aggregate into the final structure
hotel_insights = pairing_summary.groupBy("hotel_name").agg(
    F.count("*").alias("num_paired_hotels"),
    F.collect_list("pair_info").alias("pairing_details")
)

# ============================================================================
# STEP 3: Join and Reorder
# ============================================================================

final_cluster_with_insights = final_cluster_df.join(
    hotel_insights,
    on=final_cluster_df["name"] == hotel_insights["hotel_name"],
    how="left"
).drop("hotel_name")

# Reorder columns: Cluster Info first, then Insights
final_cluster_with_insights = final_cluster_with_insights.select(
    *final_cluster_df.columns,
    "num_paired_hotels",
    "pairing_details"
)

# Display the structured schema to confirm keys are present
final_cluster_with_insights.printSchema()

# ============================================================================
# STEP 4: Display & Sample View
# ============================================================================

print("\n" + "=" * 100)
print("SAMPLE INSIGHTS - DOT NOTATION VERIFIED")
print("=" * 100)

sample_view = final_cluster_with_insights.select(
    F.col("name"),
    F.explode(F.col("pairing_details")).alias("pair")
).select(
    F.col("name").alias("hotel"),
    F.col("pair.paired_hotel_name").alias("paired_with"),
    F.col("pair.is_matched"),
    # Accessing specifically named field 'scoring_metrics'
    F.col("pair.scoring_metrics.geo_distance_km.actual").alias("dist_km"),
    F.col("pair.scoring_metrics.name_score_jaccard_lcs.actual").alias("name_score"),
    F.col("pair.scoring_metrics.address_line1_score.passed").alias("addr_passed")
)

sample_view.show(truncate=False)

# Write the result
writer.write(final_cluster_with_insights, "final_cluster_insights")