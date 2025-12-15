from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from hotel_data.config.paths import (
    BASE_DELTA_PATH,
    SCHEMA_NAME,
    TABLE_HOTELS_NAME,
    TABLE_HOTELS_PAIRS_NAME,
    WAREHOUSE_DIR,
    DERBY_HOME,
    CATALOG_NAME,
)
from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.schema.delta.hotel_bronze import flattened_hotel_schema

Path(DERBY_HOME).mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder.appName("HotelsPipelineRead")
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
    # ---- SAME Hive metastore as writer ----
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
    .enableHiveSupport()
    .getOrCreate()
)

print("current_catalog:", spark.catalog.currentCatalog())
print("current_database:", spark.catalog.currentDatabase())

# Option A: catalog-style SQL reads (production-like)
# spark.sql("SHOW DATABASES").show(truncate=False)
# spark.sql(f"SHOW TABLES IN {SCHEMA_NAME}").show(truncate=False)

# spark.sql(f"SELECT * FROM {SCHEMA_NAME}.{TABLE_HOTELS_NAME}").show(5, truncate=False)
# spark.sql(f"DESCRIBE TABLE {SCHEMA_NAME}.{TABLE_HOTELS_NAME}").show(truncate=False)

# Option B: use DeltaTableManager
manager = DeltaTableManager(
    spark=spark,
    catalog_name=CATALOG_NAME,
    schema_name=SCHEMA_NAME,
    base_path=BASE_DELTA_PATH,
)

# hotel_pair = manager.read_table(TABLE_HOTELS_PAIRS_NAME)
# hotel_pair.show(5, truncate=False)


# Optional fallback: direct path read (escape hatch)
df = spark.read.format("delta").load(
    f"{BASE_DELTA_PATH}/{SCHEMA_NAME}/02_scored_pairs"
)
# df.select(F.col("address_sbert_score")).distinct().show()
col1 = "normalized_name_score_sbert"
col2 = "name_score_sbert"
df.agg(
    F.min(col1).alias(f"min_{col1}"),
    F.max(col1).alias(f"max_{col1}"),
    F.avg(col1).alias(f"avg_{col1}"),
    F.min(col2).alias(f"min_{col2}"),
    F.max(col2).alias(f"max_{col2}"),
    F.avg(col2).alias(f"avg_{col2}"),
).show(truncate=False)
# df.show(truncate=False,n=20)
# df.filter((F.col("id_j") == "39698858") & (F.col("id_i") == "39698858")).select("name_i", "name_j").show()

# df.printSchema()
# df = df.filter(F.col("id") == "39698858")
# df.select("id", "name", "cluster_id", "providerId", "geoCode_lat",
#         "geoCode_long",
#         "geohash").show(truncate=False)
# print(
#     df.select("name", "cluster_id", "id")
#       .limit(1)
#       .toJSON()
#       .collect()[0]
# )




# selected_names = [
#     "lemon tree premier mumbai international airport",
#     "lemon tree premier, mumbai international airport"
# ]

# pairs_df = (
#     df
#     .select(
#         "id",
#         "name",
#         "geoCode_lat",
#         "geoCode_long",
#         "geohash",
#         "providerId",
#         F.explode("pairing_details").alias("pair")
#     )
#     .filter(F.col("pair.paired_hotel_name").isin(selected_names))
#     .select(
#         "id",
#         "name",
#         "geoCode_lat",
#         "geoCode_long",
#         "geohash",
#         "providerId",
#         F.col("pair.paired_hotel_name").alias("paired_hotel_name"),
#         F.col("pair.match_status").alias("match_status"),
#         F.col("pair.match_reason").alias("match_reason"),
#         F.col("pair.condition_results").alias("condition_results")
#     )
# )
# conditions_df = pairs_df.select(
#     "id",
#     "name",
#     "paired_hotel_name",
#     "match_status",
#     "condition_results.geo_distance_km",
#     "condition_results.name_score_jaccard_lcs",
#     "condition_results.normalized_name_score_sbert",
#     "condition_results.star_ratings_score",
#     "condition_results.address_line1_score",
#     "condition_results.postal_code_match",
#     "condition_results.country_match",
#     "condition_results.address_sbert_score",
#     "condition_results.phone_match_score",
#     "condition_results.email_match_score",
#     "condition_results.fax_match_score"
# )

# conditions_df.show(truncate=False)



# # Unique count of id and cluster id
# result_df = df.agg(
#     F.countDistinct("id").alias("unique_id_count"),
#     F.countDistinct("cluster_id").alias("unique_cluster_id_count")
# )

# result_df.show(truncate=False)

# # # Fetch count of same id
# id_counts_df = (
#     df.select("id", "name", "cluster_id", "combined_address").groupBy("id")
#       .agg(F.count("*").alias("record_count"))
#       .filter(F.col("record_count") > 1)
#       .orderBy(F.col("record_count").desc())
# )
# id_counts_df.show()

# # Fetch count of same cluster id
# cluster_id_counts_df = (
#     df.select("id", "name", "cluster_id", "combined_address").groupBy("cluster_id")
#       .agg(F.count("*").alias("record_count"))
#       .filter(F.col("record_count") > 1)
#       .orderBy(F.col("record_count").desc())
# )
# cluster_id_counts_df.show()

# # # Filter the records with id 
# filtered_df = df.filter(F.col("id") == "39698858").select("name", "id", "cluster_id","geoCode_lat", "geoCode_long", "combined_address")
# filtered_df.show(truncate=False)
