# score_compute_pipeline.py
from pathlib import Path
from pyspark.sql import SparkSession

from hotel_data.config.paths import CATALOG_NAME, DERBY_HOME, SCHEMA_NAME, BASE_DELTA_PATH, TABLE_HOTELS_PAIRS, \
    TABLE_HOTELS_PAIRS_NAME, WAREHOUSE_DIR
from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.schema.delta.hotel_pairs import hotel_pairs_schema
from hotel_data.pipeline.preprocessor.processors.hotel_pair_scorer_processor import HotelPairScorerProcessor
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import functions as F


def main():
    # spark = (
    #     SparkSession.builder.appName("ScoreComputePipeline")
    #     .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.1")
    #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    #     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    #     .config("spark.executor.memory", "8g")
    #     .config("spark.driver.memory", "4g")
    #     .getOrCreate()
    # )
    Path(DERBY_HOME).mkdir(parents=True, exist_ok=True)
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

    spark.sparkContext.setLogLevel("WARN")

    manager = DeltaTableManager(
        spark=spark,
        catalog_name=CATALOG_NAME,
        schema_name=SCHEMA_NAME,
        base_path=BASE_DELTA_PATH,
    )

    # Read bronze hotels table
    hotels_df = manager.read_table("hotels")

    # Make sure geoHash is Array[String], nullable
    hotels_df = hotels_df.withColumn(
        "geoHash",
        F.col("geoHash").cast(ArrayType(StringType(), containsNull=True))
    )

    # Apply scoring processor
    pair_scorer = HotelPairScorerProcessor()
    scored_pairs_df = pair_scorer.process(hotels_df)
    # scored_pairs_df = scored_pairs_df.select(
    #     F.col("id_i").alias("providerHotelId1"),
    #     F.col("id_j").alias("providerHotelId2"),
    #     F.col("h1.name").alias("name1"),  # or normalized_name if needed
    #     F.col("h2.name").alias("name2"),
    #     "city_score",
    #     "rating_score",
    #     "name_score",
    #     "total_score",
    #     F.col("geoHash").cast(ArrayType(StringType(), True))
    # )
    # scored_pairs_df = scored_pairs_df.select(
    #     "id_i",
    #     "id_j",
    #     F.col("geoHash").cast(ArrayType(StringType(), True)).alias("geoHash"),
    #     "city_score",
    #     "rating_score",
    #     "name_score",
    #     "total_score"
    # )

    empty_df = spark.createDataFrame([], schema=hotel_pairs_schema)
    manager.create_table(TABLE_HOTELS_PAIRS_NAME, empty_df, comment="Hotel pairwise scores")

    # Write scored pairs
    manager.write_table(table_name=TABLE_HOTELS_PAIRS_NAME, df=scored_pairs_df, merge_schema="false", overwrite_schema="false")
    print(f"✅ Written {scored_pairs_df.count()} rows to {TABLE_HOTELS_PAIRS}")

    spark.stop()

if __name__ == "__main__":
    main()
