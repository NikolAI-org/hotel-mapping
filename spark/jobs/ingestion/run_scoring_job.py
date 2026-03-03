from hotel_data.schema.delta.hotel_pairs import hotel_pairs_schema
from pyspark.sql.types import ArrayType, StringType
from hotel_data.pipeline.preprocessor.processors.hotel_pair_scorer_processor import HotelPairScorerProcessor
from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.config.paths import BASE_DELTA_PATH, CATALOG_NAME, SCHEMA_NAME, TABLE_HOTELS_PAIRS_NAME
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# from hotel_data.pipeline.scoring.scorers.candidate_scorer import CandidateScorer

# Ensure we can import custom modules
sys.path.append('/opt/airflow')


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def run_job():
    # 1. Parse Arguments (Decoupling logic from code)
    parser = argparse.ArgumentParser()
    parser.add_argument("--supplier", required=True, help="Provider Name")
    args, unknown = parser.parse_known_args()
    provider_name = args.supplier

    spark = create_spark_session("HotelDataScoring")
    spark.sparkContext.setLogLevel("WARN")

    print(f"Scoring for: {provider_name}")

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

    challenger_df = hotels_df.filter(F.col("providerName") == provider_name)
    anchor_df = hotels_df  # All hotels are potential anchors

    challenger_count = challenger_df.count()
    anchor_count = anchor_df.count()
    print(f"Challenger: {challenger_count}, Anchor: {anchor_count}")

    # Initialize scorer with geo-only blocking
    # Geo blocker uses H3 resolution 8 (~460m, 0.74 km² per hexagon)
    pair_scorer = HotelPairScorerProcessor(
        use_geo_blocker=True
    )

    # Score pairs
    scored_pairs_df = pair_scorer.process(challenger_df, anchor_df)

    # Filter: Keep only pairs with strong name similarity (≥50%)
    filtered_pairs_df = scored_pairs_df.filter(
        F.col("average_name_score") >= 0.5
    )

    original_count = scored_pairs_df.count()
    pair_count = filtered_pairs_df.count()

    print(
        f"Generated {original_count} pairs, filtered to {pair_count} pairs (name_score >= 0.5)")
    print(f"   Reduction: {100 * (1 - pair_count/original_count):.1f}%")

    # Create table and merge results (upsert based on pair identifiers)
    empty_df = spark.createDataFrame([], schema=hotel_pairs_schema)
    manager.create_table(TABLE_HOTELS_PAIRS_NAME, empty_df,
                         comment="Hotel pairwise scores")

    if pair_count > 0:
        print(
            f"Merging {pair_count} pairs to Delta (upsert on providerHotelId_i, providerHotelId_j)...")
        manager.merge_table(
            table_name=TABLE_HOTELS_PAIRS_NAME,
            df=filtered_pairs_df,
            key_columns=["providerHotelId_i", "providerHotelId_j"]
        )
        print(f"✅ Merged {pair_count} pairs (existing updated, new inserted)")
    else:
        print("⚠️  No pairs generated")

    spark.stop()


if __name__ == "__main__":
    run_job()
