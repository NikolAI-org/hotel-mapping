import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#from hotel_data.pipeline.scoring.scorers.candidate_scorer import CandidateScorer

# Ensure we can import custom modules
sys.path.append('/opt/airflow')

from hotel_data.config.paths import BASE_DELTA_PATH, CATALOG_NAME, SCHEMA_NAME, TABLE_HOTELS_PAIRS_NAME
from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.pipeline.preprocessor.processors.hotel_pair_scorer_processor import HotelPairScorerProcessor
from pyspark.sql.types import ArrayType, StringType

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

    print(f"--- Starting Scoring for: {provider_name} ---")

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
    #anchor_df = hotels_df.filter(F.col("providerName") != provider_name)
    #The Anchor is now the ENTIRE table.
    anchor_df = hotels_df

    pair_scorer = HotelPairScorerProcessor()
    #pair_scorer = CandidateScorer()
    scored_pairs_df = pair_scorer.process(challenger_df, anchor_df)

    scored_pairs_df = scored_pairs_df.repartition(100)

    print(f"--- Upserting into Delta Table: {TABLE_HOTELS_PAIRS_NAME} ---")
    scored_pairs_df = scored_pairs_df.filter(
        F.col("providerHotelId_i").isNotNull() & F.col("providerHotelId_j").isNotNull()
    )

    # Build order-independent pair keys so (A,B) and (B,A) merge into one record.
    scored_pairs_df = scored_pairs_df.withColumn(
        "pair_key_i",
        F.concat_ws(":", F.col("providerName_i"), F.col("providerHotelId_i"))
    ).withColumn(
        "pair_key_j",
        F.concat_ws(":", F.col("providerName_j"), F.col("providerHotelId_j"))
    ).withColumn(
        "pair_key_left",
        F.least(F.col("pair_key_i"), F.col("pair_key_j"))
    ).withColumn(
        "pair_key_right",
        F.greatest(F.col("pair_key_i"), F.col("pair_key_j"))
    ).dropDuplicates(["pair_key_left", "pair_key_right"]).drop("pair_key_i", "pair_key_j")

    manager.merge_table(
        table_name=TABLE_HOTELS_PAIRS_NAME,
        df=scored_pairs_df,
        key_columns=["pair_key_left", "pair_key_right"],
    )

    print(f"✅ Written {scored_pairs_df.count()} rows to {TABLE_HOTELS_PAIRS_NAME}")
    spark.stop()


if __name__ == "__main__":
    run_job()