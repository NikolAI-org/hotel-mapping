import sys
import argparse

# Trick to stop auto-formatters from moving this below the hotel_data imports
# fmt: off
sys.path.append('/opt/airflow')
# fmt: on

from pyspark.sql.types import ArrayType, StringType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from hotel_data.pipeline.preprocessor.processors.hotel_pair_scorer_processor import HotelPairScorerProcessor
from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.config.paths import BASE_DELTA_PATH, CATALOG_NAME, SCHEMA_NAME, TABLE_HOTELS_PAIRS_NAME


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.shuffle.compress", "true")
        .config("spark.shuffle.spill.compress", "true")
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
    # anchor_df = hotels_df.filter(F.col("providerName") != provider_name)
    # The Anchor is now the ENTIRE table.
    anchor_df = hotels_df

    pair_scorer = HotelPairScorerProcessor()
    # pair_scorer = CandidateScorer()
    scored_pairs_df = pair_scorer.process(challenger_df, anchor_df)

    # coalesce avoids a full shuffle stage (unlike repartition), preventing
    # BypassMergeSortShuffleWriter from filling /tmp/spark-tmp with intermediate files.
    scored_pairs_df = scored_pairs_df.coalesce(10)

    print(f"--- Upserting into Delta Table: {TABLE_HOTELS_PAIRS_NAME} ---")
    scored_pairs_df = scored_pairs_df.filter(
        F.col("providerHotelId_i").isNotNull() & F.col(
            "providerHotelId_j").isNotNull()
    )

    # Build order-independent pair keys so (A,B) and (B,A) merge into one record.
    # dropDuplicates is intentionally omitted here: the scorer already guarantees
    # unique pairs via the (uid_i < uid_j) filter for intra-provider pairs and the
    # single-provider challenger design for inter-provider pairs. Adding dropDuplicates
    # triggered a full wide-schema shuffle (Stage 25) that exhausted /tmp/spark-tmp.
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
    ).drop("pair_key_i", "pair_key_j")

    manager.merge_table(
        table_name=TABLE_HOTELS_PAIRS_NAME,
        df=scored_pairs_df,
        key_columns=["pair_key_left", "pair_key_right"],
    )

    # Read the count from the Delta table — avoids re-running the entire scoring
    # pipeline a second time (scored_pairs_df is lazy and not cached).
    written_count = manager.read_table(TABLE_HOTELS_PAIRS_NAME).count()
    print(f"✅ Written {written_count} rows to {TABLE_HOTELS_PAIRS_NAME}")
    spark.stop()


if __name__ == "__main__":
    run_job()
