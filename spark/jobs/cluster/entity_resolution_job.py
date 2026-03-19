from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.config.paths import (
    BASE_DELTA_PATH,
    CATALOG_NAME,
    SCHEMA_NAME,
    TABLE_HOTELS_NAME,
    TABLE_HOTELS_PAIRS_NAME,
)
import hotel_data.pipeline.scoring.scorers.overall_pair_scorer as overall_pair_scorer
from spark.jobs.cluster.entity_resolution_pipeline import EntityResolutionPipeline
from pyspark.sql import SparkSession
import json
import os
import sys

# This must be first line before any hotel data import
sys.path.append("/opt/airflow")
# import pyspark.sql.functions as F


# We assume you have or will define a constant for the final clusters table
TABLE_FINAL_CLUSTERS_NAME = "final_clusters"


def main():
    spark = SparkSession.builder.appName("HotelEntityResolution").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # These would typically come from Airflow DAG parameters
    current_provider = os.getenv("PROVIDER_NAME", "hbose")
    print(f"🔎 EntityResolution provider: {current_provider}")
    TABLE_HISTORY_NAME = "comparison_audit_log"
    transitivity = os.getenv("TRANSITIVITY", "true").lower() == "true"

    config = {
        "weights": json.loads(
            os.getenv(
                "WEIGHTS", '{"average_name_score": 0.7, "address_line1_score": 0.3}'
            )
        ),
        "t_high": float(os.getenv("THRESHOLD_HIGH", 0.85)),
        "t_low": float(os.getenv("THRESHOLD_LOW", 0.80)),
        "transitivity": transitivity,
    }

    match_logic = json.loads(os.getenv("MATCH_LOGIC", "{}"))
    print(f"⚙️ Loaded match logic1 : {json.dumps(match_logic, indent=2)}")

    match_logic = overall_pair_scorer._load_match_logic()
    print(f"⚙️ Loaded match logic2 : {json.dumps(match_logic, indent=2)}")

    manager = DeltaTableManager(spark, CATALOG_NAME, SCHEMA_NAME, BASE_DELTA_PATH)
    pipeline = EntityResolutionPipeline(spark, config, match_logic)

    pipeline.run(
        manager=manager,
        table_hotels=TABLE_HOTELS_NAME,
        table_pairs=TABLE_HOTELS_PAIRS_NAME,
        table_output=TABLE_FINAL_CLUSTERS_NAME,
        table_history=TABLE_HISTORY_NAME,
        current_provider=current_provider,
    )


if __name__ == "__main__":
    main()
