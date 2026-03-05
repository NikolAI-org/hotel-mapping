import json
import os
import sys
import functools
# Ensure we can import custom modules (Standard Airflow setup)
sys.path.append('/opt/airflow')
from pyspark.sql import SparkSession, Window
# import pyspark.sql.functions as F
from pyspark.sql import functions as F
from pyspark.sql import Window

from spark.jobs.cluster.entity_resolution_pipeline import EntityResolutionPipeline



from hotel_data.config.paths import (
    BASE_DELTA_PATH, 
    CATALOG_NAME, 
    SCHEMA_NAME, 
    TABLE_HOTELS_NAME, 
    TABLE_HOTELS_PAIRS_NAME
)
from hotel_data.delta.delta_table_manager import DeltaTableManager

# We assume you have or will define a constant for the final clusters table
TABLE_FINAL_CLUSTERS_NAME = "final_clusters" 



def main():
    spark = SparkSession.builder.appName("HotelEntityResolution").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # These would typically come from Airflow DAG parameters
    current_provider = os.getenv('PROVIDER_NAME', 'hbose') 
    TABLE_HISTORY_NAME = "comparison_audit_log"
    transitivity = os.getenv("TRANSITIVITY", "true").lower() == "true"

    config = {
        'weights': json.loads(os.getenv('WEIGHTS', '{"name_score": 0.7, "addr_score": 0.3}')),
        't_high': float(os.getenv('THRESHOLD_HIGH', 0.85)),
        't_low': float(os.getenv('THRESHOLD_LOW', 0.80)),
        'transitivity': transitivity,
    }
    
    match_logic = json.loads(os.getenv('MATCH_LOGIC', '{}'))

    manager = DeltaTableManager(spark, CATALOG_NAME, SCHEMA_NAME, BASE_DELTA_PATH)
    pipeline = EntityResolutionPipeline(spark, config, match_logic)
    
    pipeline.run(
        manager=manager, 
        table_hotels=TABLE_HOTELS_NAME, 
        table_pairs=TABLE_HOTELS_PAIRS_NAME, 
        table_output=TABLE_FINAL_CLUSTERS_NAME,
        table_history=TABLE_HISTORY_NAME,
        current_provider=current_provider
    )

if __name__ == "__main__":
    main()