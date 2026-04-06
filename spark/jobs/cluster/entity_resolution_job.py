import os
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from hotel_data.delta.delta_table_manager import DeltaTableManager
from spark.jobs.cluster.engine import CanonicalIdGenerator, ClusterCohesionValidator, DualBrandVeto, MatchLogicEvaluator, MissingGeoTiebreakerVeto, RoutingDecisionEngine, VetoEngine
from spark.jobs.cluster.entity_resolution_pipeline import EntityResolutionPipeline
from spark.jobs.cluster.pair_scorer import PairScorer
from spark.jobs.cluster.strategies import TransitiveStrategy, NonTransitiveStrategy
from hotel_data.config.paths import (
    BASE_DELTA_PATH, 
    CATALOG_NAME, 
    SCHEMA_NAME, 
    TABLE_HOTELS_NAME, 
    TABLE_HOTELS_PAIRS_NAME
)

def main():
    spark = SparkSession.builder.appName("HotelEntityResolution").getOrCreate()
    
    current_provider = os.getenv('PROVIDER_NAME', 'hbose') 
    transitivity = os.getenv("TRANSITIVITY", "true").lower() == "true"
    
    # --- DEFINE THE 3-TIER TABLES ---
    TABLE_REGISTRY_NAME = "canonical_registry"
    TABLE_MAPPINGS_NAME = "cluster_mappings"
    TABLE_AUDIT_NAME = "comparison_audit_log"

    id_generator = CanonicalIdGenerator()
    
    # 1. Extract the JSON configuration
    match_logic_config = json.loads(os.getenv('MATCH_LOGIC', '{}'))
    match_evaluator = MatchLogicEvaluator(match_logic_config)
    required_providers = json.loads(os.getenv('REQUIRED_PROVIDERS', '["ean"]'))

    # --- STRATEGY FACTORY ---
    if transitivity:
        print("🚀 MODE: Transitive (Graph / Union-Find)")
        # def match_evaluator_func(pairs_df):
        #     # Your YAML matching logic goes here
        #     return pairs_df.withColumn("is_matched", F.lit(True)) 
            
        strategy = TransitiveStrategy(spark, id_generator, match_evaluator.evaluate)
    else:
        print("🎯 MODE: Non-Transitive (Hub-and-Spoke / 0-FP)")
        config = {
            'weights': json.loads(os.getenv('WEIGHTS', '{}')),
            't_high': float(os.getenv('THRESHOLD_HIGH', 0.85)),
        }
        scorer = PairScorer(config['weights'], config['t_high'], 0.80)
        veto_engine = VetoEngine([DualBrandVeto(), MissingGeoTiebreakerVeto()])
        margin = float(os.getenv('CONFLICT_MARGIN', 0.05))
        router = RoutingDecisionEngine(match_threshold=config['t_high'], conflict_margin=margin)
        cohesion_validator = ClusterCohesionValidator(required_providers=required_providers)
        
        # 2. Pass the evaluate method
        # strategy = NonTransitiveStrategy(veto_engine, router, id_generator, scorer, match_evaluator.evaluate)
        strategy = NonTransitiveStrategy(
            veto_engine, router, id_generator, scorer, match_evaluator.evaluate, cohesion_validator
        )

    # --- RUN PIPELINE ---
    pipeline = EntityResolutionPipeline(spark, strategy, id_generator)
    manager = DeltaTableManager(spark, CATALOG_NAME, SCHEMA_NAME, BASE_DELTA_PATH)
    
    # FIX: Pass all required arguments explicitly by name
    pipeline.run(
        manager=manager, 
        table_hotels=TABLE_HOTELS_NAME, 
        table_pairs=TABLE_HOTELS_PAIRS_NAME, 
        table_registry=TABLE_REGISTRY_NAME,
        table_mappings=TABLE_MAPPINGS_NAME,
        table_audit=TABLE_AUDIT_NAME,
        current_provider=current_provider
    )

if __name__ == "__main__":
    main()
