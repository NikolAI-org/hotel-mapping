import json
import os

import yaml
from pyspark.sql import SparkSession

from hotel_data.config.paths import (
    BASE_DELTA_PATH,
    CATALOG_NAME,
    SCHEMA_NAME,
    TABLE_HOTELS_NAME,
    TABLE_HOTELS_PAIRS_NAME,
)
from hotel_data.delta.delta_table_manager import DeltaTableManager
from spark.jobs.cluster.engine import (
    CanonicalIdGenerator,
    ClusterCohesionValidator,
    DynamicVetoRule,
    MatchLogicEvaluator,
    RoutingDecisionEngine,
    VetoEngine,
)
from spark.jobs.cluster.entity_resolution_pipeline import EntityResolutionPipeline
from spark.jobs.cluster.pair_scorer import PairScorer
from spark.jobs.cluster.strategies import NonTransitiveStrategy, TransitiveStrategy


def _load_config() -> dict:
    """Load config.yaml relative to the hotel_data package root."""
    import hotel_data
    config_path = os.path.join(os.path.dirname(hotel_data.__file__), "config", "config.yaml")
    with open(config_path) as fh:
        return yaml.safe_load(fh)


def main():
    spark = SparkSession.builder.appName("HotelEntityResolution").getOrCreate()

    cfg = _load_config()
    current_provider = os.getenv("PROVIDER_NAME", "hbose")
    transitivity = cfg["clustering"]["transitivity"]

    # --- DEFINE THE 3-TIER TABLES ---
    TABLE_REGISTRY_NAME = "canonical_registry"
    TABLE_MAPPINGS_NAME = "cluster_mappings"
    TABLE_AUDIT_NAME = "comparison_audit_log"

    id_generator = CanonicalIdGenerator()

    cluster_cfg = cfg["clustering"]

    # All clustering params from config.yaml — single source of truth
    match_logic_config = cfg["scoring"]["match_logic"]
    match_evaluator = MatchLogicEvaluator(match_logic_config)
    required_providers = cluster_cfg["required_providers"]

    # Veto rules
    veto_rules = []
    for veto_cfg in cluster_cfg.get("veto_rules", []):
        veto_rules.append(DynamicVetoRule(
            rule_name=veto_cfg["veto_name"], logic_config=veto_cfg["logic"]
        ))
    veto_engine = VetoEngine(veto_rules)

    # --- STRATEGY FACTORY ---
    if transitivity:
        print("MODE: Transitive (Graph / Union-Find)")
        strategy = TransitiveStrategy(spark, id_generator, match_evaluator.evaluate)
    else:
        print("MODE: Non-Transitive (Hub-and-Spoke)")
        scorer = PairScorer(
            cluster_cfg["weights"],
            cluster_cfg["threshold_high"],
            cluster_cfg["threshold_low"],
        )
        router = RoutingDecisionEngine(
            match_threshold=cluster_cfg["threshold_high"],
            conflict_margin=cluster_cfg["conflict_margin"],
        )
        cohesion_validator = ClusterCohesionValidator(
            required_providers=required_providers
        )
        strategy = NonTransitiveStrategy(
            veto_engine,
            router,
            id_generator,
            scorer,
            match_evaluator.evaluate,
            cohesion_validator,
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
        current_provider=current_provider,
    )


if __name__ == "__main__":
    main()
