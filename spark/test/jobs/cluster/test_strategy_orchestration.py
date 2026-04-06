from unittest.mock import MagicMock
from pyspark.sql import functions as F
from spark.jobs.cluster.strategies import NonTransitiveStrategy
from spark.jobs.cluster.engine import CanonicalIdGenerator

def test_orphaned_singletons_caught_by_strategy(spark):
    # 1. Arrange: Input Data
    hotels_df = spark.createDataFrame([
        ("h1", "US", "NY", "New York"), 
        ("h2", "IN", "MH", "Mumbai")
    ], ["uid", "contact_address_country_code", "contact_address_state_name", "contact_address_city_name"])
    
    pairs_df = spark.createDataFrame([("h1", "h2")], ["uid_i", "uid_j"])

    # 2. Arrange: Mocks
    mock_scorer = MagicMock()
    mock_evaluator = MagicMock()
    mock_veto = MagicMock()
    mock_router = MagicMock()
    mock_validator = MagicMock()
    
    # Simulate the Router returning a decision ONLY for h1. h2 is missing from uid_i!
    # Providing the strict schema prevents the "CANNOT_DETERMINE_TYPE" PySpark error
    mock_router.route.return_value = spark.createDataFrame([
        ("h1", "h2", "ATTACH_TO_CANONICAL", 0.95, None)
    ], schema="uid_i string, uid_j string, routing_decision string, composite_score double, veto_reason string")
    
    mock_validator.validate.return_value = mock_router.route.return_value.withColumn("assigned_canonical_id", F.col("uid_j"))
    
    real_id_gen = CanonicalIdGenerator()

    strategy = NonTransitiveStrategy(
        mock_veto, mock_router, real_id_gen, mock_scorer, mock_evaluator, mock_validator
    )

    # 3. Act
    result = strategy.cluster(new_hotels_df=hotels_df, pairs_df=pairs_df)
    
    # 4. Assert
    audit_results = {row['uid_i']: row.asDict() for row in result.audit_decisions.collect()}

    # Verify h2 (The Orphaned Singleton) was caught and generated its Hub ID
    assert "h2" in audit_results, "Anti-Join failed: h2 was dropped from the pipeline!"
    h2_generated_id = audit_results["h2"]["assigned_canonical_id"]
    assert h2_generated_id.startswith("IN-MUMBAI-MH-")

    # Verify h1 attached correctly AND inherited h2's Hub ID! (Cold Start Fix)
    assert audit_results["h1"]["routing_decision"] == "ATTACH_TO_CANONICAL"
    assert audit_results["h1"]["assigned_canonical_id"] == h2_generated_id