from spark.jobs.cluster.engine import RoutingDecisionEngine

def test_routing_scenarios(spark):
    # Setup Router: High threshold = 0.85, Conflict margin = 0.05
    router = RoutingDecisionEngine(match_threshold=0.85, conflict_margin=0.05)

    # 1. Arrange Scenario Data (Added 'is_matched' column)
    data = [
        # SCENARIO A1: Safe Attach (Clear Winner)
        ("uid_1", "uid_A", 0.95, False, None, True),
        ("uid_1", "uid_B", 0.70, False, None, True),

        # SCENARIO A3: Ambiguous Conflict (Margin < 0.05)
        ("uid_2", "uid_C", 0.92, False, None, True),
        ("uid_2", "uid_D", 0.90, False, None, True),

        # SCENARIO E1/VETO: Record hit a hard rule
        ("uid_3", "uid_E", 0.00, True, "VETO_DUAL_BRAND_TRAP", True),

        # SCENARIO A2 Fallback: Best match is below threshold
        ("uid_4", "uid_F", 0.82, False, None, True),

        # NEW SCENARIO: Failed Strict Boolean Logic (Score is high, but boolean says False)
        ("uid_5", "uid_G", 0.99, False, None, False)
    ]
    
    columns = ["uid_i", "uid_j", "composite_score", "is_vetoed", "veto_reason", "is_matched"]
    scored_pairs_df = spark.createDataFrame(data, columns)

    # 2. Act
    routed_df = router.route(scored_pairs_df)
    results = {row['uid_i']: row['routing_decision'] for row in routed_df.collect()}

    # 3. Assert Decisions
    assert results["uid_1"] == "ATTACH_TO_CANONICAL", "A1 failed: Should safely attach"
    assert results["uid_2"] == "CONFLICT_AMBIGUOUS", "A3 failed: Should trigger conflict margin"
    assert results["uid_3"] == "CONFLICT_VETOED", "Veto failed: Should respect veto status"
    assert results["uid_4"] == "CREATE_NEW_SINGLETON", "A2 failed: Sub-threshold should be singletons"
    
    # NEW ASSERTION: Boolean logic override
    assert results["uid_5"] == "FAILED_BOOLEAN_LOGIC", "Boolean failed: Should reject even high scores if is_matched is False"

    # 4. Assert Data Retention
    assert routed_df.count() == 5