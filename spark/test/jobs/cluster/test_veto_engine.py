from spark.jobs.cluster.engine import VetoEngine, DynamicVetoRule

def test_dynamic_veto_logic(spark):
    # 1. Arrange: Define JSON configurations mimicking your dynamic veto rules
    dual_brand_config = {
        "operator": "AND",
        "rules": [
            {"signal": "geo_distance_km", "comparator": "lt", "threshold": 0.05},
            {"signal": "name_score_jaccard", "comparator": "lt", "threshold": 0.4}
        ]
    }
    
    missing_geo_config = {
        "operator": "AND",
        "rules": [
            {"signal": "name_score_jaccard", "comparator": "gt", "threshold": 0.9},
            {"signal": "geo_distance_km", "comparator": "isnull"},
            {"signal": "address_line1_score", "comparator": "isnull"}
        ]
    }

    # Setup Veto Engine with the dynamically compiled rules
    rules = [
        DynamicVetoRule("VETO_DUAL_BRAND_TRAP", dual_brand_config),
        DynamicVetoRule("VETO_MISSING_GEO_TIEBREAKER", missing_geo_config)
    ]
    engine = VetoEngine(rules)

    # 2. Arrange: Test data covering various logical paths
    data = [
        # SCENARIO 1: Trigger Dual Brand (Geo < 0.05 AND Name < 0.4)
        ("uid_1", 0.01, 0.2, 0.9, 0.95), 
        # SCENARIO 2: Trigger Missing Geo (Name > 0.9 AND Geo IS NULL AND Address IS NULL)
        ("uid_2", None, 0.95, None, 0.95),
        # SCENARIO 3: Safe (Names are similar despite close proximity)
        ("uid_3", 0.01, 0.8, 0.9, 0.95),
        # SCENARIO 4: Safe (Geo exists to confirm the match)
        ("uid_4", 0.05, 0.95, 0.8, 0.95)
    ]
    columns = ["uid_i", "geo_distance_km", "name_score_jaccard", "address_line1_score", "composite_score"]
    df = spark.createDataFrame(data, columns)

    # 3. Act: Apply the dynamic engine
    vetoed_df = engine.apply(df)
    results = {row['uid_i']: row.asDict() for row in vetoed_df.collect()}

    # 4. Assert
    # uid_1: Vetoed by the dynamic Dual Brand logic
    assert results["uid_1"]["is_vetoed"] is True
    assert results["uid_1"]["veto_reason"] == "VETO_DUAL_BRAND_TRAP"
    assert results["uid_1"]["composite_score"] == 0.0

    # uid_2: Vetoed by the new 'isnull' comparator support in the evaluator
    assert results["uid_2"]["is_vetoed"] is True
    assert results["uid_2"]["veto_reason"] == "VETO_MISSING_GEO_TIEBREAKER"
    assert results["uid_2"]["composite_score"] == 0.0

    # uid_3 & uid_4: Should remain safe with their original scores
    assert results["uid_3"]["is_vetoed"] is False
    assert results["uid_3"]["composite_score"] == 0.95
    assert results["uid_4"]["is_vetoed"] is False
    assert results["uid_4"]["composite_score"] == 0.95