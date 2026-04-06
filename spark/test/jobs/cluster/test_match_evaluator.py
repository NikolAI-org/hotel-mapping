from spark.jobs.cluster.engine import MatchLogicEvaluator

def test_match_logic_evaluation(spark):
    # 1. Arrange: A simplified version of your JSON DAG config
    config = {
        "operator": "AND",
        "rules": [
            {"signal": "geo_distance_km", "threshold": 5.0, "comparator": "lte"},
            {
                "operator": "OR",
                "rules": [
                    {"signal": "name_score", "threshold": 0.8, "comparator": "gte"},
                    {"signal": "address_score", "threshold": 0.8, "comparator": "gte"}
                ]
            }
        ]
    }
    evaluator = MatchLogicEvaluator(config)

    # Mock Data to trigger different logical pathways
    data = [
        ("uid_1", 2.0, 0.9, 0.0),   # Passes both (Geo is good, Name is good)
        ("uid_2", 10.0, 0.9, 0.0),  # Fails AND block (Geo is too far)
        ("uid_3", 2.0, 0.5, 0.5),   # Fails OR block (Geo is good, but both name/address are bad)
        ("uid_4", 10.0, 0.5, 0.5),  # Fails EVERYTHING
    ]
    columns = ["uid_i", "geo_distance_km", "name_score", "address_score"]
    df = spark.createDataFrame(data, columns)

    # 2. Act
    res_df = evaluator.evaluate(df)
    results = {
        row['uid_i']: (row['is_matched'], row['failed_conditions']) 
        for row in res_df.collect()
    }

    # 3. Assert
    # uid_1: Perfect match, no failure reasons
    assert results["uid_1"][0] == True
    assert len(results["uid_1"][1]) == 0

    # uid_2: Failed the simple leaf node
    assert results["uid_2"][0] == False
    assert "geo_distance_km lte 5.0" in results["uid_2"][1]

    # uid_3: Failed the nested OR block
    assert results["uid_3"][0] == False
    assert "Failed OR Block (name_score, address_score)" in results["uid_3"][1]

    # uid_4: Failed multiple blocks (Array should contain both)
    assert results["uid_4"][0] == False
    assert len(results["uid_4"][1]) == 2
    assert "geo_distance_km lte 5.0" in results["uid_4"][1]
    assert "Failed OR Block (name_score, address_score)" in results["uid_4"][1]