from spark.jobs.cluster.engine import VetoEngine, DualBrandVeto

def test_dual_brand_veto(spark):
    # Setup Veto Engine
    engine = VetoEngine([DualBrandVeto()])

    # 1. Arrange: Veto triggers if geo < 0.05 AND jaccard < 0.4
    data = [
        # Row 1: Same building, totally different names (SHOULD VETO)
        ("uid_1", 0.01, 0.2, 0.95), 
        # Row 2: Same building, very similar names (SAFE)
        ("uid_2", 0.01, 0.8, 0.95),
        # Row 3: Far away, different names (SAFE - just a bad match, not a veto trap)
        ("uid_3", 10.0, 0.2, 0.95)
    ]
    columns = ["uid_i", "geo_distance_km", "name_score_jaccard", "composite_score"]
    df = spark.createDataFrame(data, columns)

    # 2. Act
    vetoed_df = engine.apply(df)
    # FIX: Check 'composite_score' instead of 'final_score'
    results = {row['uid_i']: (row['is_vetoed'], row['composite_score']) for row in vetoed_df.collect()}

    # 3. Assert
    # uid_1 is vetoed, its composite_score must be overwritten to 0.0
    assert results["uid_1"] == (True, 0.0) 
    
    # uid_2 and uid_3 are safe, their original score (0.95) remains
    assert results["uid_2"] == (False, 0.95)
    assert results["uid_3"] == (False, 0.95)