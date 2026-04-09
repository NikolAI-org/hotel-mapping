from spark.jobs.cluster.engine import ClusterCohesionValidator

def test_cluster_cohesion_validator(spark):
    # Setup: We strictly require matches against providers 'p2' and 'p3' if they exist in the cluster
    validator = ClusterCohesionValidator(required_providers=["p2", "p3"])

    # 1. Arrange: Proposed Attachments (What the Router decided)
    proposed_data = [
        ("uid_1", "ATTACH_TO_CANONICAL", "C1", "veto_none"), # Wants C1 (Has p2 sibling -> Pair exists -> Pass)
        ("uid_2", "ATTACH_TO_CANONICAL", "C2", "veto_none"), # Wants C2 (Has p3 sibling -> NO Pair -> Fail)
        ("uid_3", "ATTACH_TO_CANONICAL", "C3", "veto_none"), # Wants C3 (Only p5 siblings -> Safe to attach)
        ("uid_4", "CREATE_NEW_SINGLETON", "C4", "veto_none") # Already a singleton (Ignore)
    ]
    proposed_df = spark.createDataFrame(proposed_data, ["uid_i", "routing_decision", "assigned_canonical_id", "veto_reason"])

    # 2. Arrange: Existing Mappings (The current state of the clusters)
    mappings_data = [
        ("sib_1", "C1", "ACTIVE", "p2"), # C1 contains a required p2 hotel
        ("sib_2", "C2", "ACTIVE", "p3"), # C2 contains a required p3 hotel
        ("sib_3", "C3", "ACTIVE", "p5")  # C3 contains an unrequired p5 hotel
    ]
    mappings_df = spark.createDataFrame(mappings_data, ["uid", "canonical_id", "mapping_state", "providerName"])

    # 3. Arrange: Valid Pairs (The raw matched edges that passed all boolean/veto thresholds)
    pairs_data = [
        ("uid_1", "sib_1") # uid_1 successfully paired with sib_1. (Notice uid_2 is missing its pair with sib_2)
    ]
    pairs_df = spark.createDataFrame(pairs_data, ["uid_i", "uid_j"])

    # 4. Act
    result_df = validator.validate(proposed_df, mappings_df, pairs_df)
    results = {row["uid_i"]: row["routing_decision"] for row in result_df.collect()}

    # 5. Assert
    assert results["uid_1"] == "ATTACH_TO_CANONICAL", "Cohesion Failed: Should attach because valid pair exists with p2 sibling."
    assert results["uid_2"] == "CONFLICT_FAILED_COHESION", "Cohesion Failed: Should downgrade to conflict because pair with p3 sibling is missing."
    assert results["uid_3"] == "ATTACH_TO_CANONICAL", "Cohesion Failed: Should attach safely because C3 has no required providers to validate against."
    assert results["uid_4"] == "CREATE_NEW_SINGLETON", "Cohesion Failed: Should not alter records that were already singletons."