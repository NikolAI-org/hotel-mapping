# /opt/airflow/spark/jobs/cluster/validate_entity_resolution.py

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys


def main():
    if len(sys.argv) < 3:
        print("ERROR: Missing arguments.")
        print(
            "Usage: spark-submit validate_entity_resolution.py <audit_s3_path> <clusters_s3_path>"
        )
        sys.exit(1)

    audit_path = sys.argv[1]
    clusters_path = sys.argv[2]

    spark = SparkSession.builder.appName(
        "DataValidation_EntityResolution"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n========================================")
    print("🚀 RUNNING DATA QUALITY VALIDATIONS")
    print("========================================")

    errors = []

    try:
        audit_df = spark.read.format("delta").load(audit_path)
        clusters_df = spark.read.format("delta").load(clusters_path)
    except Exception as e:
        print(f"❌ CRITICAL ERROR: Could not read Delta tables. Exception: {e}")
        sys.exit(1)

    # ---------------------------------------------------------
    # VALIDATION SUITE 1: COMPARISON AUDIT LOG
    # ---------------------------------------------------------
    print("\n[1/5] Validating comparison_audit_log...")
    audit_count = audit_df.count()

    if audit_count == 0:
        print("⚠️  Warning: Audit log is empty.")
    else:
        invalid_scores = audit_df.filter(
            (F.col("score") < 0.0) | (F.col("score") > 1.0)
        ).count()
        if invalid_scores > 0:
            errors.append(
                f"Audit Log: {invalid_scores} rows have scores outside [0.0, 1.0]."
            )

        valid_statuses = ["MATCHED", "MANUAL_REVIEW", "UNMATCHED"]
        invalid_status = audit_df.filter(~F.col("status").isin(valid_statuses)).count()
        if invalid_status > 0:
            errors.append(f"Audit Log: {invalid_status} rows have an invalid status.")

    # ---------------------------------------------------------
    # VALIDATION SUITE 2: FINAL CLUSTERS
    # ---------------------------------------------------------
    print("[2/5] Validating final_clusters...")
    cluster_count = clusters_df.count()

    if cluster_count == 0:
        errors.append("Final Clusters table is completely empty!")
    else:
        null_clusters = clusters_df.filter(F.col("cluster_id").isNull()).count()
        if null_clusters > 0:
            errors.append(f"Clusters: {null_clusters} rows have a null cluster_id.")

        duplicate_uids_df = (
            clusters_df.groupBy("uid").count().filter(F.col("count") > 1)
        )
        if duplicate_uids_df.count() > 0:
            errors.append("Clusters: Duplicate UIDs found in the final table.")

    # ---------------------------------------------------------
    # VALIDATION SUITE 3: CLUSTER LOGIC DEEP DIVE (TOP CLUSTER)
    # ---------------------------------------------------------
    print("[3/5] Verifying Clustering Logic for Top Cluster...")
    if cluster_count > 0 and audit_count > 0:
        # Find the ID of the largest cluster
        top_cluster_row = (
            clusters_df.groupBy("cluster_id").count().orderBy(F.desc("count")).first()
        )

        if top_cluster_row:
            top_cid = top_cluster_row["cluster_id"]
            top_size = top_cluster_row["count"]

            print(f"\n🏆 === DEEP DIVE: TOP CLUSTER (ID: {top_cid}) ===")
            print(f"Size: {top_size} unique hotels combined into this single cluster.")

            # Extract all UIDs that belong to this top cluster
            top_uids = [
                r["uid"]
                for r in clusters_df.filter(F.col("cluster_id") == top_cid)
                .select("uid")
                .collect()
            ]

            # Find all audit pairs where BOTH hotels are inside this cluster (Internal Edges)
            internal_pairs_df = audit_df.filter(
                F.col("hotel_id").isin(top_uids)
                & F.col("compared_with_id").isin(top_uids)
            )

            internal_count = internal_pairs_df.count()
            print(f"\n📊 Internal Pairwise Connections Found: {internal_count}")

            if internal_count > 0:
                print("\n📈 Status Distribution inside this Cluster:")
                # Show how many matched, manual, or unmatched pairs exist INSIDE this single cluster
                internal_pairs_df.groupBy("status").count().orderBy("status").show(
                    truncate=False
                )

                print(
                    "Note: If you see 'UNMATCHED' or 'MANUAL_REVIEW' here, it proves TRANSITIVITY is working!"
                )
                print(
                    "(e.g., A matches B, and B matches C. Therefore A and C are clustered, even if A vs C scored poorly.)"
                )

                print("\n🔍 Sample Pairings that formed this Cluster:")
                internal_pairs_df.select(
                    "hotel_name", "compared_with_name", "score", "status"
                ).orderBy(F.desc("score")).show(15, truncate=False)
            else:
                print(
                    "No historical comparisons found for these UIDs (They may be singletons or from an older run)."
                )

    # ---------------------------------------------------------
    # VALIDATION SUITE 4: GLOBAL TRANSITIVITY PROOF
    # ---------------------------------------------------------
    print("\n[4/5] Proving Global Transitivity...")
    # Safe aliases to prevent ambiguous joins
    c1 = clusters_df.select(F.col("uid").alias("u1"), F.col("cluster_id").alias("cid1"))
    c2 = clusters_df.select(F.col("uid").alias("u2"), F.col("cluster_id").alias("cid2"))

    transitivity_df = audit_df.join(c1, audit_df.hotel_id == c1.u1, "inner").join(
        c2, audit_df.compared_with_id == c2.u2, "inner"
    )

    # Find pairs that failed direct matching but share the same cluster_id
    transitive_bridges = transitivity_df.filter(
        (F.col("status") != "MATCHED") & (F.col("cid1") == F.col("cid2"))
    )

    bridge_count = transitive_bridges.count()
    print(f"🌉 Found {bridge_count} Transitive Connections across the dataset!")
    if bridge_count > 0:
        transitive_bridges.select(
            "hotel_name", "compared_with_name", "score", "status", "cid1"
        ).show(5, truncate=False)

    # ---------------------------------------------------------
    # VALIDATION SUITE 5: RULE ENGINE VS. AUDIT SCORE CHECK
    # ---------------------------------------------------------
    print("\n[5/5] Checking Discrepancies between Boolean Logic and Weighted Scores...")

    if "is_matched" in audit_df.columns and "geo_distance_km" in audit_df.columns:
        # 1. Sanity Check: Did any hotel bypass the 0.5km distance limit?
        rule_violations = audit_df.filter(
            (F.col("is_matched") == True) & (F.col("geo_distance_km") > 0.5)
        ).count()
        if rule_violations > 0:
            errors.append(
                f"Rule Engine Failure: {rule_violations} pairs clustered with a distance > 0.5km."
            )
        else:
            print("✅ Rule Engine strictly enforced Geo-Distance constraints.")

        # 2. Score Discrepancies
        print("\n📈 High Weighted Score (MATCHED) but Rejected by Rule Engine:")
        rejected_df = audit_df.filter(
            (F.col("status") == "MATCHED") & (F.col("is_matched") == False)
        )
        if rejected_df.count() > 0:
            rejected_df.select(
                "hotel_name",
                "compared_with_name",
                "score",
                "geo_distance_km",
                "is_matched",
            ).show(5, truncate=False)
        else:
            print("No high-scoring pairs were rejected.")

        print("\n📈 Borderline Score (MANUAL_REVIEW) but Accepted by Rule Engine:")
        accepted_df = audit_df.filter(
            (F.col("status") == "MANUAL_REVIEW") & (F.col("is_matched") == True)
        )
        if accepted_df.count() > 0:
            accepted_df.select(
                "hotel_name",
                "compared_with_name",
                "score",
                "geo_distance_km",
                "is_matched",
            ).show(5, truncate=False)
        else:
            print("No borderline pairs were auto-accepted.")

    else:
        print(
            "⚠️ Skipping Suite 5: 'is_matched' and 'geo_distance_km' columns not found in audit log."
        )

    # ---------------------------------------------------------
    # VALIDATION SUITE 6: LOGIC RECONSTRUCTION TEST
    # ---------------------------------------------------------
    print("\n[6/6] Verifying Rule Engine Logic Integrity...")

    # If Distance <= 0.5 AND Name Jaccard >= 0.9 -> Must be True
    logic_failure_df = audit_df.filter(
        (F.col("geo_distance_km") <= 0.5)
        & (F.col("name_score_jaccard") >= 0.9)
        & (F.col("is_matched") == False)
    )

    logic_failures = logic_failure_df.count()
    if logic_failures > 0:
        print(
            f"❌ LOGIC FAILED: Found {logic_failures} pairs that should have matched but didn't."
        )
        logic_failure_df.show(5, truncate=False)
    else:
        print("✅ LOGIC PASSED: The Jaccard threshold rule evaluated perfectly.")
    # ---------------------------------------------------------
    # AGGREGATE RESULTS
    # ---------------------------------------------------------
    print("\n========================================")
    if errors:
        print("❌ VALIDATION FAILED!")
        for err in errors:
            print(f"   -> {err}")
        sys.exit(1)
    else:
        print("✅ VALIDATION PASSED SUCCESSFULLY!")
        print(f"   -> Total Unique Hotels: {cluster_count}")
        print(f"   -> Total Audit Log Entries: {audit_count}")

    spark.stop()


if __name__ == "__main__":
    main()
