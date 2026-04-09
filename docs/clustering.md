
---

# Hotel Entity Resolution Pipeline: Technical Documentation

## Overview
This system is a highly configurable, PySpark-based Entity Resolution (ER) pipeline designed to deduplicate and cluster hotel inventory across multiple suppliers (e.g., Expedia, GrnConnect, Hobse). It guarantees data integrity through a strict Hub-and-Spoke (Non-Transitive) architecture, deterministic ID generation, and multi-layered validation engines to achieve a "Zero False Positive" (0-FP) merge rate.

---

## 1. Cluster Creation Logic
The core clustering logic (specifically in `NonTransitiveStrategy`) evaluates incoming hotel pairs through a rigorous, multi-stage gauntlet before allowing an attachment to a "Canonical Hub" (Golden Record).

The flow of data through the engines is as follows:

1. **Base Scoring (`PairScorer`)**: Calculates a `composite_score` (0.0 to 1.0) based on configured weights (e.g., Jaccard name score, Address SBERT score).
2. **Boolean Evaluation (`MatchLogicEvaluator`)**: Applies a strict, nested JSON/YAML ruleset (AND/OR trees). If a pair fails the hard boolean logic, `is_matched` becomes `False`.
3. **Veto Engine (`VetoEngine`)**: Scans for specific business anti-patterns (e.g., `DualBrandVeto` for hotels in the same building with different names). If triggered, the score is forced to `0.0`.
4. **Routing Engine (`RoutingDecisionEngine`)**: Ranks candidate pairs for a given hotel and makes a routing decision (`ATTACH_TO_CANONICAL`, `CONFLICT_AMBIGUOUS`, etc.) based on thresholds and margins.
5. **Intra-Cluster Cohesion (`ClusterCohesionValidator`)**: Before finalizing an attachment, it checks the target cluster. If the cluster contains hotels from `REQUIRED_PROVIDERS` (e.g., "ean"), the incoming hotel *must* also have a valid pair with those specific sibling hotels.
6. **Orphan Catching**: A `left_anti` join ensures that hotels with zero candidate pairs are automatically caught and routed as new singletons.

---

## 2. Scenarios Solved & Cluster Formation

Clusters are formed using a **Hub-and-Spoke** model. A single Canonical ID acts as the Hub, and supplier hotels (Spokes) point to it. The pipeline elegantly resolves several complex real-world scenarios:

* **Clear Winner (`ATTACH_TO_CANONICAL`)**: A hotel matches a Hub with a score $\ge$ `threshold_high` AND outscores the second-best match by more than the `conflict_margin`.
* **Ambiguity (`CONFLICT_AMBIGUOUS`)**: A hotel matches two different Hubs with scores very close to each other (difference $<$ `conflict_margin`). Instead of guessing incorrectly, it rejects the merge and safely creates a new singleton.
* **The Dual-Brand Trap (`CONFLICT_VETOED`)**: Two distinct hotel brands share the exact same building/address. The Veto Engine detects high geo-proximity but low name similarity and blocks the merge.
* **The Sub-Threshold Match (`CREATE_NEW_SINGLETON`)**: The best match is below the safe threshold, meaning it's a completely new property.
* **Triangle Completion Failure (`CONFLICT_FAILED_COHESION`)**: Solves the "Cluster Drift" problem. If Hotel A wants to join Cluster 1, but Cluster 1 contains a required Expedia hotel that Hotel A *doesn't* match, the merge is rejected.
* **Orphaned Singletons (`NO_CANDIDATE_PAIRS`)**: Hotels so unique they generated zero candidate pairs in the upstream blocking phase are safely caught and given their own Hub ID.

---

## 3. Canonical ID Generation Logic
To ensure IDs are immutable, reproducible, and easily debuggable, the `CanonicalIdGenerator` uses a deterministic Geo-Hashing approach.

**Format:** `[COUNTRY]-[CITY]-[STATE]-[SHA256]`
**Example:** `US-NEW_YORK-NY-33112ee14ee4`

**Generation Steps:**
1. **Data Cleaning**: Strips whitespace, replaces internal spaces with underscores, and capitalizes geo-fields.
2. **Fallback Logic**: If a field is missing or null, it falls back to a safe default (`XX` for country, `UNKNOWN` for city/state).
   * *Example dirty data fallback:* `XX-UNKNOWN-UNKNOWN-a1b2c3d4...`
3. **Hashing**: Generates a SHA-256 hash of the underlying supplier's `uid` (taking the first 12 characters) to guarantee uniqueness.
4. **Cold Start Inheritance**: During initial pipeline runs, if Hotel A attaches to Hotel B, Hotel A inherits Hotel B's generated geo-hash to form the first cluster.

---

## 4. Debugging and Logging Details

The architecture uses a "Write-Audit-Publish" paradigm, making debugging extremely transparent. 

**The Audit Log (`comparison_audit_log`)**:
Every hotel processed by the pipeline writes exactly one row to an append-only Delta table. This table includes:
* `target_canonical_id`: The ID it tried to attach to.
* `decision`: The exact router decision (`ATTACH...`, `CONFLICT_AMBIGUOUS`, etc.).
* `confidence_score`: The final weighted composite score.
* `veto_reason`: If rejected, exactly *why* it was rejected (e.g., `MISSING_REQUIRED_PROVIDER_MATCH`, `VETO_DUAL_BRAND_TRAP`).

**Match Evaluator Arrays (`failed_conditions`)**:
The `MatchLogicEvaluator` dynamically parses the JSON DAG and appends a `failed_conditions` array column to the Spark execution plan. If a pair fails the boolean logic, the exact nested block that failed (e.g., `["Failed OR Block (name_score_jaccard...)", "geo_distance_km lte 0.5"]`) is recorded.

**Console Diagnostics**:
On job start, the `PairScorer` prints the exact weights and available schema columns to `stdout` ensuring immediate failure if upstream schemas change unexpectedly.

---

## 5. Configurations
The pipeline relies entirely on environment variables passed down from the Airflow DAG (`e2e_pipeline.py`), making it easily tunable without code changes.

* `WEIGHTS`: JSON dictionary defining the contribution of each algorithmic score (e.g., `{"name_score_jaccard": 0.1, "address_sbert_score": 0.1}`).
* `MATCH_LOGIC`: A recursive JSON tree defining hard `AND`/`OR` gates for geometric, textual, and phonetic minimums.
* `THRESHOLD_HIGH` (Default `0.85`): The minimum composite score required to attach to a hub.
* `CONFLICT_MARGIN` (Default `0.05`): The minimum score delta required between the #1 and #2 match to confidently avoid ambiguous merges.
* `REQUIRED_PROVIDERS`: A JSON array (e.g., `["grnconnect", "ean"]`) dictating which providers enforce strict Intra-Cluster Cohesion validations.
* `TRANSITIVITY` (Boolean): Toggles between the `TransitiveStrategy` (Legacy Graph/Union-Find) and the 0-FP `NonTransitiveStrategy` (Hub-and-Spoke).

---

## 6. Architectural & Design Highlights

**1. The 3-Tier Delta Lake Output**
The `EntityResolutionPipeline` separates outputs into three distinct Delta tables to satisfy both analytical and operational needs:
* **Tier 1 (Audit Log)**: Append-only ledger of every decision made.
* **Tier 2 (Cluster Mappings)**: Upsert table acting as the "Spokes". Maps supplier `uid` -> `canonical_id`.
* **Tier 3 (Canonical Registry)**: Upsert table acting as the "Hubs". Contains only the Golden Records.

**2. SOLID Principles & Dependency Injection**
The `NonTransitiveStrategy` takes its engines (`VetoEngine`, `Router`, `Validator`, `Scorer`) via constructor injection. This makes unit testing trivial (using `MagicMock` as seen in `test_strategy_orchestration.py`) and allows you to hot-swap routing logic without touching the underlying Spark transformations.

**3. Idempotent Execution**
Because Canonical IDs are generated via deterministic hashing (`SHA256`) rather than auto-incrementing integers, and because Delta Lake `merge_table` operations handle the upserts, the Spark job is entirely idempotent. If an Airflow task crashes due to OOM/Timeouts (e.g., the `SIGTERM` zombie task scenario), the job can be safely retried without creating duplicate clusters.