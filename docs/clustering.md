## **Entity Resolution Pipeline: Technical Overview**

The primary goal of this pipeline is to ingest hotel records from different providers, determine which records represent the same physical hotel, and group them into a stable `cluster_id`. It uses a combination of **weighted scoring** and **nested boolean logic** (via YAML/JSON) to make matching decisions.

### **1. High-Level System Architecture**

The process follows these four main phases:

* **Ingestion:** Reads data from Delta tables, filtering for specific providers (e.g., `hbose`).
* **Scoring:** Calculates a `composite_score` based on weighted similarities (Name, Address, Geo-distance).
* **Logic Evaluation:** Applies complex business rules (e.g., "Must have Name Score > 0.8 AND Distance < 100m") to determine a hard `is_matched` boolean.
* **Clustering (Union-Find):** Groups matched pairs into global clusters, ensuring transitivity (if A=B and B=C, then A=C).

---

## **2. Detailed Algorithm & Component Flow**

### **A. Scoring Engine (`pair_scorer.py`)**

This component calculates a unified similarity score between two hotels.

* **Weighted Average:** It takes various signals (Jaccard, Levenshtein, SBERT) and applies weights defined in the Airflow environment.
* **Classification:** It assigns a preliminary status: `MATCHED`, `MANUAL_REVIEW`, or `UNMATCHED` based on two thresholds ($t_{high}$ and $t_{low}$).

### **B. Rule Engine (`entity_resolution_pipeline.py`)**

This is the "brain" of the operation. It uses `build_match_expression` to recursively convert a JSON logic tree into a **PySpark SQL Expression**.

* **Boolean Logic:** Supports nested `AND`/`OR` operations.
* **Signal Comparisons:** Evaluates signals like `geo_distance_km` or `email_match_score` against specific thresholds.
* **Auditability:** The `append_failure_reasons` method identifies exactly which rule failed for a specific pair, storing it in an array for debugging.

1. Core Mechanism: Recursive Expression Building
The engine uses the Composite Design Pattern. It treats individual signal checks (leaves) and logical groups (branches) the same way, allowing for infinite nesting.
In entity_resolution_pipeline.py, the build_match_expression method performs this recursion:

```
def build_match_expression(self, rule_node: dict) -> F.Column:
    # 1. BRANCH LOGIC: Handle AND/OR blocks
    if "operator" in rule_node:
        op = rule_node["operator"].upper()
        # Recursively call this function for every child rule
        child_exprs = [self.build_match_expression(child) for child in rule_node["rules"]]
        # Reduce the list of columns into a single boolean Column (A & B & C...)
        return functools.reduce(lambda x, y: x & y if op == "AND" else x | y, child_exprs)
    
    # 2. LEAF LOGIC: Handle specific signal thresholds
    elif "signal" in rule_node:
        signal_col = F.coalesce(F.col(rule_node["signal"]), F.lit(0.0))
        threshold = float(rule_node["threshold"])
        comp = rule_node["comparator"].lower()
        
        # Returns a Spark Column object: (F.col("geo_distance") <= 0.1)
        if comp == "lte": return signal_col <= threshold
        # ... other comparators (gte, eq, etc.)
```
2. Logical Flow Diagram
```
CONFIG (JSON)                      PYSPARK EXPRESSION (SQL)
----------------------------       -------------------------------------------
{                                  
  "operator": "AND",               ( (name_score >= 0.8 OR sbert_score >= 0.9)
  "rules": [                         AND
    {                                (geo_distance <= 0.5) )
      "operator": "OR",            
      "rules": [ ... ]             
    },                             
    {                              
      "signal": "geo_distance",    
      "threshold": 0.5             
    }                              
  ]                                
}
```

3. Auditability: Why did a match fail?
A common frustration in Entity Resolution is not knowing why two hotels didn't cluster. Your implementation solves this with append_failure_reasons.

The Loop: It iterates through every top-level rule in your AND block.

The Boolean Inverse: For each rule, it checks ~rule_expr (the NOT operator).

The Collection: If a rule evaluates to False, the name of that rule (e.g., "Failed AND Block (name_score_jaccard...)") is added to a list.

The Result: This results in a failed_conditions column in your audit log, stored as an array<string>.


### **C. Incremental Clustering (`flexible_clustering.py`)**

Unlike standard clustering, this implementation is **stateful** and **incremental**.

* **State Awareness:** It reads `existing_clusters_df` to ensure that previously assigned `cluster_id`s remain stable.
* **Driver-Side Union-Find:**
1. Collects all `is_matched` edges.
2. Performs a Union-Find on the Spark Driver.
3. **Stability Logic:** If an existing numerical `cluster_id` is found in a group, it forces the entire group to adopt that ID.
4. **New IDs:** If a group is entirely new, it generates a new incremental ID starting from `max_cluster_id + 1`.


1. The Core "Stateful" Algorithm
The algorithm uses a Union-Find (Disjoint Set Union) structure. It prioritizes stability by merging new "potential" clusters into existing, established "numerical" IDs.

Key Steps in FlexibleClustering.py:
- Initialize from State: It maps existing hotels to their current cluster_id.
- Edge Collection: It gathers "edges" (connections) only from pairs that passed the Rule Engine (is_matched == True).
- Union-Find on Driver: It processes these connections. If a new hotel matches an existing hotel, the new hotel "inherits" the existing numerical ID.
- New Cluster Generation: Any group that doesn't touch an existing cluster gets a brand-new ID starting from max_cluster_id + 1.

2. Logical Flow

```
STEP 1: INITIAL STATE (from Delta Table)
[Hotel A: ID 100]   [Hotel B: ID 100]   [Hotel C: ID 101]

STEP 2: NEW DATA & MATCHES (Edges)
New Hotel D <---matches---> Hotel B (Existing)
New Hotel E <---matches---> New Hotel F

STEP 3: UNION-FIND (Merging)
Group 1: {A, B, D}  --> Root is 100 (Existing ID)
Group 2: {C}        --> Root is 101 (Existing ID)
Group 3: {E, F}     --> Root is New (Needs New ID)

STEP 4: FINAL ASSIGNMENT
Hotel D gets ID 100
Hotel E gets ID 102 (Max + 1)
Hotel F gets ID 102

```

3. Code Reference: The Stability Logic

The most critical part is the union function within _driver_side_union_find. It contains the logic that prevents IDs from changing randomly.

```
# Reference from flexible_clustering.py
def union(i, j):
    root_i, root_j = find(i), find(j)
    if root_i != root_j:
        # PRIORITIZATION LOGIC: 
        # If one root is a known number (e.g., "100") and the other is a 
        # string UID (e.g., "hotel_555"), ALWAYS pick the number.
        is_num_i = str(root_i).isdigit()
        is_num_j = str(root_j).isdigit()
        
        if is_num_i and not is_num_j:
            parent[root_j] = root_i  # Merge new into existing
        elif is_num_j and not is_num_i:
            parent[root_i] = root_j  # Merge new into existing
        else:
            # Deterministic merge for two new or two existing groups
            if str(root_i) < str(root_j): parent[root_j] = root_i
            else: parent[root_i] = root_j

```

4. Key Points
Stability: This logic prevents "Cluster Flapping." Without the isdigit() check, a merge between a new hotel and an old cluster might accidentally rename the whole cluster to a random UID.

Driver Scalability: Note that the clustering happens on the Spark Driver using .collect(). This works perfectly for hotel providers (usually thousands or tens of thousands of records), but if you were clustering millions of edges, this would need to move to a distributed GraphX or GraphFrames approach.

Transitivity Toggle: The transitivity_enabled flag (set via environment variable) allows you to turn off the "friend of a friend" matching if the clusters become too "blobby" (over-clustering).

---

## **3. Key Features**

| Feature | Implementation Detail |
| --- | --- |
| **Transitivity** | Managed by the `Union-Find` algorithm; can be toggled via `TRANSITIVITY` env var. |
| **Audit Log** | The `comparison_audit_log` table captures every pair evaluated, its scores, and why it failed the logic. |
| **Deduplication** | Uses `.dropDuplicates(["uid"])` at multiple stages to prevent data skew and primary key violations. |
| **Scalability** | Scoring and rule evaluation happen in Spark; only the final clustering (edges) is pulled to the Driver. |

---

## **4. Data Flow Walkthrough**

1. **Job Entry:** `entity_resolution_job.py` initializes the `SparkSession` and loads configurations.
2. **Fallback Logic:** If no pairs are found for a provider, the system treats every hotel as a new, unique cluster.
3. **The "Main" Path:**
* **Scoring:** Pairs get a `composite_score`.
* **Matching:** `is_matched` is calculated via the recursive rule builder.
* **Clustering:** `FlexibleClustering` merges new hotels into existing clusters or creates new ones.
* **Transitivity Check:** A specific flag `transitivity_issue` is raised if the logic says two hotels match, but they ended up in different clusters (useful for tuning).


