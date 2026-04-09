from typing import List, Tuple, Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class FlexibleClustering:
    def __init__(self, spark, transitivity_enabled=True):
        self.spark = spark
        self.transitivity_enabled = transitivity_enabled

    def cluster(self, new_hotels_df: DataFrame, scored_pairs_df: DataFrame, existing_clusters_df: DataFrame = None) -> DataFrame:
        # 1. Collect Edges from Scored Pairs (Only those satisfying the Match Logic)
        matched_pairs = scored_pairs_df.filter(F.col("is_matched") == True).select("uid_i", "uid_j")
        edges_list = matched_pairs.rdd.map(lambda row: (row[0], row[1])).collect()
        
        # 2. Collect UIDs of the new provider
        new_uids = [row.uid for row in new_hotels_df.select("uid").distinct().collect()]
        
        # 3. Handle Existing State & Find Max Cluster ID
        existing_map = {}
        max_cluster_id = 0
        
        if existing_clusters_df:
            existing_rows = existing_clusters_df.select("uid", "cluster_id").collect()
            existing_map = {str(r['uid']): str(r['cluster_id']) for r in existing_rows}
            
            # Find the highest existing numerical cluster ID to increment from
            for cid in existing_map.values():
                if str(cid).isdigit():
                    max_cluster_id = max(max_cluster_id, int(cid))

        # 4. Perform Union-Find on Driver
        parent_map = self._driver_side_union_find(edges_list, existing_map, new_uids, max_cluster_id)
        
        # 5. Map back to Spark
        cluster_data = [(uid, str(cluster_id)) for uid, cluster_id in parent_map.items() if uid in new_uids]
        cluster_df = self.spark.createDataFrame(cluster_data, ["uid", "cluster_id"])
        
        # We no longer need the F.coalesce(..., F.col("uid")) fallback because parent_map guarantees an ID
        return new_hotels_df.join(cluster_df, on="uid", how="left")

    def _driver_side_union_find(self, edges, existing_map, new_uids, max_cluster_id):
        parent = {}
        
        # Init: Use existing cluster_id as root
        for uid, cid in existing_map.items():
            parent[uid] = cid
            if cid not in parent: parent[cid] = cid
            
        # Init: Set new items to their own UID temporarily
        for uid in new_uids:
            if uid not in parent: parent[uid] = uid

        def find(i):
            if parent[i] == i: return i
            parent[i] = find(parent[i])
            return parent[i]

        def union(i, j):
            root_i, root_j = find(i), find(j)
            if root_i != root_j:
                # Prioritize merging into EXISTING numerical IDs over new String UIDs
                is_num_i = str(root_i).isdigit()
                is_num_j = str(root_j).isdigit()
                
                if is_num_i and not is_num_j:
                    parent[root_j] = root_i
                elif is_num_j and not is_num_i:
                    parent[root_i] = root_j
                else:
                    # Deterministic merge
                    if str(root_i) < str(root_j): parent[root_j] = root_i
                    else: parent[root_i] = root_j

        if self.transitivity_enabled:
            for u, v in edges:
                if u in parent and v in parent: union(u, v)
        
        # --- Generate Final Numerical IDs ---
        final_map = {}
        next_cluster_id = max_cluster_id + 1
        root_to_numeric = {}

        # Pre-register existing numerical IDs so they don't get reassigned
        for cid in existing_map.values():
            if str(cid).isdigit():
                root_to_numeric[cid] = cid
        
        for uid in list(existing_map.keys()) + new_uids:
            root = find(uid)
            
            # If the root is a string UID, it means this is a brand new cluster
            if root not in root_to_numeric:
                root_to_numeric[root] = str(next_cluster_id)
                next_cluster_id += 1
            
            final_map[uid] = root_to_numeric[root]
        
        return final_map