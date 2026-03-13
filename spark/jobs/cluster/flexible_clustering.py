from typing import List, Tuple, Dict
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

class FlexibleClustering:
    def __init__(self, spark, transitivity_enabled=True):
        self.spark = spark
        self.transitivity_enabled = transitivity_enabled

    def cluster(self, new_hotels_df: DataFrame, scored_pairs_df: DataFrame, existing_clusters_df: DataFrame = None) -> DataFrame:
        # 1. Collect Edges (Matches)
        matched_pairs = scored_pairs_df.filter(F.col("is_matched") == True).select("uid_i", "uid_j")
        edges_list = matched_pairs.rdd.map(lambda row: (row[0], row[1])).collect()
        
        # 2. Collect New UIDs
        new_uids = [row.uid for row in new_hotels_df.select("uid").distinct().collect()]
        
        # 3. Handle Existing State
        existing_map = {}
        max_cluster_id = 0
        if existing_clusters_df:
            existing_rows = existing_clusters_df.select("uid", "cluster_id").collect()
            # Map UID -> ClusterID string
            existing_map = {str(r['uid']): str(r['cluster_id']) for r in existing_rows}
            for cid in existing_map.values():
                if str(cid).isdigit():
                    max_cluster_id = max(max_cluster_id, int(cid))

        # 4. Run the fixed Logic
        parent_map = self._driver_side_union_find(edges_list, existing_map, new_uids, max_cluster_id)
        
        # 5. Map back to Spark
        cluster_data = [(uid, str(cluster_id)) for uid, cluster_id in parent_map.items() if uid in new_uids]
        cluster_df = self.spark.createDataFrame(cluster_data, ["uid", "cluster_id"])
        
        return new_hotels_df.join(cluster_df, on="uid", how="left")

    def _driver_side_union_find(self, edges, existing_map, new_uids, max_cluster_id):
        # parent stores the mapping: item -> its_parent_item (or cluster_id)
        parent = {}
        
        # A. Pre-load existing clusters to maintain incremental consistency
        # We treat existing cluster_ids as roots
        for uid, cid in existing_map.items():
            parent[uid] = str(cid)
            if str(cid) not in parent:
                parent[str(cid)] = str(cid)

        # B. Helper: Find the root (or existing Cluster ID)
        def find(i):
            if i not in parent: 
                parent[i] = i
                return i
            if parent[i] == i: 
                return i
            # Path compression
            parent[i] = find(parent[i])
            return parent[i]

        # C. Helper: Union (Transitive)
        def union_transitive(i, j):
            root_i, root_j = find(i), find(j)
            if root_i != root_j:
                # Prioritize existing numerical IDs
                is_num_i = str(root_i).isdigit()
                is_num_j = str(root_j).isdigit()
                if is_num_i and not is_num_j: parent[root_j] = root_i
                elif is_num_j and not is_num_i: parent[root_i] = root_j
                else:
                    if str(root_i) < str(root_j): parent[root_j] = root_i
                    else: parent[root_i] = root_j

        # D. Helper: Direct Link (Non-Transitive)
        def link_direct(i, j, current_max):
            root_i, root_j = find(i), find(j)
            # If both are already in DIFFERENT established clusters, do nothing (prevent bridge)
            if root_i != root_j:
                i_has_cluster = str(root_i).isdigit()
                j_has_cluster = str(root_j).isdigit()
                
                if not i_has_cluster and not j_has_cluster:
                    # Neither is in a cluster, create a new one for both
                    current_max += 1
                    new_cid = str(current_max)
                    parent[root_i] = new_cid
                    parent[root_j] = new_cid
                elif i_has_cluster and not j_has_cluster:
                    # i has a cluster, bring j into it
                    parent[root_j] = root_i
                elif j_has_cluster and not i_has_cluster:
                    # j has a cluster, bring i into it
                    parent[root_i] = root_j
            return current_max

        # E. Process Edges (THE CORE FIX)
        # We no longer skip this loop based on the transitivity flag.
        temp_max = max_cluster_id
        for u, v in edges:
            if self.transitivity_enabled:
                union_transitive(u, v)
            else:
                temp_max = link_direct(u, v, temp_max)

        # F. Final Pass: Assign IDs to all new hotels
        final_map = {}
        next_id = temp_max + 1
        
        for uid in new_uids:
            root = find(uid)
            if str(root).isdigit():
                final_map[uid] = str(root)
            else:
                # Singleton: give unique ID
                final_map[uid] = str(next_id)
                next_id += 1
                
        return final_map