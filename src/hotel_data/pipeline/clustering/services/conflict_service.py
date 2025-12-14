# services/conflict_service.py

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import (
    col, when, lit, struct, array, explode, 
    collect_set, max as spark_max, row_number
)
from pyspark.sql.window import Window
from hotel_data.pipeline.clustering.core.clustering_interfaces import ConflictDetectionStrategy
from typing import Dict, Any, List, Tuple, Set
from collections import defaultdict, deque
import logging

class ConflictPath:
    """Represents a conflict path (chain)"""
    def __init__(self, path: List[str], scores: List[float]):
        self.path = path
        self.scores = scores
    
    def __repr__(self):
        return f"Path: {' → '.join(self.path)}, Scores: {self.scores}"
    
    def length(self):
        return len(self.path)


class TransitiveConflictDetector(ConflictDetectionStrategy):
    """
    Detects conflicts where transitive property is violated
    
    Prevents clustering chains that would incorrectly group
    dissimilar hotels together.
    
    Features:
    - Transitive property validation
    - Chain detection and prevention
    - Multiple resolution strategies
    - Detailed statistics and reporting
    - Extensible for custom validators
    """
    
    def __init__(
        self,
        logger,
        confidence_threshold: float = 0.70,
        conflict_tolerance: float = 0.15,
        max_chain_length: int = 3,
        resolution_strategy: str = "remove_weakest"
    ):
        """
        Initialize conflict detector
        
        Args:
            logger: Logger instance
            confidence_threshold: Score threshold to consider valid pairs
            conflict_tolerance: How much score drop allowed before conflict
            max_chain_length: Maximum allowed chain length (prevents long chains)
            resolution_strategy: How to resolve conflicts
                - "remove_weakest": Remove lowest-score pair
                - "remove_all": Remove all conflicting pairs
                - "adjust_confidence": Lower confidence of weak links
                - "break_chain": Remove middle pair
        """
        self.logger = logger
        self.confidence_threshold = confidence_threshold
        self.conflict_tolerance = conflict_tolerance
        self.max_chain_length = max_chain_length
        self.resolution_strategy = resolution_strategy
        
        # Statistics
        self.stats = {
            'total_pairs': 0,
            'conflicts_detected': 0,
            'conflict_types': defaultdict(int),
            'chains_found': 0,
            'longest_chain': 0
        }
        
        self.logger.info(
            "TransitiveConflictDetector initialized",
            threshold=confidence_threshold,
            tolerance=conflict_tolerance,
            max_chain=max_chain_length,
            strategy=resolution_strategy
        )
    
    # ════════════════════════════════════════════════════════════════════════
    # MAIN METHODS
    # ════════════════════════════════════════════════════════════════════════
    
    def detect_conflicts(self, scored_pairs_df: DataFrame) -> DataFrame:
        """
        Detect conflicts in scored pairs using transitive property
        
        Process:
        1. Build pair graph
        2. Find all triplets (A-B, B-C, A-C)
        3. Check for transitive violations
        4. Detect chains (sequences of connected pairs)
        5. Mark conflicts with severity
        """
        self.logger.info("Detecting conflicts (transitive property)...")
        
        # Count input
        total_pairs = scored_pairs_df.count()
        self.stats['total_pairs'] = total_pairs
        
        self.logger.info(f"Processing {total_pairs} pairs for conflicts")
        
        # ═════════════════════════════════════════════════════════════════
        # PHASE 1: PREPARE DATA
        # ═════════════════════════════════════════════════════════════════
        
        df = scored_pairs_df
        
        # Filter to valid pairs (above threshold)
        valid_pairs = df.filter(col("confidence_level").isin("HIGH", "MEDIUM"))
        valid_count = valid_pairs.count()
        
        self.logger.info(
            f"Valid pairs (HIGH/MEDIUM confidence): {valid_count}/{total_pairs}"
        )
        
        # ═════════════════════════════════════════════════════════════════
        # PHASE 2: FIND TRIPLETS
        # ═════════════════════════════════════════════════════════════════
        
        self.logger.info("Finding triplets for transitive checking...")
        
        # Rename valid pairs to get A-B and B-C
        ab_pairs = valid_pairs.select(
            col("id_i").alias("a"),
            col("id_j").alias("b"),
            col("composite_score").alias("ab_score")
        )
        
        bc_pairs = valid_pairs.select(
            col("id_i").alias("b"),
            col("id_j").alias("c"),
            col("composite_score").alias("bc_score")
        )
        
        # Join on B (middle hotel)
        triplets = (
            ab_pairs
            .join(bc_pairs, on="b", how="inner")
            .select(
                col("a"),
                col("b"),
                col("c"),
                col("ab_score"),
                col("bc_score")
            )
        )
        
        triplet_count = triplets.count()
        self.logger.info(f"Found {triplet_count} potential triplets")
        
        # ═════════════════════════════════════════════════════════════════
        # PHASE 3: CHECK TRANSITIVITY
        # ═════════════════════════════════════════════════════════════════
        
        self.logger.info("Checking transitive property for all triplets...")
        
        # Get all A-C direct scores
        ac_direct = (
            df.filter(
                (col("composite_score") >= 0.0)
            )
            .select(
                col("id_i").alias("a"),
                col("id_j").alias("c"),
                col("composite_score").alias("ac_direct_score"),
                col("confidence_level").alias("ac_confidence")
            )
        )
        
        # Join triplets with direct A-C scores
        conflict_candidates = (
            triplets
            .join(
                ac_direct,
                on=["a", "c"],
                how="left"
            )
            .fillna(0.0, subset=["ac_direct_score"])
            .fillna("NONE", subset=["ac_confidence"])
        )
        
        # Mark conflicts: A-B-C path exists, but A-C missing or low
        conflict_df = self._mark_conflicts(conflict_candidates)
        
        conflict_count = conflict_df.filter(col("has_conflict")).count()
        self.stats['conflicts_detected'] = conflict_count
        
        self.logger.info(f"Conflicts detected: {conflict_count}")
        
        # ═════════════════════════════════════════════════════════════════
        # PHASE 4: DETECT CHAINS
        # ═════════════════════════════════════════════════════════════════
        
        self.logger.info("Detecting chains (long connected sequences)...")
        chains = self._detect_chains(valid_pairs)
        chain_count = len(chains)
        self.stats['chains_found'] = chain_count
        
        if chain_count > 0:
            max_chain = max(len(c.path) for c in chains)
            self.stats['longest_chain'] = max_chain
            self.logger.warning(
                f"Chains detected: {chain_count} (longest: {max_chain} hotels)"
            )
        
        # ═════════════════════════════════════════════════════════════════
        # PHASE 5: ADD CONFLICT INFO TO ALL PAIRS
        # ═════════════════════════════════════════════════════════════════
        
        # Start with original pairs
        result_df = df
        
        # Mark which pairs are in conflicts
        conflict_pairs = conflict_df.select("a", "b", "c", "has_conflict", "conflict_reason")
        
        # Add conflict columns
        result_df = self._add_conflict_columns(result_df, conflict_df)
        
        self.logger.info("Conflict detection complete")
        
        return result_df
    
    def resolve_conflicts(self, pairs_with_conflicts_df: DataFrame) -> Tuple[DataFrame, Dict[str, Any]]:
        """
        Resolve conflicts and return both cleaned DataFrame and resolution metrics
        
        Returns:
            (resolved_pairs_df, resolution_stats)
        """
        self.logger.info(f"Resolving conflicts using strategy: {self.resolution_strategy}")
        
        df = pairs_with_conflicts_df
        initial_count = df.count()
        conflict_count = df.filter(col("has_conflict") == True).count() if "has_conflict" in df.columns else 0
        
        try:
            if self.resolution_strategy == "remove_weakest":
                resolved_df = self._resolve_remove_weakest(df)
            elif self.resolution_strategy == "remove_all":
                resolved_df = self._resolve_remove_all(df)
            elif self.resolution_strategy == "adjust_confidence":
                resolved_df = self._resolve_adjust_confidence(df)
            elif self.resolution_strategy == "break_chain":
                resolved_df = self._resolve_break_chain(df)
            else:
                self.logger.warning(f"Unknown strategy: {self.resolution_strategy}")
                resolved_df = df
            
            # Collect statistics
            final_count = resolved_df.count()
            pairs_removed = initial_count - final_count
            
            stats = {
                'strategy': self.resolution_strategy,
                'initial_pairs': initial_count,
                'conflicts_detected': conflict_count,
                'pairs_removed': pairs_removed,
                'pairs_kept': final_count,
                'removal_rate': round((pairs_removed / max(initial_count, 1)) * 100, 2)
            }
            
            self.logger.info(
                f"✓ Conflict resolution complete",
                strategy=self.resolution_strategy,
                removed=pairs_removed,
                kept=final_count
            )
            
            return resolved_df, stats
            
        except Exception as e:
            self.logger.error(f"Conflict resolution failed: {str(e)}")
            return df, {'error': str(e), 'strategy': self.resolution_strategy}

    
    def get_statistics(self) -> Dict[str, Any]:
        """Get conflict detection statistics"""
        return {
            'total_pairs': self.stats['total_pairs'],
            'conflicts_detected': self.stats['conflicts_detected'],
            'conflict_types': dict(self.stats['conflict_types']),
            'chains_found': self.stats['chains_found'],
            'longest_chain': self.stats['longest_chain'],
            'conflict_percentage': (
                self.stats['conflicts_detected'] / max(self.stats['total_pairs'], 1) * 100
            )
        }
    
    # ════════════════════════════════════════════════════════════════════════
    # HELPER METHODS - CONFLICT DETECTION
    # ════════════════════════════════════════════════════════════════════════
    
    def _mark_conflicts(self, triplets_df: DataFrame) -> DataFrame:
        """
        Mark which triplets have conflicts
        
        Conflict logic:
        1. If A-B-C path exists with high scores
        2. But A-C direct is missing or low
        3. Then conflict exists
        """
        
        # Calculate expected score (harmonic mean of A-B and B-C)
        expected_score_expr = (
            (col("ab_score") * col("bc_score")) / 
            ((col("ab_score") + col("bc_score")) / 2 + 0.0001)
        )
        
        # Calculate score difference
        score_diff_expr = expected_score_expr - col("ac_direct_score")
        
        # Mark conflict if:
        # - Expected score much higher than actual
        # - AND A-C score is below threshold
        has_conflict_expr = (
            (score_diff_expr > self.conflict_tolerance) &
            (col("ac_direct_score") < self.confidence_threshold)
        )
        
        result = (
            triplets_df
            .withColumn("expected_ac_score", expected_score_expr)
            .withColumn("score_difference", score_diff_expr)
            .withColumn("has_conflict", has_conflict_expr)
            .withColumn(
                "conflict_reason",
                when(has_conflict_expr, 
                    F.concat_ws(
                        "; ",
                        lit("Transitive violation"),
                        F.concat(lit("A-B: "), col("ab_score")),
                        F.concat(lit("B-C: "), col("bc_score")),
                        F.concat(lit("A-C expected: "), col("expected_ac_score")),
                        F.concat(lit("A-C actual: "), col("ac_direct_score"))
                    )
                ).otherwise(lit(""))
            )
            .withColumn(
                "conflict_type",
                when(has_conflict_expr, lit("TRANSITIVE")).otherwise(lit("NONE"))
            )
            .withColumn(
                "severity",
                when(has_conflict_expr,
                    (col("score_difference") / 1.0)  # Normalize severity
                ).otherwise(lit(0.0))
            )
        )
        
        return result
    
    def _detect_chains(self, valid_pairs_df: DataFrame) -> List[ConflictPath]:
        """
        Detect chains (sequences of connected pairs)
        
        A chain is problematic because:
        A → B → C → D → E
        
        May connect A with E even if they're very different
        """
        
        self.logger.info("Analyzing chain structure...")
        
        # Build adjacency from pairs
        pairs_list = (
            valid_pairs_df
            .select("id_i", "id_j", "composite_score")
            .collect()
        )
        
        # Build graph
        graph = defaultdict(list)
        for row in pairs_list:
            graph[row.id_i].append((row.id_j, row.composite_score))
            graph[row.id_j].append((row.id_i, row.composite_score))  # Undirected
        
        # Find chains using DFS
        chains = []
        visited_chains = set()
        
        for start_node in graph.keys():
            if start_node in visited_chains:
                continue
            
            # DFS to find longest path
            path, scores = self._dfs_find_longest_path(
                graph,
                start_node,
                max_depth=self.max_chain_length
            )
            
            if len(path) >= 3:  # Only care about chains of 3+
                path_key = tuple(sorted(path))
                if path_key not in visited_chains:
                    chains.append(ConflictPath(path, scores))
                    visited_chains.add(path_key)
        
        self.logger.info(f"Chain analysis complete: {len(chains)} chains found")
        
        return chains
    
    def _dfs_find_longest_path(
        self,
        graph: Dict,
        node: str,
        visited: Set[str] | None = None,
        path: List[str] | None = None,
        scores: List[float] | None = None,
        max_depth: int = 3
    ) -> Tuple[List[str], List[float]]:
        """DFS to find longest path in graph"""
        
        if visited is None:
            visited = set()
        if path is None:
            path = []
        if scores is None:
            scores = []
        
        visited.add(node)
        path.append(node)
        
        longest_path = path.copy()
        longest_scores = scores.copy()
        
        if len(path) <= max_depth:
            for neighbor, score in graph[node]:
                if neighbor not in visited:
                    new_path, new_scores = self._dfs_find_longest_path(
                        graph,
                        neighbor,
                        visited.copy(),
                        path + [neighbor],
                        scores + [score],
                        max_depth
                    )
                    
                    if len(new_path) > len(longest_path):
                        longest_path = new_path
                        longest_scores = new_scores
        
        return longest_path, longest_scores
    
    def _add_conflict_columns(
        self,
        original_df: DataFrame,
        conflict_df: DataFrame
    ) -> DataFrame:
        """Add conflict columns to original pairs"""
        
        # Get conflicts
        conflict_pairs = (
            conflict_df
            .filter(col("has_conflict"))
            .select(
                col("a").alias("id_i"),
                col("b").alias("id_j"),
                col("has_conflict"),
                col("conflict_reason"),
                col("conflict_type"),
                col("severity")
            )
        )
        
        # Left join with original
        result = (
            original_df
            .join(
                conflict_pairs,
                on=["id_i", "id_j"],
                how="left"
            )
            .fillna(False, subset=["has_conflict"])
            .fillna("", subset=["conflict_reason"])
            .fillna("NONE", subset=["conflict_type"])
            .fillna(0.0, subset=["severity"])
        )
        
        return result
    
    # ════════════════════════════════════════════════════════════════════════
    # RESOLUTION STRATEGIES
    # ════════════════════════════════════════════════════════════════════════
    
    def _resolve_remove_weakest(self, df: DataFrame) -> DataFrame:
        """
        Remove lowest-score pair from each conflict group
        Uses Window functions for efficient ranking
        """
        self.logger.info("Resolving conflicts: removing weakest pairs")
        
        if "has_conflict" not in df.columns:
            return df
        
        try:
            # Get conflicting pairs only
            conflicting = df.filter(col("has_conflict") == True)
            non_conflicting = df.filter(col("has_conflict") == False)
            
            if conflicting.count() == 0:
                return df
            
            # Rank pairs by score within conflict groups
            from pyspark.sql import Window
            
            windowed = conflicting.withColumn(
                "score_rank",
                row_number().over(
                    Window.partitionBy("conflict_reason")
                    .orderBy(col("composite_score").asc())  # Lowest first
                )
            )
            
            # Keep pairs that are NOT the weakest (rank > 1)
            kept_conflicts = windowed.filter(col("score_rank") > 1).drop("score_rank")
            
            # Combine with non-conflicting pairs
            result = kept_conflicts.union(non_conflicting).dropDuplicates()
            
            self.logger.debug(
                f"Removed {conflicting.count() - kept_conflicts.count()} weakest pairs"
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Remove weakest failed: {str(e)}")
            self.logger.warning("Falling back to keeping non-conflicting pairs")
            return df.filter(col("has_conflict") == False)


    
    def _resolve_remove_all(self, df: DataFrame) -> DataFrame:
        """Remove all pairs involved in conflicts"""
        self.logger.info("Resolving conflicts: removing all conflicting pairs")
        
        try:
            # Keep all metadata columns
            non_conflicting = df.filter(col("has_conflict") == False)
            
            removed_count = df.count() - non_conflicting.count()
            self.logger.debug(f"Removed {removed_count} conflicting pairs")
            
            return non_conflicting
            
        except Exception as e:
            self.logger.error(f"Remove all failed: {str(e)}")
            return df  # Return original

    
    def _resolve_adjust_confidence(self, df: DataFrame) -> DataFrame:
        """
        Lower confidence of conflicting pairs
        Keeps all pairs but marks them as unreliable
        """
        self.logger.info("Resolving conflicts: adjusting confidence levels")
        
        try:
            adjusted = (
                df
                .withColumn(
                    "original_confidence",
                    col("confidence_level")  # Track original
                )
                .withColumn(
                    "confidence_level",
                    when(
                        col("has_conflict") == True,
                        lit("LOW")  # Downgrade conflicting pairs
                    ).otherwise(col("confidence_level"))
                )
                .withColumn(
                    "confidence_adjusted",
                    col("has_conflict")  # Flag which were adjusted
                )
            )
            
            adjusted_count = adjusted.filter(col("confidence_adjusted")).count()
            self.logger.debug(f"Adjusted confidence for {adjusted_count} pairs")
            
            return adjusted
            
        except Exception as e:
            self.logger.error(f"Adjust confidence failed: {str(e)}")
            return df  # Return original

    
    def _resolve_break_chain(self, df: DataFrame) -> DataFrame:
        """
        Break conflict chains by removing intermediate pairs
        
        Example: A-B-C chain → remove B to break into A and C
        """
        self.logger.info("Resolving conflicts: breaking chains")
        
        if "has_conflict" not in df.columns:
            return df
        
        try:
            conflicting = df.filter(col("has_conflict") == True)
            non_conflicting = df.filter(col("has_conflict") == False)
            
            if conflicting.count() == 0:
                return df
            
            # Rank pairs within each conflict group
            from pyspark.sql import Window
            
            windowed = conflicting.withColumn(
                "chain_rank",
                row_number().over(
                    Window.partitionBy("conflict_reason")
                    .orderBy(col("composite_score").desc())
                )
            )
            
            # Keep only odd-numbered pairs (removes every other = breaks chains)
            broken_chains = windowed.filter((col("chain_rank") % 2) == 1).drop("chain_rank")
            
            result = broken_chains.union(non_conflicting).dropDuplicates()
            
            removed = conflicting.count() - broken_chains.count()
            self.logger.debug(f"Broke chains: removed {removed} intermediate pairs")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Break chain failed: {str(e)}")
            self.logger.warning("Falling back to remove weakest")
            return self._resolve_remove_weakest(df)

