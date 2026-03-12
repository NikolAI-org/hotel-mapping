"""
Overall Pair Score Builder
==========================
Reads ``scoring.match_logic`` from config.yaml **once** (cached at module
level) and returns a pure PySpark Column expression for ``overall_pair_score``.

Scoring principle
-----------------
For every leaf condition in config.yaml the sub-score is anchored as:

  gte comparator (higher is better, e.g. name_score_jaccard):
    value = 0           → sub_score = 0.0
    value = threshold   → sub_score = 0.75  (just passing)
    value = 1.0         → sub_score = 1.0

  lte comparator (lower is better, e.g. geo_distance_km):
    value = 0           → sub_score = 1.0   (perfect)
    value = threshold   → sub_score = 0.75  (just passing)
    value = 2*threshold → sub_score = 0.0   (too far)

Compound rules are aggregated as:
  OR  → max(sub_scores)   — best contributing signal wins
  AND → mean(sub_scores) when all sub-scores ≥ 0.75 (all groups pass their
            threshold), so above-threshold quality is rewarded;
        min(sub_scores)  when any sub-score < 0.75 (a required group failed
            its threshold), so the weakest group gates the overall score
            and drives it below 0.75.

Result is clamped to [0.0, 1.0] and cast to float.
"""

import os
from functools import reduce
from typing import Optional

import yaml
from pyspark.sql import Column
from pyspark.sql import functions as F


# ─────────────────────────────────────────────────────────────────────────────
# Config loading
# ─────────────────────────────────────────────────────────────────────────────

def _find_config_path() -> str:
    """Resolve config.yaml relative to the hotel_data package root."""
    import hotel_data
    return os.path.join(os.path.dirname(hotel_data.__file__), "config", "config.yaml")


def _load_match_logic(config_path: Optional[str] = None) -> dict:
    path = config_path or _find_config_path()
    with open(path, "r") as fh:
        cfg = yaml.safe_load(fh)
    return cfg["scoring"]["match_logic"]


# ─────────────────────────────────────────────────────────────────────────────
# Signal → sub-score expression
# ─────────────────────────────────────────────────────────────────────────────

def _signal_score_expr(signal: str, threshold: float, comparator: str) -> Column:
    """
    Returns a PySpark Column expression mapping a raw signal value to [0, 1]
    such that value==threshold → 0.75, value==best → 1.0, value==0/worst → 0.0.
    """
    c = F.col(signal).cast("double")
    t = float(threshold)

    if comparator == "gte":
        # Higher is better. Best = 1.0, pass = threshold, worst = 0.
        if t <= 0.0:
            # Any positive value is fine; threshold is trivially met.
            return F.lit(1.0).cast("float")

        if t >= 1.0:
            # Threshold is already the maximum possible; score is linear 0→0.75.
            return F.greatest(F.lit(0.0), F.lit(0.75) * c / F.lit(t)).cast("float")

        # two-segment linear:
        #   [0, t]  →  [0.00, 0.75]   slope = 0.75 / t
        #   [t, 1]  →  [0.75, 1.00]   slope = 0.25 / (1 - t)
        return F.least(
            F.lit(1.0),
            F.when(
                c >= t,
                F.lit(0.75) + F.lit(0.25) * (c - F.lit(t)) / F.lit(1.0 - t),
            ).otherwise(
                F.greatest(F.lit(0.0), F.lit(0.75) * c / F.lit(t)),
            ),
        ).cast("float")

    else:  # lte — lower is better (e.g. geo_distance_km)
        if t <= 0.0:
            return F.when(c == 0, F.lit(1.0)).otherwise(F.lit(0.0)).cast("float")

        # two-segment linear:
        #   [0,  t]  →  [1.00, 0.75]  (the closer to 0 the better)
        #   [t, 2t]  →  [0.75, 0.00]  (past threshold, decays to 0 at 2*t)
        return F.greatest(
            F.lit(0.0),
            F.when(
                c <= t,
                F.lit(0.75) + F.lit(0.25) * (F.lit(t) - c) / F.lit(t),
            ).otherwise(
                F.lit(0.75) * (F.lit(1.0) - (c - F.lit(t)) / F.lit(t)),
            ),
        ).cast("float")


# ─────────────────────────────────────────────────────────────────────────────
# Recursive rule → Column expression
# ─────────────────────────────────────────────────────────────────────────────

def _rule_to_expr(rule: dict) -> Column:
    """Recursively convert a match_logic rule node to a PySpark Column expression."""
    if "signal" in rule:
        # Leaf node
        return _signal_score_expr(
            rule["signal"],
            rule["threshold"],
            rule["comparator"],
        )

    operator = rule["operator"].upper()
    sub_exprs = [_rule_to_expr(r) for r in rule["rules"]]

    if operator == "OR":
        # Best contributor wins
        return F.greatest(*sub_exprs).cast("float")

    # AND — mean when all pass (rewards quality); min when any fail (gates result)
    else:
        n = len(sub_exprs)
        all_pass = reduce(
            lambda a, b: a & b,
            [e >= F.lit(0.75) for e in sub_exprs],
        )
        mean_val = reduce(lambda a, b: a + b, sub_exprs) / F.lit(float(n))
        return F.when(all_pass, mean_val).otherwise(F.least(*sub_exprs)).cast("float")


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

# Module-level cache so config.yaml is read only once per Spark executor process.
_CACHED_EXPR: Optional[Column] = None


def build_overall_pair_score_expr(config_path: Optional[str] = None) -> Column:
    """
    Return a PySpark Column expression for ``overall_pair_score``.

    Parameters
    ----------
    config_path:
        Explicit path to config.yaml.  If *None*, resolved automatically
        relative to the ``hotel_data`` package root.

    Returns
    -------
    Column
        Float column in [0, 1] where 0.75 means "just meets configured
        thresholds", values above 0.75 indicate better-than-required matches,
        and values below 0.75 indicate weaker matches.
    """
    global _CACHED_EXPR
    if _CACHED_EXPR is None:
        match_logic = _load_match_logic(config_path)
        _CACHED_EXPR = _rule_to_expr(match_logic)
    return _CACHED_EXPR
