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

Zero-score replacement
----------------------
When ``scoring.zero_score_replacement`` is set in config.yaml, any pair whose
overall_pair_score computes to exactly 0.0 is lifted to that value instead.
The recommended value is 0.74999 — just below the 0.75 match threshold — so
the pair is never auto-merged but rises to the top of the manual-review queue.
Set the key to null or remove it to keep raw 0.0 scores.

Compound rules are aggregated as:
  OR  → max(sub_scores)   — best contributing signal wins
  AND → mean(sub_scores) when all sub-scores ≥ 0.75 (all groups pass their
            threshold), so above-threshold quality is rewarded;
        mean_of_nonzero(sub_scores) × 0.74999  when any sub-score < 0.75
            (a required group failed its threshold).  Zero sub-scores are
            excluded from the mean so they act as a hard gate (pulling the
            result into the failing window via the ×0.74999 multiplier) without
            dragging down the mean contribution of the remaining strong groups.
            A same-supplier pair (G6 veto → 0) with excellent name/geo signals
            scores near 0.65 rather than collapsing to 0, making it clearly
            visible at the top of the manual-review queue.

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


def _load_scoring_config(config_path: Optional[str] = None) -> dict:
    path = config_path or _find_config_path()
    with open(path, "r") as fh:
        cfg = yaml.safe_load(fh)
    return cfg["scoring"]


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
            score_expr = F.lit(1.0)
        elif t >= 1.0:
            # Threshold is already the maximum possible; score is linear 0→0.75.
            score_expr = F.greatest(F.lit(0.0), F.lit(0.75) * c / F.lit(t))
        else:
            # two-segment linear:
            #   [0, t]  →  [0.00, 0.75]   slope = 0.75 / t
            #   [t, 1]  →  [0.75, 1.00]   slope = 0.25 / (1 - t)
            score_expr = F.least(
                F.lit(1.0),
                F.when(
                    c >= t,
                    F.lit(0.75) + F.lit(0.25) * (c - F.lit(t)) / F.lit(1.0 - t),
                ).otherwise(
                    F.greatest(F.lit(0.0), F.lit(0.75) * c / F.lit(t)),
                ),
            )
    else:  # lte — lower is better (e.g. geo_distance_km)
        if t <= 0.0:
            # No gradient threshold to score against — use inverted value: 1 - c.
            # Lower value → higher contribution (e.g. supplier_score=0 → 1.0, =1 → 0.0).
            # Clamped to [0, 1] to guard against values outside that range.
            score_expr = F.greatest(F.lit(0.0), F.least(F.lit(1.0), F.lit(1.0) - c))
        else:
            # two-segment linear:
            #   [0,  t]  →  [1.00, 0.75]  (the closer to 0 the better)
            #   [t, 2t]  →  [0.75, 0.00]  (past threshold, decays to 0 at 2*t)
            score_expr = F.greatest(
                F.lit(0.0),
                F.when(
                    c <= t,
                    F.lit(0.75) + F.lit(0.25) * (F.lit(t) - c) / F.lit(t),
                ).otherwise(
                    F.lit(0.75) * (F.lit(1.0) - (c - F.lit(t)) / F.lit(t)),
                ),
            )

    return score_expr.cast("float")


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
        if len(sub_exprs) == 1:
            return sub_exprs[0].cast("float")
        return F.greatest(*sub_exprs).cast("float")

    # AND — mean when all pass (rewards quality);
    #       mean_of_nonzero × 0.74999 when any fail — zero sub-scores are
    #       excluded from the mean so they act as a hard gate without dragging
    #       down the contribution of strong groups.  The ×0.74999 multiplier
    #       guarantees the pair never crosses the 0.75 merge threshold.
    else:
        n = len(sub_exprs)
        all_pass = reduce(
            lambda a, b: a & b,
            [e >= F.lit(0.75) for e in sub_exprs],
        )
        mean_all = reduce(lambda a, b: a + b, sub_exprs) / F.lit(float(n))
        # Failing branch: mean over non-zero sub-scores only
        nz_sum = reduce(
            lambda a, b: a + b,
            [F.when(e > F.lit(0.0), e).otherwise(F.lit(0.0)) for e in sub_exprs],
        )
        nz_count = reduce(
            lambda a, b: a + b,
            [
                F.when(e > F.lit(0.0), F.lit(1.0)).otherwise(F.lit(0.0))
                for e in sub_exprs
            ],
        )
        mean_nz = nz_sum / F.when(nz_count > F.lit(0.0), nz_count).otherwise(F.lit(1.0))
        return (
            F.when(all_pass, mean_all).otherwise(mean_nz * F.lit(0.74999)).cast("float")
        )


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

# Module-level cache so config.yaml is read only once per Spark executor process.
# Set to None to force a rebuild after any code or config change.
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
        scoring_cfg = _load_scoring_config(config_path)
        expr = _rule_to_expr(scoring_cfg["match_logic"])
        # Apply zero_score_replacement if configured: pairs that score exactly
        # 0.0 are lifted to just below the match threshold so they surface in
        # manual-review queues rather than being invisible at absolute zero.
        replacement = scoring_cfg.get("zero_score_replacement")
        if replacement is not None:
            expr = (
                F.when(expr == F.lit(0.0), F.lit(float(replacement)))
                .otherwise(expr)
                .cast("float")
            )
        _CACHED_EXPR = expr
    return _CACHED_EXPR


def invalidate_cache() -> None:
    """Force rebuild of the cached expression on next call (e.g. after config change)."""
    global _CACHED_EXPR
    _CACHED_EXPR = None
