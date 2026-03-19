"""
Tests for overall_pair_score computation.
==========================================
Tests mirror the PySpark scoring logic using a pure-Python evaluator so that
no running SparkSession is required.  The pure-Python evaluator is intentionally
inlined here (not imported from the scorer) so that the tests remain isolated and
act as a specification of the expected behaviour.

The config used below matches the current state of config.yaml:

  scoring.match_logic (AND of four groups):
    Group 1 (OR): name / normalised-name signals with various thresholds
    Group 2 (OR): address_line1_score | address_sbert_score | geo_distance_km (lte)
    Group 3 (leaf): property_type_score >= 0.8
    Group 4 (pair of leaves, expanded as AND): name_unit_score >= 0.5  and
                                                address_unit_score >= 0.5

Score anchoring:
  gte comparator:  value=0 → 0.0,  value=threshold → 0.75,  value=1.0 → 1.0
  lte comparator:  value=0 → 1.0,  value=threshold → 0.75,  value=2*t → 0.0
"""

import unittest
from typing import Dict, Any


# ─────────────────────────────────────────────────────────────────────────────
# Pure-Python scorer (mirrors overall_pair_scorer.py logic)
# ─────────────────────────────────────────────────────────────────────────────


def _signal_score(value: float, threshold: float, comparator: str) -> float:
    t = float(threshold)
    v = float(value)

    if comparator == "gte":
        if t <= 0.0:
            return 1.0
        if t >= 1.0:
            return max(0.0, 0.75 * v / t)
        if v >= t:
            return min(1.0, 0.75 + 0.25 * (v - t) / (1.0 - t))
        return max(0.0, 0.75 * v / t)

    else:  # lte — lower is better
        if t <= 0.0:
            return 1.0 if v == 0 else 0.0
        if v <= t:
            return 0.75 + 0.25 * (t - v) / t
        return max(0.0, 0.75 * (1.0 - (v - t) / t))


def _rule_score(rule: dict, pair: Dict[str, Any]) -> float:
    """Recursively evaluate a match_logic rule against a score dict."""
    if "signal" in rule:
        val = float(pair.get(rule["signal"], 0.0))
        return _signal_score(val, rule["threshold"], rule["comparator"])

    operator = rule["operator"].upper()
    sub_scores = [_rule_score(r, pair) for r in rule["rules"]]

    if operator == "OR":
        return max(sub_scores)
    else:  # AND — mean when all pass (>=0.75); min when any group fails
        if all(s >= 0.75 for s in sub_scores):
            return sum(sub_scores) / len(sub_scores)
        return min(sub_scores)


# ─────────────────────────────────────────────────────────────────────────────
# Inline config (mirrors current config.yaml scoring.match_logic)
# ─────────────────────────────────────────────────────────────────────────────

_MATCH_LOGIC = {
    "operator": "AND",
    "rules": [
        # Group 1: name / normalised-name signals (OR)
        {
            "operator": "OR",
            "rules": [
                {
                    "signal": "name_score_containment",
                    "threshold": 0.95,
                    "comparator": "gte",
                },
                {
                    "signal": "name_score_jaccard",
                    "threshold": 0.90,
                    "comparator": "gte",
                },
                {"signal": "name_score_lcs", "threshold": 0.90, "comparator": "gte"},
                {"signal": "name_score_sbert", "threshold": 0.90, "comparator": "gte"},
                {
                    "signal": "name_score_levenshtein",
                    "threshold": 0.90,
                    "comparator": "gte",
                },
                {
                    "signal": "normalized_name_score_jaccard",
                    "threshold": 0.95,
                    "comparator": "gte",
                },
                {
                    "signal": "normalized_name_score_lcs",
                    "threshold": 0.95,
                    "comparator": "gte",
                },
                {
                    "signal": "normalized_name_score_levenshtein",
                    "threshold": 0.95,
                    "comparator": "gte",
                },
                {
                    "signal": "normalized_name_score_sbert",
                    "threshold": 0.95,
                    "comparator": "gte",
                },
                {
                    "signal": "average_name_score",
                    "threshold": 0.80,
                    "comparator": "gte",
                },
                {
                    "signal": "average_normalized_name_score",
                    "threshold": 0.90,
                    "comparator": "gte",
                },
                {
                    "operator": "AND",
                    "rules": [
                        {
                            "signal": "normalized_name_score_containment",
                            "threshold": 1.0,
                            "comparator": "gte",
                        },
                        {
                            "signal": "name_score_sbert",
                            "threshold": 0.75,
                            "comparator": "gte",
                        },
                        {
                            "signal": "geo_distance_km",
                            "threshold": 0.2,
                            "comparator": "lte",
                        },
                    ],
                },
            ],
        },
        # Group 2: address / geo signal (OR)
        {
            "operator": "OR",
            "rules": [
                {
                    "signal": "address_line1_score",
                    "threshold": 0.5,
                    "comparator": "gte",
                },
                {
                    "signal": "address_sbert_score",
                    "threshold": 0.5,
                    "comparator": "gte",
                },
                {"signal": "geo_distance_km", "threshold": 0.2, "comparator": "lte"},
            ],
        },
        # Group 3: property type
        {"signal": "property_type_score", "threshold": 0.8, "comparator": "gte"},
        # Group 4a: name unit
        {"signal": "name_unit_score", "threshold": 0.5, "comparator": "gte"},
        # Group 4b: address unit
        {"signal": "address_unit_score", "threshold": 0.5, "comparator": "gte"},
    ],
}


def overall_pair_score(pair: Dict[str, Any]) -> float:
    """Compute overall_pair_score from a plain signal dict."""
    return round(_rule_score(_MATCH_LOGIC, pair), 6)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: build a baseline pair where every score is exactly at its threshold
# ─────────────────────────────────────────────────────────────────────────────


def _at_threshold() -> Dict[str, float]:
    """All signals set to their config thresholds → expected score = 0.75."""
    return {
        # Group 1 — any one signal at its threshold is enough for the OR
        "name_score_containment": 0.95,
        "name_score_jaccard": 0.90,
        "name_score_lcs": 0.90,
        "name_score_sbert": 0.90,
        "name_score_levenshtein": 0.90,
        "normalized_name_score_jaccard": 0.95,
        "normalized_name_score_lcs": 0.95,
        "normalized_name_score_levenshtein": 0.95,
        "normalized_name_score_sbert": 0.95,
        "average_name_score": 0.80,
        "average_normalized_name_score": 0.90,
        "normalized_name_score_containment": 1.00,
        # Group 2
        "address_line1_score": 0.50,
        "address_sbert_score": 0.50,
        "geo_distance_km": 0.20,
        # Group 3
        "property_type_score": 0.80,
        # Group 4
        "name_unit_score": 0.50,
        "address_unit_score": 0.50,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Test helper
# ─────────────────────────────────────────────────────────────────────────────


def _approx(a: float, b: float, tol: float = 1e-4) -> bool:
    return abs(a - b) <= tol


class TestSignalScore(unittest.TestCase):
    """Unit tests for the leaf signal scoring function."""

    # ── gte comparator ──────────────────────────────────────────────────────

    def test_gte_at_threshold_yields_0_75(self):
        # e.g. name_score_jaccard = 0.90 with threshold 0.90
        self.assertTrue(_approx(_signal_score(0.90, 0.90, "gte"), 0.75))

    def test_gte_at_zero_yields_0(self):
        self.assertTrue(_approx(_signal_score(0.0, 0.90, "gte"), 0.0))

    def test_gte_at_1_yields_1(self):
        self.assertTrue(_approx(_signal_score(1.0, 0.90, "gte"), 1.0))

    def test_gte_midway_below_threshold(self):
        # value = threshold/2 → sub-score = 0.75/2 = 0.375
        self.assertTrue(_approx(_signal_score(0.45, 0.90, "gte"), 0.375))

    def test_gte_midway_above_threshold(self):
        # value = (threshold + 1) / 2 = 0.95, threshold=0.9
        # sub-score = 0.75 + 0.25 * (0.95 - 0.90) / (1 - 0.90) = 0.875
        self.assertTrue(_approx(_signal_score(0.95, 0.90, "gte"), 0.875))

    def test_gte_threshold_is_1_clamps_to_0_75(self):
        # Threshold at 1.0 → best achievable score is 0.75
        self.assertTrue(_approx(_signal_score(1.0, 1.0, "gte"), 0.75))

    def test_gte_threshold_zero_yields_1(self):
        # Trivial threshold → always 1.0
        self.assertEqual(_signal_score(0.5, 0.0, "gte"), 1.0)

    # ── lte comparator ──────────────────────────────────────────────────────

    def test_lte_at_zero_yields_1(self):
        self.assertTrue(_approx(_signal_score(0.0, 0.20, "lte"), 1.0))

    def test_lte_at_threshold_yields_0_75(self):
        self.assertTrue(_approx(_signal_score(0.20, 0.20, "lte"), 0.75))

    def test_lte_at_double_threshold_yields_0(self):
        self.assertTrue(_approx(_signal_score(0.40, 0.20, "lte"), 0.0))

    def test_lte_beyond_double_threshold_clamps_to_0(self):
        self.assertTrue(_approx(_signal_score(1.0, 0.20, "lte"), 0.0))

    def test_lte_midway_below_threshold(self):
        # value = 0.1, threshold=0.2  → 0.75 + 0.25*(0.2-0.1)/0.2 = 0.875
        self.assertTrue(_approx(_signal_score(0.10, 0.20, "lte"), 0.875))

    def test_lte_midway_above_threshold(self):
        # value = 0.3, threshold=0.2  → 0.75*(1-(0.3-0.2)/0.2) = 0.375
        self.assertTrue(_approx(_signal_score(0.30, 0.20, "lte"), 0.375))


class TestOverallPairScore(unittest.TestCase):
    """Integration tests: full overall_pair_score for realistic hotel pairs."""

    # ── Baseline: all signals at threshold → 0.76 ───────────────────────────
    # With the hybrid AND, every group scores exactly 0.75 (threshold)…
    # except: name_score_sbert=0.90 appears in both the outer OR (threshold=0.90,
    # scores 0.75 there) and the inner AND sub-rule (threshold=0.75, scores 0.90
    # there because 0.90 > 0.75).  The inner AND becomes mean(0.75, 0.90, 0.75)=0.80,
    # which wins Group1 OR.  All groups are still ≥ 0.75 → mean applies:
    # mean(0.80, 0.75, 0.75, 0.75, 0.75) = 0.76.

    def test_all_at_threshold_yields_0_75(self):
        pair = _at_threshold()
        s = overall_pair_score(pair)
        self.assertTrue(
            _approx(s, 0.76, tol=1e-3),
            f"Expected 0.76 when all signals are at threshold (sbert cross-effect), got {s}",
        )

    # ── Perfect match: all signals at 1.0 / best possible ───────────────────

    def test_perfect_match_yields_close_to_1(self):
        pair = {
            "name_score_containment": 1.0,
            "name_score_jaccard": 1.0,
            "name_score_lcs": 1.0,
            "name_score_sbert": 1.0,
            "name_score_levenshtein": 1.0,
            "normalized_name_score_jaccard": 1.0,
            "normalized_name_score_lcs": 1.0,
            "normalized_name_score_levenshtein": 1.0,
            "normalized_name_score_sbert": 1.0,
            "average_name_score": 1.0,
            "average_normalized_name_score": 1.0,
            "normalized_name_score_containment": 1.0,
            "address_line1_score": 1.0,
            "address_sbert_score": 1.0,
            "geo_distance_km": 0.0,
            "property_type_score": 1.0,
            "name_unit_score": 1.0,
            "address_unit_score": 1.0,
        }
        s = overall_pair_score(pair)
        self.assertGreater(s, 0.95, f"Perfect match should be > 0.95, got {s}")

    # ── Strong name, address at threshold ────────────────────────────────────

    def test_strong_name_address_at_threshold_above_0_75(self):
        pair = _at_threshold()
        # Boost all name scores to near-perfect
        for k in [
            "name_score_containment",
            "name_score_jaccard",
            "name_score_lcs",
            "name_score_sbert",
            "name_score_levenshtein",
            "normalized_name_score_jaccard",
            "normalized_name_score_lcs",
            "normalized_name_score_levenshtein",
            "normalized_name_score_sbert",
            "average_name_score",
            "average_normalized_name_score",
        ]:
            pair[k] = 1.0
        s = overall_pair_score(pair)
        # All groups pass ≥ 0.75 → mean kicks in; Group1 = 1.0 lifts the overall
        # above 0.75.  mean(1.0, 0.75, 0.75, 0.75, 0.75) = 0.80 > 0.75.
        self.assertGreater(s, 0.75, f"Strong name match should exceed 0.75, got {s}")

    # ── Weak signals: property_type_score below threshold ────────────────────

    def test_property_type_below_threshold_reduces_score(self):
        pair_pass = _at_threshold()
        pair_weak = _at_threshold()
        pair_weak["property_type_score"] = 0.4  # well below 0.8 threshold
        s_pass = overall_pair_score(pair_pass)
        s_weak = overall_pair_score(pair_weak)
        self.assertLess(
            s_weak, s_pass, "Weak property_type_score should lower overall score"
        )

    # ── Only geo_distance_km passes address group, nothing else ─────────────

    def test_address_group_via_geo_only(self):
        pair = _at_threshold()
        pair["address_line1_score"] = 0.0
        pair["address_sbert_score"] = 0.0
        # perfect geo → address OR passes via geo
        pair["geo_distance_km"] = 0.0
        s = overall_pair_score(pair)
        # All groups pass ≥ 0.75 → mean; Group2 geo = 1.0 lifts the overall.
        # mean(0.75, 1.0, 0.75, 0.75, 0.75) = 0.80 > 0.75.
        self.assertGreater(s, 0.75, f"Perfect geo should yield score > 0.75, got {s}")

    # ── Address group fails (no signal passes) ───────────────────────────────

    def test_address_group_failing_reduces_score(self):
        pair_pass = _at_threshold()
        pair_fail = _at_threshold()
        pair_fail["address_line1_score"] = 0.0
        pair_fail["address_sbert_score"] = 0.0
        pair_fail["geo_distance_km"] = 5.0  # far away
        s_pass = overall_pair_score(pair_pass)
        s_fail = overall_pair_score(pair_fail)
        self.assertLess(s_fail, s_pass, "Failing address group should lower score")

    # ── name_unit_score below threshold ──────────────────────────────────────

    def test_weak_name_unit_score_reduces_overall(self):
        pair_pass = _at_threshold()
        pair_weak = _at_threshold()
        pair_weak["name_unit_score"] = 0.0
        s_pass = overall_pair_score(pair_pass)
        s_weak = overall_pair_score(pair_weak)
        self.assertLess(
            s_weak, s_pass, "Weak name_unit_score should lower overall score"
        )

    # ── address_unit_score below threshold ───────────────────────────────────

    def test_weak_address_unit_score_reduces_overall(self):
        pair_pass = _at_threshold()
        pair_weak = _at_threshold()
        pair_weak["address_unit_score"] = 0.0
        s_pass = overall_pair_score(pair_pass)
        s_weak = overall_pair_score(pair_weak)
        self.assertLess(
            s_weak, s_pass, "Weak address_unit_score should lower overall score"
        )

    # ── Name group: only one signal passes but barely ────────────────────────

    def test_name_group_only_containment_at_threshold(self):
        pair = _at_threshold()
        # Zero out all name signals except containment which stays at threshold
        for k in [
            "name_score_jaccard",
            "name_score_lcs",
            "name_score_sbert",
            "name_score_levenshtein",
            "normalized_name_score_jaccard",
            "normalized_name_score_lcs",
            "normalized_name_score_levenshtein",
            "normalized_name_score_sbert",
            "average_name_score",
            "average_normalized_name_score",
        ]:
            pair[k] = 0.0
        pair["name_score_containment"] = 0.95  # exactly at threshold
        s = overall_pair_score(pair)
        # OR group picks max; containment at threshold → 0.75
        # Overall is AND of all groups — all others at threshold → overall ~ 0.75
        self.assertTrue(_approx(s, 0.75, tol=0.01), f"Expected ~0.75, got {s}")

    # ── Composite AND rule inside name group (normalized_name_containment = 1,
    #    name_sbert = 0.75, geo_distance_km = 0.2) ─────────────────────────

    def test_inner_and_rule_at_threshold_contributes_0_75(self):
        pair = {sig: 0.0 for sig in _at_threshold()}
        # Fire only via the nested AND branch in Group 1
        pair["normalized_name_score_containment"] = 1.00
        pair["name_score_sbert"] = 0.75
        pair["geo_distance_km"] = 0.20
        # Address group: pass via geo
        pair["address_line1_score"] = 0.0
        pair["address_sbert_score"] = 0.0
        # Other mandatory signals at threshold
        pair["property_type_score"] = 0.80
        pair["name_unit_score"] = 0.50
        pair["address_unit_score"] = 0.50

        group1_and_score = _rule_score(
            {
                "operator": "AND",
                "rules": [
                    {
                        "signal": "normalized_name_score_containment",
                        "threshold": 1.0,
                        "comparator": "gte",
                    },
                    {
                        "signal": "name_score_sbert",
                        "threshold": 0.75,
                        "comparator": "gte",
                    },
                    {
                        "signal": "geo_distance_km",
                        "threshold": 0.2,
                        "comparator": "lte",
                    },
                ],
            },
            pair,
        )
        self.assertTrue(
            _approx(group1_and_score, 0.75, tol=0.01),
            f"Inner AND at thresholds should yield ~0.75, got {group1_and_score}",
        )

    # ── All signals at worst possible value → very low score ─────────────────
    # For gte signals worst = 0.0; for lte signals worst = a very large value.
    # Setting geo_distance_km=0.0 would be *perfect* (distance = 0 km), which
    # inadvertently lifts both the Group-1 inner-AND and the Group-2 OR via geo.
    # Use 100.0 km to represent a clearly failing geo condition.

    def test_all_signals_zero_yields_low_score(self):
        pair = {sig: 0.0 for sig in _at_threshold()}
        pair["geo_distance_km"] = 100.0  # worst case for lte — very far apart
        s = overall_pair_score(pair)
        self.assertLess(
            s, 0.1, f"All-worst signals should give very low score, got {s}"
        )

    # ── Score is monotone: better signals → higher overall ──────────────────

    def test_score_increases_with_better_signals(self):
        base = _at_threshold()
        better = _at_threshold()
        better["name_score_jaccard"] = 1.0
        better["address_line1_score"] = 1.0
        better["property_type_score"] = 1.0
        s_base = overall_pair_score(base)
        s_better = overall_pair_score(better)
        self.assertGreaterEqual(
            s_better, s_base, "Improving signals must not lower overall score"
        )

    # ── Score is monotone: worse signals → lower overall ────────────────────

    def test_score_decreases_with_worse_signals(self):
        base = _at_threshold()
        worse = _at_threshold()
        worse["name_score_jaccard"] = 0.5
        worse["address_line1_score"] = 0.2
        worse["property_type_score"] = 0.5
        s_base = overall_pair_score(base)
        s_worse = overall_pair_score(worse)
        self.assertLessEqual(
            s_worse, s_base, "Worsening signals must not raise overall score"
        )

    # ── Concrete pair: property_type above threshold lifts overall above 0.75 ─
    # name_score_containment=0.95  → exactly at threshold → 0.75  (wins Group1 OR)
    # geo_distance_km=0.2          → exactly at threshold → 0.75  (Group2 OR)
    # property_type_score=0.90     → above 0.80 threshold → 0.875 (Group3)
    # name_unit_score=0.5          → exactly at threshold → 0.75  (Group4a)
    # address_unit_score=0.5       → exactly at threshold → 0.75  (Group4b)
    # All groups ≥ 0.75 → mean applies:
    # mean(0.75, 0.75, 0.875, 0.75, 0.75) = 0.775
    # property_type_score scoring above threshold DOES lift the overall because
    # all groups pass and the hybrid AND uses mean for passing cases.

    def test_containment_at_threshold_with_high_property_yields_0_775(self):
        pair = {
            "name_score_containment": 0.95,
            "name_score_jaccard": 0.80,
            "normalized_name_score_jaccard": 0.90,
            "average_name_score": 0.79,
            "geo_distance_km": 0.20,
            "property_type_score": 0.90,
            "name_unit_score": 0.50,
            "address_unit_score": 0.50,
        }
        s = overall_pair_score(pair)
        self.assertTrue(
            _approx(s, 0.775, tol=1e-3),
            f"Expected 0.775 (property_type above threshold lifts overall), got {s}",
        )

    # ── Concrete pair: best name signal just misses threshold → overall < 0.75
    # name_score_containment=0.94  → below 0.95 threshold → 0.75*0.94/0.95 ≈ 0.7421
    #   (this is the best Group1 signal; all others score lower)
    # geo_distance_km=0.2  → at threshold → 0.75  (Group2)
    # property_type_score=0.9 → above threshold → 0.875  (Group3)
    # name/address_unit_score=0.5 → at threshold → 0.75 each
    # With min-AND: min(0.7421, 0.75, 0.875, 0.75, 0.75) = 0.7421 < 0.75
    # Even though property_type scored well above threshold, missing the name
    # group by a tiny margin drives the overall below 0.75.

    def test_containment_just_below_threshold_yields_below_0_75(self):
        pair = {
            "name_score_containment": 0.94,
            "name_score_jaccard": 0.80,
            "normalized_name_score_jaccard": 0.90,
            "average_name_score": 0.78,
            "geo_distance_km": 0.20,
            "property_type_score": 0.90,
            "name_unit_score": 0.50,
            "address_unit_score": 0.50,
        }
        s = overall_pair_score(pair)
        expected = round(0.75 * 0.94 / 0.95, 6)  # ≈ 0.742105
        self.assertTrue(
            _approx(s, expected, tol=1e-3),
            f"Expected ~{expected} (name just below threshold), got {s}",
        )
        self.assertLess(s, 0.75, "Missing name threshold must drive overall below 0.75")

    # ── Score is always in [0, 1] ────────────────────────────────────────────

    def test_score_clamped_between_0_and_1(self):
        for pair in [
            _at_threshold(),
            {sig: 0.0 for sig in _at_threshold()},
            {sig: 1.0 for sig in _at_threshold()},
        ]:
            s = overall_pair_score(pair)
            self.assertGreaterEqual(s, 0.0, f"Score {s} below 0")
            self.assertLessEqual(s, 1.0, f"Score {s} above 1")


if __name__ == "__main__":
    unittest.main()
