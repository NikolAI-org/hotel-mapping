import sys
import types
import unittest

# name_utils imports fuzzywuzzy and pyspark at module import time.
if "fuzzywuzzy" not in sys.modules:
    fuzzywuzzy_mod = types.ModuleType("fuzzywuzzy")

    class _Fuzz:
        @staticmethod
        def ratio(_a, _b):
            return 0

        @staticmethod
        def partial_ratio(_a, _b):
            return 0

    fuzzywuzzy_mod.fuzz = _Fuzz
    sys.modules["fuzzywuzzy"] = fuzzywuzzy_mod

if "pyspark" not in sys.modules:
    sys.modules["pyspark"] = types.ModuleType("pyspark")

if "pyspark.sql" not in sys.modules:
    sql_mod = types.ModuleType("pyspark.sql")

    class _DataFrame:
        pass

    class _Column:
        pass

    sql_mod.DataFrame = _DataFrame
    sql_mod.Column = _Column
    sys.modules["pyspark.sql"] = sql_mod

if "pyspark.sql.functions" not in sys.modules:
    functions_mod = types.ModuleType("pyspark.sql.functions")

    def _noop(*_args, **_kwargs):
        return None

    for _name in ("col", "trim", "regexp_replace", "udf"):
        setattr(functions_mod, _name, _noop)
    sys.modules["pyspark.sql.functions"] = functions_mod

if "pyspark.sql.types" not in sys.modules:
    sys.modules["pyspark.sql.types"] = types.ModuleType("pyspark.sql.types")

functions_mod = sys.modules["pyspark.sql.functions"]
types_mod = sys.modules["pyspark.sql.types"]

if not hasattr(functions_mod, "udf"):

    def _identity_udf(fn, _return_type=None):
        return fn

    functions_mod.udf = _identity_udf

if not hasattr(functions_mod, "col"):

    def _identity_col(name):
        return name

    functions_mod.col = _identity_col

if not hasattr(types_mod, "FloatType"):

    class _FloatType:
        pass

    types_mod.FloatType = _FloatType

if not hasattr(types_mod, "StringType"):

    class _StringType:
        pass

    types_mod.StringType = _StringType

from hotel_data.pipeline.preprocessor.processors.name_formatter_processor import (
    _strip_trailing_landmark_clause,
)
from hotel_data.pipeline.preprocessor.utils.name_utils import (
    _name_residual_score,
    enhanced_name_scorer,
)

CONTAINMENT_ALGO = "containment"


class TestLandmarkClauseFalsePositiveRegression(unittest.TestCase):
    """
    Regression: Carlton Hotel used Taj Mahal Palace as a directional landmark.
    After stripping the trailing landmark clause, the normalized names of these
    two completely different properties must NOT score high on containment.
    """

    _RAW_NAME_A = "carlton hotel mumbai - behind taj mahal palace colaba mumbai"
    _RAW_NAME_B = "the taj mahal palace, mumbai"

    def _normalize(self, name: str) -> str:
        return _strip_trailing_landmark_clause(name)

    def test_raw_names_give_high_containment_without_normalization(self):
        # Demonstrates why the fix is needed: raw names give a misleadingly high score.
        score = enhanced_name_scorer(
            self._RAW_NAME_A, self._RAW_NAME_B, CONTAINMENT_ALGO, True
        )
        self.assertGreaterEqual(score, 0.9)

    def test_normalized_names_give_low_containment_after_landmark_strip(self):
        norm_a = self._normalize(self._RAW_NAME_A)
        norm_b = self._normalize(self._RAW_NAME_B)
        # norm_a → "carlton hotel mumbai", norm_b → "the taj mahal palace, mumbai"
        # No shared content-bearing tokens → containment must be well below threshold.
        score = enhanced_name_scorer(norm_a, norm_b, CONTAINMENT_ALGO, True)
        self.assertLess(score, 0.5)

    def test_landmark_clause_is_stripped_correctly(self):
        self.assertEqual(self._normalize(self._RAW_NAME_A), "carlton hotel mumbai")
        self.assertEqual(self._normalize(self._RAW_NAME_B), self._RAW_NAME_B)


class TestNameResidualScore(unittest.TestCase):
    def test_strong_identity_conflict_vetoes(self):
        a = "hotel bkc crown near trade centre visa consulate"
        b = "hotel bkc inn near trade center visa consulate"
        self.assertEqual(_name_residual_score(a, b, 0.8), 0.0)

    def test_one_sided_strong_identity_penalty(self):
        a = "sea green hotel"
        b = "sea green south hotel"
        self.assertEqual(_name_residual_score(a, b, 0.8), 0.9)

    def test_user_case_conflict_at_mid_jaccard(self):
        a = "hotel bkc crown near trade centre visa consulate"
        b = "hotel bkc inn near trade center visa consulate"
        self.assertEqual(_name_residual_score(a, b, 0.5), 0.0)


class TestNameResidualScoreFix1BrandVsLocation(unittest.TestCase):
    """
    Fix 1: Brand terms on one side are a VETO (0.0), not just a penalty (0.9).
    Location terms on one side remain a penalty (0.9).
    """

    def test_x_by_y_brand_pattern_vetoes(self):
        # "residence inn by marriott miami airport" vs "miami airport marriott"
        # "residence" and "inn" are brand identity terms unique to name_a → VETO
        a = "residence inn by marriott miami airport"
        b = "miami airport marriott"
        self.assertEqual(_name_residual_score(a, b, 0.6), 0.0)

    def test_express_brand_variant_vetoes(self):
        # "Holiday Inn Express" vs "Holiday Inn" — express is a brand differentiator
        a = "holiday inn express"
        b = "holiday inn"
        self.assertEqual(_name_residual_score(a, b, 0.67), 0.0)

    def test_residence_brand_one_sided_vetoes(self):
        # "Marriott Residence Inn" vs "Marriott Hotel" — residence is brand identity
        a = "marriott residence"
        b = "marriott hotel"
        self.assertEqual(_name_residual_score(a, b, 0.5), 0.0)

    def test_location_term_one_sided_is_still_penalty_not_veto(self):
        # "Hotel Airport" vs "Hotel" — airport is a location modifier, not a brand
        a = "hotel airport"
        b = "hotel"
        # jaccard < 0.5 so residual short-circuits to 1.0 (safe default)
        # Use a higher jaccard to trigger residual analysis
        self.assertEqual(_name_residual_score(a, b, 0.6), 0.9)

    def test_conflicting_location_terms_veto(self):
        # "Grand Hotel North" vs "Grand Hotel South" — conflicting directions → VETO
        a = "grand hotel north"
        b = "grand hotel south"
        self.assertEqual(_name_residual_score(a, b, 0.67), 0.0)

    def test_same_brand_term_both_sides_is_safe(self):
        # "Crown Plaza Mumbai" vs "Crown Plaza Delhi" — crown in both (intersection)
        # After fix: no brand in residuals, no location in residuals → 1.0
        a = "crown plaza mumbai"
        b = "crown plaza"
        # "mumbai" lands in residual_a but isn't a brand/location term → safe
        self.assertEqual(_name_residual_score(a, b, 0.67), 1.0)

    def test_low_jaccard_skips_residual_analysis(self):
        # Score < 0.5 → residual check is skipped entirely → 1.0
        self.assertEqual(_name_residual_score("residence inn", "marriott", 0.3), 1.0)

    def test_brand_conflict_both_sides_vetoes(self):
        # "Grand Palace" vs "Grand Inn" — palace vs inn both brand terms → VETO
        a = "grand palace"
        b = "grand inn"
        self.assertEqual(_name_residual_score(a, b, 0.5), 0.0)


if __name__ == "__main__":
    unittest.main()
