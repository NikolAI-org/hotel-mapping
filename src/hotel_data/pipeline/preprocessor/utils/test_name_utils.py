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


if __name__ == "__main__":
    unittest.main()
