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
    sys.modules["pyspark.sql"] = types.ModuleType("pyspark.sql")

if "pyspark.sql.functions" not in sys.modules:
    sys.modules["pyspark.sql.functions"] = types.ModuleType("pyspark.sql.functions")

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

from hotel_data.pipeline.preprocessor.utils.name_utils import _name_residual_score


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
