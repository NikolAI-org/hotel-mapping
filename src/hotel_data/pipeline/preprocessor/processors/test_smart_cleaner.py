import unittest
import sys
import types

# smart_cleaner imports pyspark at module import time.
# Provide minimal stubs so we can unit-test pure Python logic without Spark runtime.
if "pyspark" not in sys.modules:
    pyspark_mod = types.ModuleType("pyspark")
    sql_mod = types.ModuleType("pyspark.sql")
    functions_mod = types.ModuleType("pyspark.sql.functions")
    types_mod = types.ModuleType("pyspark.sql.types")

    def _identity_udf(fn, _return_type=None):
        return fn

    def _identity_col(name):
        return name

    class _StringType:
        pass

    functions_mod.udf = _identity_udf
    functions_mod.col = _identity_col
    types_mod.StringType = _StringType

    sys.modules["pyspark"] = pyspark_mod
    sys.modules["pyspark.sql"] = sql_mod
    sys.modules["pyspark.sql.functions"] = functions_mod
    sys.modules["pyspark.sql.types"] = types_mod

from hotel_data.pipeline.preprocessor.processors.smart_cleaner import (
    smart_suffix_remover,
    normalize_real_estate_terms,
)


class TestSmartSuffixRemover(unittest.TestCase):
    def test_handles_missing_inputs(self):
        self.assertIsNone(smart_suffix_remover(None, "some address"))
        self.assertEqual(smart_suffix_remover("hotel astros", None), "hotel astros")
        self.assertEqual(smart_suffix_remover("", "some address"), "")

    def test_removes_trailing_address_suffix_when_safe(self):
        name = "astros residency andheri west"
        address_line = "bungalows no 174, mhada, andheri west"

        # Removes trailing tokens found in address while preserving meaningful prefix.
        self.assertEqual(smart_suffix_remover(name, address_line), "astros residency")

    def test_keeps_token_when_only_generic_leftover(self):
        name = "hotel hoxton"
        address_line = "hotel hoxton, bungalows no 174, andheri west"

        # Guardrail: do not strip if removal would leave only low-information tokens.
        self.assertEqual(smart_suffix_remover(name, address_line), "hotel hoxton")

    def test_stops_at_first_non_address_token(self):
        name = "astros plaza hoxton"
        address_line = "bungalows no 174, andheri west"

        # No trailing token overlap, so the original name stays intact.
        self.assertEqual(
            smart_suffix_remover(name, address_line), "astros plaza hoxton"
        )


class TestNormalizeRealEstateTerms(unittest.TestCase):
    def test_splits_digit_prefixed_units(self):
        self.assertEqual(
            normalize_real_estate_terms("2bhk apartment"), "2 bhk apartment"
        )

    def test_splits_roman_numeral_units(self):
        self.assertEqual(
            normalize_real_estate_terms("bedroomiibath"), "bedroom ii bath"
        )
        self.assertEqual(
            normalize_real_estate_terms("bedroomivbath"), "bedroom iv bath"
        )

    def test_preserves_none_and_empty(self):
        self.assertIsNone(normalize_real_estate_terms(None))
        self.assertEqual(normalize_real_estate_terms(""), "")


if __name__ == "__main__":
    unittest.main()
