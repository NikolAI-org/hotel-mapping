import sys
import types
import unittest


# mismatch_rules imports pyspark at module import time; provide minimal stubs.
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

if not hasattr(types_mod, "FloatType"):
    class _FloatType:
        pass
    types_mod.FloatType = _FloatType

from hotel_data.pipeline.scoring.scorers.mismatch_rules import _address_unit_match_score, _type_match_score


class TestTypeMatchScore(unittest.TestCase):
    def test_unknown_vs_hotel_is_neutral(self):
        self.assertEqual(_type_match_score("hotel", "unknown"), 0.5)
        self.assertEqual(_type_match_score("unknown", "hotel"), 0.5)

    def test_missing_type_is_neutral(self):
        self.assertEqual(_type_match_score("hotel", None), 0.5)
        self.assertEqual(_type_match_score(None, "resort"), 0.5)

    def test_soft_match_group_scores_point8(self):
        self.assertEqual(_type_match_score("hotel", "resort"), 0.8)

    def test_hard_mismatch_scores_zero(self):
        self.assertEqual(_type_match_score("condo", "hotel"), 0.0)


class TestAddressUnitMatchScore(unittest.TestCase):
    def _assert_symmetric(self, addr_a, addr_b, postal_a, postal_b, expected):
        self.assertEqual(
            _address_unit_match_score(addr_a, addr_b, postal_a, postal_b),
            expected,
        )
        self.assertEqual(
            _address_unit_match_score(addr_b, addr_a, postal_b, postal_a),
            expected,
        )

    def test_returns_zero_for_conflicting_house_numbers(self):
        a = "4rd floor kamal mansion hnaa rd, colaba"
        b = "121,kartar bhavan,shahid bhagat singh road,near electric house,colaba"
        self.assertEqual(_address_unit_match_score(a, b), 0.0)

    def test_returns_one_when_house_number_overlaps(self):
        a = "4rd floor kamal mansion hnaa rd, colaba"
        b = "bungalows no 4, shahid bhagat singh road, colaba"
        self.assertEqual(_address_unit_match_score(a, b), 1.0)

    def test_returns_half_when_one_side_has_no_number(self):
        a = "kamal mansion hnaa rd, colaba"
        b = "121 kartar bhavan shahid bhagat singh road, colaba"
        self.assertEqual(_address_unit_match_score(a, b), 0.5)

    def test_ignores_postal_code_mismatch_numbers(self):
        a = "kamal mansion, colaba, mumbai 400001"
        b = "kartar bhavan, colaba, mumbai 110001"
        # Postal digits must be removed using dedicated postal columns.
        self.assertEqual(_address_unit_match_score(a, b, "400001", "110001"), 1.0)

    def test_house_and_postal_permutations_and_combinations(self):
        # Each row validates both A->B and B->A to cover permutations.
        # expected=0.0 only when BOTH sides have non-postal house numbers and they conflict.
        cases = [
            {
                "name": "same_house_different_postal",
                "addr_a": "flat 12, alpha house, colaba, mumbai 400001",
                "addr_b": "unit 12, beta house, colaba, mumbai 110001",
                "postal_a": "400001",
                "postal_b": "110001",
                "expected": 1.0,
            },
            {
                "name": "different_house_different_postal",
                "addr_a": "flat 12, alpha house, colaba, mumbai 400001",
                "addr_b": "unit 99, beta house, colaba, mumbai 110001",
                "postal_a": "400001",
                "postal_b": "110001",
                "expected": 0.0,
            },
            {
                "name": "different_house_same_postal",
                "addr_a": "flat 12, alpha house, colaba, mumbai 400001",
                "addr_b": "unit 99, beta house, colaba, mumbai 400001",
                "postal_a": "400001",
                "postal_b": "400001",
                "expected": 0.0,
            },
            {
                "name": "same_house_same_postal",
                "addr_a": "flat 12, alpha house, colaba, mumbai 400001",
                "addr_b": "unit 12, beta house, colaba, mumbai 400001",
                "postal_a": "400001",
                "postal_b": "400001",
                "expected": 1.0,
            },
            {
                "name": "one_side_house_other_side_only_postal",
                "addr_a": "flat 12, alpha house, colaba, mumbai 400001",
                "addr_b": "beta house, colaba, mumbai 110001",
                "postal_a": "400001",
                "postal_b": "110001",
                "expected": 0.5,
            },
            {
                "name": "both_only_postal_numbers",
                "addr_a": "alpha house, colaba, mumbai 400001",
                "addr_b": "beta house, colaba, mumbai 110001",
                "postal_a": "400001",
                "postal_b": "110001",
                "expected": 1.0,
            },
            {
                "name": "multi_house_numbers_with_overlap",
                "addr_a": "flat 12, wing 4, alpha house, colaba, mumbai 400001",
                "addr_b": "unit 12, block 99, beta house, colaba, mumbai 110001",
                "postal_a": "400001",
                "postal_b": "110001",
                "expected": 1.0,
            },
            {
                "name": "multi_house_numbers_without_overlap",
                "addr_a": "flat 12, wing 4, alpha house, colaba, mumbai 400001",
                "addr_b": "unit 99, block 8, beta house, colaba, mumbai 110001",
                "postal_a": "400001",
                "postal_b": "110001",
                "expected": 0.0,
            },
            {
                "name": "ordinal_house_number_overlap",
                "addr_a": "4rd floor, alpha house, colaba, mumbai 400001",
                "addr_b": "flat 4, beta house, colaba, mumbai 110001",
                "postal_a": "400001",
                "postal_b": "110001",
                "expected": 1.0,
            },
            {
                "name": "ordinal_vs_different_house_number",
                "addr_a": "4rd floor, alpha house, colaba, mumbai 400001",
                "addr_b": "flat 121, beta house, colaba, mumbai 110001",
                "postal_a": "400001",
                "postal_b": "110001",
                "expected": 0.0,
            },
        ]

        for case in cases:
            with self.subTest(case=case["name"]):
                self._assert_symmetric(
                    case["addr_a"],
                    case["addr_b"],
                    case["postal_a"],
                    case["postal_b"],
                    case["expected"],
                )


if __name__ == "__main__":
    unittest.main()