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

from hotel_data.pipeline.scoring.scorers.mismatch_rules import _address_unit_match_score, _type_match_score, _unit_match_score


class TestTypeMatchScore(unittest.TestCase):
    def test_unknown_vs_hotel_is_neutral(self):
        self.assertEqual(_type_match_score("hotel", "unknown"), 0.5)
        self.assertEqual(_type_match_score("unknown", "hotel"), 0.5)

    def test_missing_type_is_neutral(self):
        self.assertEqual(_type_match_score("hotel", None), 0.5)
        self.assertEqual(_type_match_score(None, "resort"), 0.5)

    def test_soft_match_group_scores_point8(self):
        self.assertEqual(_type_match_score("hotel", "resort"), 0.8)

    def test_substring_type_match_scores_point8(self):
        self.assertEqual(_type_match_score("hotel", "aparthotel"), 0.8)
        self.assertEqual(_type_match_score("aparthotel", "hotel"), 0.8)

    def test_hard_mismatch_scores_zero(self):
        self.assertEqual(_type_match_score("condo", "hotel"), 0.0)


class TestAddressUnitMatchScore(unittest.TestCase):
    def _assert_symmetric(self, addr_a, addr_b, postal_a, postal_b, expected):
        self.assertAlmostEqual(
            _address_unit_match_score(addr_a, addr_b, postal_a, postal_b),
            expected,
            places=6,
        )
        self.assertAlmostEqual(
            _address_unit_match_score(addr_b, addr_a, postal_b, postal_a),
            expected,
            places=6,
        )

    def test_returns_zero_for_conflicting_house_numbers(self):
        a = "4rd floor kamal mansion hnaa rd, colaba"
        b = "121,kartar bhavan,shahid bhagat singh road,near electric house,colaba"
        self.assertEqual(_address_unit_match_score(a, b), 0.0)

    def test_returns_one_when_house_number_overlaps(self):
        a = "4rd floor kamal mansion hnaa rd, colaba"
        b = "bungalows no 4, shahid bhagat singh road, colaba"
        self.assertEqual(_address_unit_match_score(a, b), 1.0)

    def test_returns_ambiguous_when_one_side_has_no_number(self):
        a = "kamal mansion hnaa rd, colaba"
        b = "121 kartar bhavan shahid bhagat singh road, colaba"
        self.assertEqual(_address_unit_match_score(a, b), 0.9)

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
                "expected": 0.9,
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
                "expected": 0.3,
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

    def test_building_overlap_with_conflicting_extra_units_returns_partial_weighted_jaccard(self):
        addr_i = (
            "9 floor building no.11 (sra near trade centre, bandra kurla complex, "
            "bandra east, mumbai, maharashtra 400051"
        )
        addr_j = "building no- 11 shop no 819, motilal nagar, road bandra"

        self.assertAlmostEqual(
            _address_unit_match_score(addr_i, addr_j, "400051", None),
            0.3,
            places=6,
        )

    def test_one_sided_numeric_evidence_is_ambiguous(self):
        one_num = _address_unit_match_score("building no 11", "road bandra")
        two_nums = _address_unit_match_score("9 floor building no 11", "road bandra")
        self.assertAlmostEqual(one_num, 0.9, places=6)
        self.assertAlmostEqual(two_nums, 0.9, places=6)

    def test_gradual_jaccard_overlap_examples(self):
        # {11, 22} vs {11, 33} => partial overlap: (1/3) * 0.9 = 0.3
        score_partial = _address_unit_match_score(
            "building 11 unit 22",
            "building 11 unit 33",
        )
        self.assertAlmostEqual(score_partial, 0.3, places=6)

        # Strict subset case should map to 0.85, not weighted Jaccard.
        score_stronger = _address_unit_match_score(
            "building 11 unit 22",
            "building 11 unit 22 block 44",
        )
        self.assertAlmostEqual(score_stronger, 0.85, places=6)
        self.assertGreater(score_stronger, score_partial)

    def test_user_case_building_overlap_with_extra_shop_number(self):
        # Treat 400051 as postal code using the dedicated postal arg:
        # {9, 11} vs {11, 819} => overlap {11} => (1/3) * 0.9 = 0.3
        addr_a = (
            "9 floor building no.11 (sra near trade centre, bandra kurla complex, "
            "bandra east, mumbai, maharashtra 400051"
        )
        addr_b = "building no- 11 shop no 819, motilal nagar, road bandra"
        self.assertAlmostEqual(_address_unit_match_score(addr_a, addr_b, "400051", None), 0.3, places=6)

    def test_user_case_jankidevi_school_conflicting_numbers(self):
        # {91} vs {4} => hard conflict
        addr_a = "91 jankidevi school rd  andheri west  svp nagar"
        addr_b = "jankidevi public school road, mhada, 4 bunglow, andheri (west)"
        self.assertEqual(_address_unit_match_score(addr_a, addr_b), 0.0)

    def test_user_case_phase_and_plot_conflicting_numbers(self):
        # {495, 3, 2} vs {5} => hard conflict
        addr_a = "495/3, phase- 2, chardhi rd"
        addr_b = "plot no.5, laxmi nagar, near garden estate building, mumbai, mumbai"
        self.assertEqual(_address_unit_match_score(addr_a, addr_b), 0.0)


class TestUnitMatchScore(unittest.TestCase):
    def test_no_unit_evidence_is_neutral_match(self):
        self.assertEqual(_unit_match_score("hotel sunrise", "resort sunrise"), 1.0)

    def test_base_name_is_ignored_for_conflicting_units(self):
        self.assertEqual(_unit_match_score("hotel alpha phase 1", "resort beta phase 2"), 0.0)

    def test_roman_numeric_equivalence_matches(self):
        self.assertEqual(_unit_match_score("hotel orchid phase ii", "hotel orchid phase 2"), 1.0)

    def test_contextual_single_letter_unit_matches(self):
        self.assertEqual(_unit_match_score("tower a at green view", "tower A greenview"), 1.0)

    def test_one_sided_unit_evidence_is_ambiguous(self):
        self.assertEqual(_unit_match_score("hotel phase iii", "hotel"), 0.9)

    def test_subset_unit_evidence_uses_subset_weight(self):
        self.assertEqual(_unit_match_score("phase 2 tower a", "phase ii"), 0.85)

    def test_partial_overlap_uses_weighted_jaccard(self):
        score = _unit_match_score("phase 2 tower a wing b", "phase ii block b unit 9")
        self.assertAlmostEqual(score, 0.45, places=6)

    def test_user_case_name_with_one_sided_numeric_unit(self):
        name_i = "hotel versova inn"
        name_j = "hotel 97 inn- andheri versova"
        self.assertEqual(_unit_match_score(name_i, name_j), 0.9)

    def test_compact_bhk_number_conflict_is_hard_mismatch(self):
        self.assertEqual(_unit_match_score("2bhk belapur", "1bhk belapur"), 0.0)

    def test_compact_and_spaced_bhk_numbers_match(self):
        self.assertEqual(_unit_match_score("2bhk belapur", "2 bhk belapur"), 1.0)

    def test_user_case_private_bedroom_bath_variant_matches(self):
        name_a = "1 private bedroom ii bath in a modern 2 bhk in powai"
        name_b = "1 private bedroom in a modern 2 bhk in powai"
        self.assertEqual(_unit_match_score(name_a, name_b), 0.85)


if __name__ == "__main__":
    unittest.main()