"""
Tests for chain_name_score in chain_name_utils.py

Coverage
--------
- Missing / null / unknown chain sentinels  → 0.5  (neutral)
- Exact match (same string)                 → 1.0
- Case / whitespace normalization           → 1.0
- Strong containment                        → 0.9
- High token overlap (Jaccard ≥ 0.5)       → score in (0.5, 0.9)
- Low token overlap / clearly diff brands   → 0.0
- Mixed: one side missing, other present    → 0.5  (neutral)
"""

import unittest

from hotel_data.pipeline.preprocessor.utils.chain_name_utils import chain_name_score


class TestMissingOrUnknown(unittest.TestCase):
    """Both or one side has no useful chain information → neutral 0.5."""

    def test_both_none(self):
        self.assertEqual(chain_name_score(None, None), 0.5)

    def test_one_none(self):
        self.assertEqual(chain_name_score("Marriott", None), 0.5)
        self.assertEqual(chain_name_score(None, "Marriott"), 0.5)

    def test_empty_string(self):
        self.assertEqual(chain_name_score("", "Marriott"), 0.5)
        self.assertEqual(chain_name_score("Marriott", ""), 0.5)
        self.assertEqual(chain_name_score("", ""), 0.5)

    def test_no_chain_sentinel(self):
        self.assertEqual(chain_name_score("No chain", "Marriott"), 0.5)
        self.assertEqual(chain_name_score("Marriott", "No chain"), 0.5)
        self.assertEqual(chain_name_score("No chain", "No chain"), 0.5)

    def test_no_chain_case_insensitive(self):
        self.assertEqual(chain_name_score("NO CHAIN", "Hilton"), 0.5)
        self.assertEqual(chain_name_score("no chain", "Hilton"), 0.5)

    def test_none_sentinel(self):
        self.assertEqual(chain_name_score("None", "Marriott"), 0.5)

    def test_independent_sentinel(self):
        self.assertEqual(chain_name_score("Independent", "Marriott"), 0.5)

    def test_unknown_sentinel(self):
        self.assertEqual(chain_name_score("Unknown", "Marriott"), 0.5)


class TestExactMatch(unittest.TestCase):
    """Identical chain names → 1.0."""

    def test_exact_same(self):
        self.assertEqual(chain_name_score("Marriott", "Marriott"), 1.0)

    def test_exact_same_case_insensitive(self):
        self.assertEqual(chain_name_score("Marriott", "marriott"), 1.0)
        self.assertEqual(chain_name_score("HILTON WORLDWIDE", "Hilton Worldwide"), 1.0)

    def test_exact_with_whitespace(self):
        self.assertEqual(chain_name_score("  Marriott  ", "Marriott"), 1.0)

    def test_exact_multiword(self):
        self.assertEqual(
            chain_name_score(
                "Taj Hotels, Resorts & Palaces", "Taj Hotels, Resorts & Palaces"
            ),
            1.0,
        )

    def test_exact_real_brands(self):
        for brand in ["Hyatt Hotels", "Lemon Tree", "Sofitel", "Novotel", "Ramada"]:
            with self.subTest(brand=brand):
                self.assertEqual(chain_name_score(brand, brand), 1.0)


class TestContainmentMatch(unittest.TestCase):
    """One name is a substring of the other → 0.9."""

    def test_short_in_long(self):
        # "hilton" is contained in "hilton worldwide"
        self.assertEqual(chain_name_score("Hilton", "Hilton Worldwide"), 0.9)
        self.assertEqual(chain_name_score("Hilton Worldwide", "Hilton"), 0.9)

    def test_marriott_variants(self):
        self.assertEqual(chain_name_score("Marriott", "Marriott International"), 0.9)

    def test_ihg_variants(self):
        self.assertEqual(
            chain_name_score("IHG", "InterContinental Hotels Group"), 0.0
        )  # no containment, no token overlap

    def test_taj_short_form(self):
        # "Taj Hotels" is contained in "Taj Hotels, Resorts & Palaces"
        self.assertEqual(
            chain_name_score("Taj Hotels", "Taj Hotels, Resorts & Palaces"), 0.9
        )


class TestTokenJaccardMatch(unittest.TestCase):
    """Meaningful word overlap (Jaccard ≥ 0.5) → score in (0.5, 0.9)."""

    def test_high_overlap_returns_above_half(self):
        # "best western hotels resorts" vs "best western plus hotels"
        # neither contains the other, token jaccard = 3/5 = 0.6
        # → score = 0.5 + 0.4 * (0.6 - 0.5) / 0.5 = 0.58
        score = chain_name_score("Best Western Hotels Resorts", "Best Western Plus Hotels")
        self.assertGreater(score, 0.5)
        self.assertLess(score, 0.9)

    def test_score_is_float_between_bounds(self):
        score = chain_name_score(
            "The Indian Hotels Co Ltd", "The Indian Hotels Company"
        )
        # "the", "indian", "hotels" are shared → jaccard ≥ 0.5
        self.assertGreaterEqual(score, 0.5)
        self.assertLessEqual(score, 0.9)


class TestClearMismatch(unittest.TestCase):
    """Clearly different brands → 0.0 (veto)."""

    def test_completely_different(self):
        self.assertEqual(chain_name_score("Marriott", "Hilton Worldwide"), 0.0)

    def test_different_indian_brands(self):
        self.assertEqual(chain_name_score("Lemon Tree", "Taj Hotels, Resorts & Palaces"), 0.0)

    def test_different_global_brands(self):
        self.assertEqual(chain_name_score("Sofitel", "Ramada"), 0.0)
        self.assertEqual(chain_name_score("Novotel", "Comfort Inn"), 0.0)

    def test_single_word_no_overlap(self):
        self.assertEqual(chain_name_score("Hyatt", "Marriott"), 0.0)

    def test_zostel_vs_marriott(self):
        self.assertEqual(chain_name_score("Zostel", "Marriott"), 0.0)


class TestRealWorldPairs(unittest.TestCase):
    """End-to-end checks using realistic cross-supplier pairs from mumbai data."""

    def test_expedia_grn_same_brand(self):
        # expedia: "Taj Hotels, Resorts & Palaces"
        # grnconnect: "Taj Hotels, Resorts & Palaces"
        self.assertEqual(
            chain_name_score(
                "Taj Hotels, Resorts & Palaces", "Taj Hotels, Resorts & Palaces"
            ),
            1.0,
        )

    def test_expedia_ratehawk_marriott(self):
        self.assertEqual(chain_name_score("Marriott", "Marriott"), 1.0)

    def test_expedia_hobse_hobse_no_chain(self):
        # hobse has no chainName → neutral
        self.assertEqual(chain_name_score("Hilton Worldwide", None), 0.5)

    def test_bookingcom_expedia_different_brands(self):
        self.assertEqual(
            chain_name_score("InterContinental Hotels Group", "Marriott"), 0.0
        )

    def test_ratehawk_no_chain_vs_expedia_chain(self):
        self.assertEqual(chain_name_score("No chain", "Hyatt Hotels"), 0.5)


if __name__ == "__main__":
    unittest.main()
