import re
from fuzzywuzzy import fuzz
from hotel_data.pipeline.preprocessor.utils.constants import (
    STOP_WORDS,
    STOP_PHRASES,
    STRONG_IDENTITY_TERMS,
    BRAND_IDENTITY_TERMS,
    LOCATION_IDENTITY_TERMS,
)
from hotel_data.config.scoring_config import ScoringConstants
import pyspark.sql.functions as F
import pyspark.sql.types as T

JACCARD_ALGO = "jaccard"
LCS_ALGO = "lcs"
LEVENSHTEIN_ALGO = "levenshtein"
CONTAINMENT_ALGO = "containment"


def extract_units(text):
    """Extracts critical unit identifiers: Numbers, Roman Numerals, Single Letters"""
    if not text:
        return set()
    text = text.upper()

    units = set()

    # 1. Arabic Numbers (e.g., "1", "101")
    units.update(re.findall(r"\d+", text))

    # 2. Roman Numerals (Phase I, II, V...)
    # Pattern must be defined in ScoringConstants or locally
    # r"\b(I|II|III|IV|V|VI|VII|VIII|IX|X)\b"
    units.update(re.findall(ScoringConstants.ROMAN_NUMERAL_PATTERN, text))

    # 3. Single Block Letters (Block A, Wing B)
    # We look for single letters surrounded by boundaries
    units.update(re.findall(r"\b[A-Z]\b", text))

    return units


def calculate_unit_score(s1: str, s2: str) -> float:
    """
    Returns:
    1.0 -> Match (or neutral/empty)
    0.0 -> HARD Mismatch (Different numbers found)
    """
    if not s1 or not s2:
        return 1.0  # Neutral/Safe

    units1 = extract_units(s1)
    units2 = extract_units(s2)

    # If BOTH sides have units, and they are completely different (disjoint) -> Mismatch
    # e.g. {1} vs {2} -> Disjoint -> 0.0
    # e.g. {1, A} vs {1} -> Intersection exists -> 1.0 (Safe)
    if units1 and units2 and units1.isdisjoint(units2):
        return 0.0

    return 1.0


# def get_numeric_penalty(s1: str, s2: str) -> float:
#     """
#     Checks for numeric mismatch and returns the penalty value.
#     Used by both string scorers and SBERT scorers.
#     # e.g., "OYO Townhouse 123" -> {'123'}
#     """
#     if not s1 or not s2:
#         return 0.0
#
#     nums1 = set(re.findall(r'\d+', s1))
#     nums2 = set(re.findall(r'\d+', s2))
#
#     # If BOTH have numbers, and those numbers are DIFFERENT, return penalty.
#     if nums1 and nums2 and nums1 != nums2:
#         return ScoringConstants.NUMERIC_MISMATCH_PENALTY
#
#     return 0.0


def bigram_jaccard(s1: str, s2: str) -> float:
    """
    Calculates character bi-gram Jaccard similarity between two strings.

    Each string is decomposed into overlapping character 2-grams (after lowercasing
    and stripping whitespace), then standard Jaccard is applied on the resulting sets.

    Examples:
        "grand palace" → {"gr","ra","an","nd","d ","  ","pa","al","la","ac","ce"}
        bigram_jaccard("grand palace", "grand palce") → ~0.73

    Returns:
        float: Score in [0.0, 1.0].
               1.0  → identical bigram sets
               0.5  → both empty (neutral)
               0.0  → one side empty, or completely disjoint bigram sets
    """
    if not s1 and not s2:
        return ScoringConstants.BOTH_EMPTY_SCORE

    if not s1 or not s2:
        return ScoringConstants.ONE_SIDE_EMPTY_SCORE

    s1 = s1.lower().strip()
    s2 = s2.lower().strip()

    def to_bigrams(text: str) -> set:
        return {text[i : i + 2] for i in range(len(text) - 1)}

    bg1 = to_bigrams(s1)
    bg2 = to_bigrams(s2)

    if not bg1 and not bg2:
        return ScoringConstants.BOTH_EMPTY_SCORE
    if not bg1 or not bg2:
        return ScoringConstants.ONE_SIDE_EMPTY_SCORE

    intersection = len(bg1 & bg2)
    union = len(bg1 | bg2)
    return intersection / union if union > 0 else 0.0


def enhanced_name_scorer(
    s1: str, s2: str, algo: str = "jaccard", perform_cleaning: bool = True
) -> float:
    """
    Calculates a similarity score based on the selected algorithm.

    Args:
        s1 (str): First string.
        s2 (str): Second string.
        algo (str): One of ['jaccard', 'lcs', 'levenshtein']. Defaults to 'jaccard'.
        perform_cleaning (bool): If True, removes stop words/phrases.
                                 Set to False for raw 'name' comparisons.

    Returns:
        float: Similarity score between 0.0 and 1.0.
    """

    # HELPER: Get Ordered Tokens (List)
    def get_tokens(text, keep_stop_words=False):
        """Tokenizes the text, converts to lowercase, and filters stop words."""
        # Note: We keep this because we need to support the raw 'name' column
        # and we need to produce Sets/Sorted Strings for the algorithms.
        if not text:
            return []
        text = text.lower()

        # 1. Remove Phrases (Always done if cleaning is on) (Order matters!)
        # We use \b (word boundaries) to ensure we don't accidentally match inside words
        # e.g. removing "oyo" shouldn't break "toyota"
        if perform_cleaning:
            for phrase in STOP_PHRASES:
                # Pattern: \bphrase\b (whole word match only)
                pattern = r"\b" + re.escape(phrase) + r"\b"
                text = re.sub(pattern, " ", text)

        # 2. Standard Cleanup (Punctuation)
        text = text.replace(",", " ").replace("-", " ").replace(".", " ").strip()
        tokens = text.split()

        # 3. Filter Stop Words
        # Logic: For Jaccard we remove them. For Levenshtein/Weighted, we might KEEP them
        # so we can weight them down explicitly (instead of deleting them).
        if perform_cleaning and not keep_stop_words:
            return [t for t in tokens if t not in STOP_WORDS and t.isalnum()]
        else:
            return [t for t in tokens if t.isalnum()]

    # --- 1. Preprocessing ---
    # For Jaccard: Clean aggressively (remove "hotel", "residency")
    tokens_jaccard_1 = get_tokens(s1, keep_stop_words=False)
    tokens_jaccard_2 = get_tokens(s2, keep_stop_words=False)

    # For Levenshtein/LCS: Keep stop words so we can see "Residency" and apply low weight
    tokens_weighted_1 = get_tokens(s1, keep_stop_words=True)
    tokens_weighted_2 = get_tokens(s2, keep_stop_words=True)

    final_score = 0.0

    set1 = set(tokens_jaccard_1)
    set2 = set(tokens_jaccard_2)

    # --- 1. Jaccard Calculation (Uses Sets) ---
    if algo == JACCARD_ALGO:
        if not set1 and not set2:
            final_score = ScoringConstants.BOTH_EMPTY_SCORE  # 0.5
        elif not set1 or not set2:
            final_score = ScoringConstants.ONE_SIDE_EMPTY_SCORE  # 0.0
        else:
            intersection = len(set1.intersection(set2))
            union = len(set1.union(set2))
            final_score = intersection / union if union > 0 else 0.0

    elif algo == CONTAINMENT_ALGO:
        if not set1 and not set2:
            final_score = ScoringConstants.BOTH_EMPTY_SCORE
        elif not set1 or not set2:
            final_score = ScoringConstants.ONE_SIDE_EMPTY_SCORE
        else:
            intersection = len(set1.intersection(set2))
            # Key difference: Divide by MIN length, not UNION
            min_len = min(len(set1), len(set2))
            final_score = intersection / min_len if min_len > 0 else 0.0

    # --- 2. String Reconstruction (Uses ORDERED Tokens) ---
    elif algo == LCS_ALGO:
        # Use the tokens that include "hotel" etc, but rely on length penalty
        # JOIN THE LISTS (Preserves "pinaki comfort stay")
        str1 = " ".join(tokens_weighted_1)
        str2 = " ".join(tokens_weighted_2)

        # --- ISSUE 3 FIX: EMPTY DATA CHECK ---
        # If both are empty (e.g. they were just stop words), score 0.0
        if not str1 or not str2:
            final_score = ScoringConstants.RECONSTRUCTION_EMPTY_SCORE
        else:

            def longest_common_substring(a, b):
                m = [[0] * (1 + len(b)) for i in range(1 + len(a))]
                longest = 0
                for i in range(1, 1 + len(a)):
                    for j in range(1, 1 + len(b)):
                        if a[i - 1] == b[j - 1]:
                            m[i][j] = m[i - 1][j - 1] + 1
                            longest = max(longest, m[i][j])
                        else:
                            m[i][j] = 0
                return longest

            lcs_len = longest_common_substring(str1, str2)

            # IMPROVEMENT: Use MAX length denominator to penalize subset matches
            # "Amar Residency" (14) vs "AR Residency" (12) -> 12/14 = 0.85 (Not 1.0)
            max_len = max(len(str1), len(str2))
            final_score = 0.0 if max_len == 0 else lcs_len / max_len

    # --- 3. LEVENSHTEIN (Weighted Token Logic) ---
    elif algo == LEVENSHTEIN_ALGO:
        str1 = " ".join(tokens_weighted_1)
        str2 = " ".join(tokens_weighted_2)

        if not str1 or not str2:
            final_score = ScoringConstants.RECONSTRUCTION_EMPTY_SCORE

        # Single Letter Guard
        elif (
            len(str1) < ScoringConstants.MIN_CHARS_REQUIRED_FOR_MATCHING
            or len(str2) < ScoringConstants.MIN_CHARS_REQUIRED_FOR_MATCHING
        ) and str1 != str2:
            final_score = ScoringConstants.MIN_CHARS_MISSING_SCORE

        else:
            # A. Calculate Standard Fuzzy Scores
            strict_lev = fuzz.ratio(str1, str2) / 100.0
            partial_lev = fuzz.partial_ratio(str1, str2) / 100.0

            # B. Calculate Information Overlap (The Innovation)
            t1_set = set(tokens_weighted_1)
            t2_set = set(tokens_weighted_2)
            common_tokens = t1_set.intersection(t2_set)
            all_tokens = t1_set.union(t2_set)

            def get_weight(token):
                if token in ScoringConstants.LOW_INFO_TERMS:
                    return ScoringConstants.LOW_INFO_WEIGHT
                return ScoringConstants.HIGH_INFO_WEIGHT

            match_weight = sum(get_weight(t) for t in common_tokens)
            total_weight = sum(get_weight(t) for t in all_tokens)

            # info_ratio: What % of the *information* is shared?
            # If we only share "Residency" (0.2) but miss "Amar" (1.0) and "AR" (1.0),
            # ratio = 0.2 / 2.2 = 0.09 (Very Low)
            info_ratio = match_weight / total_weight if total_weight > 0 else 0.0

            # C. Damping Formula
            # We trust the high Partial Ratio (1.0) ONLY if Info Ratio is also decent.
            # Formula: partial_score * (0.4 + 0.6 * info_ratio)
            # This dampens "false positive" subsets while keeping true matches high.
            damped_score = partial_lev * (0.4 + 0.6 * info_ratio)

            # Allow strict match to win if it's high (for typos like "Amxr" vs "Amar")
            final_score = max(strict_lev, damped_score)

    # --- 4. Apply Numeric Penalty (Common) ---
    # Logic: If BOTH have numbers, and those numbers are DIFFERENT, apply penalty.
    # This applies to the result of ANY selected algorithm.     # e.g., "OYO Townhouse 123" -> {'123'}
    # removing penalty since added name_unit_score|address_unit_score
    # final_score -= get_numeric_penalty(s1, s2)

    # Ensure score doesn't drop below zero
    return max(0.0, final_score)


def _name_residual_score(name_a: str, name_b: str, jaccard_score: float) -> float:
    """
    Penalizes highly similar strings that conflict on critical identity terms.
    """
    # 1. Safe default for missing data
    if not name_a or not name_b or jaccard_score is None:
        return 1.0

    # 2. THE OPTIMIZATION: Only inspect if Jaccard says they are highly similar
    if float(jaccard_score) < 0.5:
        return 1.0

    # 3. Clean and Tokenize
    def get_clean_set(text):
        text = text.lower().replace(",", " ").replace("-", " ").replace(".", " ")
        return {t for t in text.split() if t not in STOP_WORDS and t.isalnum()}

    set_a = get_clean_set(name_a)
    set_b = get_clean_set(name_b)

    # 4. Find the Residuals (The Leftovers)
    intersection = set_a.intersection(set_b)
    residual_a = set_a - intersection
    residual_b = set_b - intersection

    # 5. Extract brand and location terms from the leftovers separately
    brand_res_a = residual_a.intersection(BRAND_IDENTITY_TERMS)
    brand_res_b = residual_b.intersection(BRAND_IDENTITY_TERMS)
    loc_res_a = residual_a.intersection(LOCATION_IDENTITY_TERMS)
    loc_res_b = residual_b.intersection(LOCATION_IDENTITY_TERMS)

    # CASE 1: Hard conflict in brand identity (e.g., Crown vs Inn)
    if brand_res_a and brand_res_b and brand_res_a != brand_res_b:
        return 0.0  # VETO

    # CASE 2: Hard conflict in location modifiers (e.g., North vs South)
    if loc_res_a and loc_res_b and loc_res_a != loc_res_b:
        return 0.0  # VETO

    # CASE 3: One-sided brand term — different brand identity (e.g., "Residence Inn" vs "Marriott")
    if (brand_res_a and not brand_res_b) or (brand_res_b and not brand_res_a):
        return 0.0  # VETO — brand term present on only one side is a hard mismatch

    # CASE 4: One-sided location modifier — possible variant name (e.g., "Airport Hotel" vs "Hotel")
    if (loc_res_a and not loc_res_b) or (loc_res_b and not loc_res_a):
        return 0.9  # HEAVY PENALTY — but not a definitive mismatch

    # Safe - The leftovers were harmless words (like "near", "building")
    return 1.0


# Register the UDF
name_residual_udf = F.udf(_name_residual_score, T.FloatType())
