import re
from fuzzywuzzy import fuzz
from hotel_data.pipeline.preprocessor.utils.constants import STOP_WORDS, STOP_PHRASES
from hotel_data.config.scoring_config import ScoringConstants

JACCARD_ALGO = "jaccard"
LCS_ALGO = "lcs"
LEVENSHTEIN_ALGO = "levenshtein"


def get_numeric_penalty(s1: str, s2: str) -> float:
    """
    Checks for numeric mismatch and returns the penalty value.
    Used by both string scorers and SBERT scorers.
    # e.g., "OYO Townhouse 123" -> {'123'}
    """
    if not s1 or not s2:
        return 0.0

    nums1 = set(re.findall(r'\d+', s1))
    nums2 = set(re.findall(r'\d+', s2))

    # If BOTH have numbers, and those numbers are DIFFERENT, return penalty.
    if nums1 and nums2 and nums1 != nums2:
        return ScoringConstants.NUMERIC_MISMATCH_PENALTY

    return 0.0

def enhanced_name_scorer(s1: str, s2: str, algo: str = 'jaccard', perform_cleaning: bool = True) -> float:
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
        if not text: return []
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
        text = text.replace(',', ' ').replace('-', ' ').replace('.', ' ').strip()
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

    # --- 1. Jaccard Calculation (Uses Sets) ---
    if algo == JACCARD_ALGO:
        set1 = set(tokens_jaccard_1)
        set2 = set(tokens_jaccard_2)
        if not set1 and not set2:
            final_score = ScoringConstants.BOTH_EMPTY_SCORE #0.5
        elif not set1 or not set2:
            final_score = ScoringConstants.ONE_SIDE_EMPTY_SCORE #0.0
        else:
            intersection = len(set1.intersection(set2))
            union = len(set1.union(set2))
            final_score = intersection / union if union > 0 else 0.0

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
        elif (len(str1) < ScoringConstants.MIN_CHARS_REQUIRED_FOR_MATCHING or len(
                str2) < ScoringConstants.MIN_CHARS_REQUIRED_FOR_MATCHING) and str1 != str2:
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
    final_score -= get_numeric_penalty(s1, s2)

    # Ensure score doesn't drop below zero
    return max(0.0, final_score)