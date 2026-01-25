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

    # --- 0. Numeric Sanity Check (Pre-calculation) ---
    # Extract all sequences of digits from both strings
    # e.g., "OYO Townhouse 123" -> {'123'}
    nums1 = set(re.findall(r'\d+', s1))
    nums2 = set(re.findall(r'\d+', s2))

    # HELPER: Get Ordered Tokens (List)
    def get_tokens(text):
        """Tokenizes the text, converts to lowercase, and filters stop words."""
        # Note: We keep this because we need to support the raw 'name' column
        # and we need to produce Sets/Sorted Strings for the algorithms.
        if not text: return []
        text = text.lower()

        # 2. Remove PHRASES (Order matters!)
        # We use \b (word boundaries) to ensure we don't accidentally match inside words
        # e.g. removing "oyo" shouldn't break "toyota"
        if perform_cleaning:
            for phrase in STOP_PHRASES:
                # Pattern: \bphrase\b (whole word match only)
                pattern = r"\b" + re.escape(phrase) + r"\b"
                text = re.sub(pattern, " ", text)

        # 3. Standard Cleanup (Punctuation)
        text = text.replace(',', ' ').replace('-', ' ').replace('.', ' ').strip()
        tokens = text.split()

        # 5. Filter single stop words
        if perform_cleaning:
            return [t for t in tokens if t not in STOP_WORDS and t.isalnum()]
        else:
            return [t for t in tokens if t.isalnum()]

    # --- 1. Preprocessing ---
    # Get ORDERED lists first
    tokens1 = get_tokens(s1)
    tokens2 = get_tokens(s2)

    # Create SETS only for Jaccard
    set1 = set(tokens1)
    set2 = set(tokens2)

    final_score = 0.0

    # --- 2. Jaccard Calculation (Uses Sets) ---
    if algo == JACCARD_ALGO:
        if not set1 and not set2:
            final_score = ScoringConstants.JACCARD_BOTH_EMPTY_SCORE #0.5
        elif not set1 or not set2:
            final_score = ScoringConstants.JACCARD_ONE_SIDE_EMPTY_SCORE #0.0
        else:
            intersection = len(set1.intersection(set2))
            union = len(set1.union(set2))
            final_score = intersection / union if union > 0 else 0.0

    # --- 3. String Reconstruction (Uses ORDERED Tokens) ---
    elif algo in [LCS_ALGO, LEVENSHTEIN_ALGO]:
        # JOIN THE LISTS (Preserves "pinaki comfort stay")
        str1 = " ".join(tokens1)
        str2 = " ".join(tokens2)

        # --- ISSUE 3 FIX: EMPTY DATA CHECK ---
        # If both are empty (e.g. they were just stop words), score 0.0
        if not str1 and not str2:
            final_score = ScoringConstants.RECONSTRUCTION_EMPTY_SCORE #0.0
        elif not str1 or not str2:
            final_score = ScoringConstants.RECONSTRUCTION_EMPTY_SCORE #0.0

        # --- NEW FIX: SINGLE LETTER BUG CHECK ("V" vs "Shivaji") ---
        # If the cleaned string is tiny (< 3 chars), we require an EXACT match.
        # "V" vs "V" -> Match.
        # "V" vs "Shivaji" -> Mismatch.
        elif (len(str1) < ScoringConstants.MIN_CHARS_REQUIRED_FOR_MATCHING or len(str2) < ScoringConstants.MIN_CHARS_REQUIRED_FOR_MATCHING) and str1 != str2:
            final_score = ScoringConstants.MIN_CHARS_MISSING_SCORE #0.0

        else:
            # LCS Logic
            if algo == LCS_ALGO:
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
                min_len = min(len(str1), len(str2))
                final_score = 0.0 if min_len == 0 else lcs_len / min_len

            # Levenshtein Logic
            elif algo == LEVENSHTEIN_ALGO:
                # strict_lev: Good for typos and spacing
                strict_lev = fuzz.ratio(str1, str2) / 100.0
                # partial_lev: Good for subsets
                partial_lev = fuzz.partial_ratio(str1, str2) / 100.0
                # Take the MAXIMUM of the two
                final_score = max(strict_lev, partial_lev)

    # --- 4. Apply Numeric Penalty (Common) ---
    # Logic: If BOTH have numbers, and those numbers are DIFFERENT, apply penalty.
    # This applies to the result of ANY selected algorithm.
    final_score -= get_numeric_penalty(s1, s2)
    #if nums1 and nums2 and nums1 != nums2:
    #    final_score -= ScoringConstants.NUMERIC_MISMATCH_PENALTY #0.7

    # Ensure score doesn't drop below zero
    return max(0.0, final_score)