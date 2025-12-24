import re
from fuzzywuzzy import fuzz
from hotel_data.pipeline.preprocessor.utils.constants import STOP_WORDS, STOP_PHRASES

JACCARD_ALGO = "jaccard"
LCS_ALGO = "lcs"
LEVENSHTEIN_ALGO = "levenshtein"


def enhanced_name_scorer(s1: str, s2: str, algo: str = 'jaccard') -> float:
    """
    Calculates a similarity score based on the selected algorithm.

    Args:
        s1 (str): First string.
        s2 (str): Second string.
        algo (str): One of ['jaccard', 'lcs', 'levenshtein']. Defaults to 'jaccard'.

    Returns:
        float: Similarity score between 0.0 and 1.0.
    """

    # --- 0. Numeric Sanity Check (Pre-calculation) ---
    # Extract all sequences of digits from both strings
    # e.g., "OYO Townhouse 123" -> {'123'}
    nums1 = set(re.findall(r'\d+', s1))
    nums2 = set(re.findall(r'\d+', s2))

    def clean_and_tokenize(text):
        """Tokenizes the text, converts to lowercase, and filters stop words."""
        # Note: We keep this because we need to support the raw 'name' column
        # and we need to produce Sets/Sorted Strings for the algorithms.
        if not text: return set()

        # 1. Convert to lowercase first
        text = text.lower()

        # 2. Remove PHRASES (Order matters!)
        # We use \b (word boundaries) to ensure we don't accidentally match inside words
        # e.g. removing "oyo" shouldn't break "toyota"
        for phrase in STOP_PHRASES:
            # Pattern: \bphrase\b (whole word match only)
            pattern = r"\b" + re.escape(phrase) + r"\b"
            text = re.sub(pattern, " ", text)

        # 3. Standard Cleanup (Punctuation)
        text = text.replace(',', ' ').replace('-', ' ').replace('.', ' ').strip()

        # 4. Tokenize
        tokens = text.split()

        # 5. Filter single stop words
        return set(t for t in tokens if t not in STOP_WORDS and t.isalnum())

    # --- 1. Preprocessing (Common) ---
    set1 = clean_and_tokenize(s1)
    set2 = clean_and_tokenize(s2)

    # Initialize score variable
    final_score = 0.0

    # --- 2. Jaccard Calculation ---
    if algo == JACCARD_ALGO:
        if not set1 and not set2:
            final_score = 1.0  # Both empty/stop-words -> technically "same" info
        elif not set1 or not set2:
            final_score = 0.0
        else:
            intersection = len(set1.intersection(set2))
            union = len(set1.union(set2))
            final_score = intersection / union if union > 0 else 0.0

    # --- 3. String Reconstruction (Needed for LCS & Levenshtein) ---
    elif algo in [LCS_ALGO, LEVENSHTEIN_ALGO]:
        # Use the cleaned tokens joined back to handle word order
        str1 = " ".join(sorted(list(set1))) if set1 else s1.lower().strip()
        str2 = " ".join(sorted(list(set2))) if set2 else s2.lower().strip()

        # --- ISSUE 3 FIX: EMPTY DATA CHECK ---
        # If both are empty (e.g. they were just stop words), score 0.0
        if not str1 and not str2:
            final_score = 0.0
        elif not str1 or not str2:
            final_score = 0.0

        # --- NEW FIX: SINGLE LETTER BUG CHECK ("V" vs "Shivaji") ---
        # If the cleaned string is tiny (< 3 chars), we require an EXACT match.
        # "V" vs "V" -> Match.
        # "V" vs "Shivaji" -> Mismatch.
        elif (len(str1) < 3 or len(str2) < 3) and str1 != str2:
            final_score = 0.0

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
    if nums1 and nums2 and nums1 != nums2:
        final_score -= 0.7

    # Ensure score doesn't drop below zero
    return max(0.0, final_score)