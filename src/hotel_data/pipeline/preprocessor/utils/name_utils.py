import re
from fuzzywuzzy import fuzz

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

    # Common stop words/terms often found in hotel names (LOWERCASE)
    STOP_WORDS = {'hotel', 'inn', 'lodge', 'motel', 'resort', 'spa', 'hostal', 'guesthouse', 'casa', 'auberge',
                  'apart-hotel', 'suites', 'executive suites', 'residences', 'b&b', 'bed and breakfast', 'hostel', 'at',
                  'near', 'by', 'the', 'on', 'of', 'in', 'next to', 'close to', 'opposite', 'near to', 'located at',
                  'located in', 'east', 'west', 'north', 'south', 'central', 'downtown', 'city', 'town', 'village',
                  'metropolitan', 'airport', 'beach', 'waterfront', 'harbor', 'view', 'views', 'vista', 'garden',
                  'gardens', 'park', 'plaza', 'square', 'terrace', 'court', 'quarters', 'grand', 'royal', 'king',
                  'queen', 'palace', 'imperial', 'crown', 'premier', 'prestige', 'deluxe', 'luxury', 'superior',
                  'exclusive', 'small', 'boutique', 'extended stay', 'budget', 'microtel', 'old', 'new', 'historic',
                  'vintage', 'modern', 'a', 'an', 'and', 'for', 'with', 'to', 'or', 'via', 'from', 'is', 'just', 'plus',
                  'el', 'la', 'los', 'las', 'del', 'de', 'y', 'le', 'les', 'du', 'des', 'et', 'chez', 'der', 'die',
                  'das', 'und', 'il', 'i', 'gli', 'e', 'di'}

    def clean_and_tokenize(text):
        """Tokenizes the text, converts to lowercase, and filters stop words."""
        text = text.replace(',', ' ').replace('-', ' ').replace('.', ' ').strip()
        tokens = text.lower().split()
        return set(t for t in tokens if t not in STOP_WORDS and t.isalnum())

    # --- 1. Preprocessing (Common) ---
    set1 = clean_and_tokenize(s1)
    set2 = clean_and_tokenize(s2)

    # Initialize score variable
    final_score = 0.0

    # --- 2. Jaccard Calculation ---
    if algo == JACCARD_ALGO:
        if not set1 and not set2:
            final_score = 1.0
        else:
            intersection = len(set1.intersection(set2))
            union = len(set1.union(set2))
            final_score = intersection / union if union > 0 else 0.0

    # --- 3. String Reconstruction (Needed for LCS & Levenshtein) ---
    elif algo in [LCS_ALGO, LEVENSHTEIN_ALGO]:
        # Use the cleaned tokens joined back for check to handle word order
        str1 = " ".join(sorted(list(set1))) if set1 else s1.lower()
        str2 = " ".join(sorted(list(set2))) if set2 else s2.lower()

        # Handle empty strings case immediately
        if not str1 and not str2:
            final_score = 1.0
        elif not str1 or not str2:
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