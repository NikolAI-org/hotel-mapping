import re

def enhanced_name_scorer(s1: str, s2: str) -> float:
    """
    Calculates a hybrid similarity score (Jaccard + Containment Ratio)
    between two strings, providing robustness for names with common prefixes
    but differing suffixes. This function is symmetrical.
    """

    # --- 0. Numeric Sanity Check (The Fix) ---
    # Extract all sequences of digits from both strings
    # e.g., "OYO Townhouse 123" -> {'123'}
    nums1 = set(re.findall(r'\d+', s1))
    nums2 = set(re.findall(r'\d+', s2))

    # Common stop words/terms often found in hotel names and addresses that should be ignored
    STOP_WORDS = {'Hotel', 'Inn', 'Lodge', 'Motel', 'Resort', 'Spa', 'Hostal', 'Guesthouse', 'Casa', 'Auberge',
                  'Apart-Hotel', 'Suites', 'Executive Suites', 'Residences', 'B&B', 'Bed and Breakfast', 'Hostel', 'At',
                  'Near', 'By', 'The', 'On', 'Of', 'In', 'Next to', 'Close to', 'Opposite', 'Near to', 'Located at',
                  'Located in', 'East', 'West', 'North', 'South', 'Central', 'Downtown', 'City', 'Town', 'Village',
                  'Metropolitan', 'Airport', 'Beach', 'Waterfront', 'Harbor', 'View', 'Views', 'Vista', 'Garden',
                  'Gardens', 'Park', 'Plaza', 'Square', 'Terrace', 'Court', 'Quarters', 'Grand', 'Royal', 'King',
                  'Queen', 'Palace', 'Imperial', 'Crown', 'Premier', 'Prestige', 'Deluxe', 'Luxury', 'Superior',
                  'Exclusive', 'Small', 'Boutique', 'Extended Stay', 'Budget', 'Microtel', 'Old', 'New', 'Historic',
                  'Vintage', 'Modern', 'A', 'An', 'And', 'For', 'With', 'To', 'Or', 'Via', 'From', 'Is', 'Just', 'Plus',
                  'El', 'La', 'Los', 'Las', 'Del', 'De', 'Y', 'Le', 'Les', 'Du', 'Des', 'Et', 'Chez', 'Der', 'Die',
                  'Das', 'Und', 'Il', 'I', 'Gli', 'E', 'Di'}

    def clean_and_tokenize(text):
        """Tokenizes the text, converts to lowercase, and filters stop words."""
        text = text.replace(',', ' ').replace(
            '-', ' ').replace('.', ' ').strip()
        tokens = text.lower().split()
        return set(t for t in tokens if t not in STOP_WORDS and t.isalnum())

    # --- 1. Token-based Jaccard Similarity (Symmetrical) ---
    set1 = clean_and_tokenize(s1)
    set2 = clean_and_tokenize(s2)

    if not set1 and not set2:
        jaccard_score = 1.0
    else:
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        jaccard_score = intersection / union if union > 0 else 0.0

    # --- 2. Longest Common Substring (LCS) Containment Ratio (Symmetrical) ---

    def longest_common_substring(a, b):
        """Finds the length of the longest common substring."""
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

    # Use the cleaned tokens joined back for LCS check
    str1 = " ".join(sorted(list(set1))) if set1 else s1.lower()
    str2 = " ".join(sorted(list(set2))) if set2 else s2.lower()

    if not str1 and not str2:
        lcs_ratio = 1.0
    elif not str1 or not str2:
        lcs_ratio = 0.0
    else:
        lcs_len = longest_common_substring(str1, str2)
        min_len = min(len(str1), len(str2))

        if min_len == 0:
            lcs_ratio = 0.0
        else:
            lcs_ratio = lcs_len / min_len

    # --- 3. Combined Score Calculation ---
    combined_score = (jaccard_score + lcs_ratio) / 2.0

    # --- 4. Apply Numeric Penalty ---
    # Logic: If BOTH have numbers, and those numbers are DIFFERENT, apply penalty.
    if nums1 and nums2 and nums1 != nums2:
        combined_score -= 0.7  # Apply the requested penalty

    # Ensure score doesn't drop below zero
    return max(0.0, combined_score)
