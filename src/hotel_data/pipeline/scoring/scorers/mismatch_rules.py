import re
import pyspark.sql.functions as F
import pyspark.sql.types as T


# ==========================================
# 1. Property Type Match Score
# ==========================================
def _type_match_score(type_a: str, type_b: str) -> float:
    # 0.5 = Neutral when type evidence is missing/unknown
    if not type_a or not type_b:
        return 0.5

    a, b = type_a.strip().lower(), type_b.strip().lower()

    # Treat unknown placeholders as missing/neutral type evidence.
    unknown_markers = {"unknown", "na", "n/a", "null", "none", ""}
    if a in unknown_markers or b in unknown_markers:
        return 0.5

    if a == b:
        return 1.0

    soft_matches = [
        {"hotel", "resort", "motel", "inn"},
        {"villa", "home", "chalet", "house"},
        {"condo", "apartment", "aparthotel", "flat"},
        {"hostel", "dormitory"}
    ]

    # 0.8 = Soft mismatch (e.g., Hotel vs Resort)
    if any({a, b}.issubset(group) for group in soft_matches):
        return 0.8

        # 0.0 = Hard mismatch veto (e.g., Condo vs Hotel)
    return 0.0


type_match_udf = F.udf(_type_match_score, T.FloatType())


# ==========================================
# 2. Unit / Phase Match Score
# ==========================================
def _unit_match_score(name_a: str, name_b: str) -> float:
    if not name_a or not name_b:
        return 1.0

    a, b = name_a.lower(), name_b.lower()

    # THE FIX:
    # \d{1,4} limits it to 4 digits. It completely ignores 5+ digit Zip Codes and Property IDs.
    # Removed [a-z] to prevent "Collection O" from being treated as a unit.
    unit_pattern = r'\b(\d{1,4}(?:st|nd|rd|th)?|i{1,3}|iv|v|vi{1,3}|ix|x{1,2})\b'

    # Extract units
    units_a = set(re.findall(unit_pattern, a))
    units_b = set(re.findall(unit_pattern, b))

    # Strip units for base text
    base_a = re.sub(unit_pattern, '', a)
    base_b = re.sub(unit_pattern, '', b)

    # Clean remaining whitespaces/punctuation
    base_a_clean = re.sub(r'[^a-z]', '', base_a)
    base_b_clean = re.sub(r'[^a-z]', '', base_b)

    # RULE 1: If the base textual names match...
    if base_a_clean == base_b_clean:
        if units_a and units_b:
            if units_a == units_b:
                return 1.0
            # THE FIX: If B is just missing info that A has (or vice versa), it's NOT a contradiction!
            elif units_a.issubset(units_b) or units_b.issubset(units_a):
                return 0.9
            else:
                # Only veto if they share NO common truth (e.g., {1} vs {2})
                return 0.0

        elif units_a or units_b:
            return 0.9  # One has a unit, the other has absolutely none
        else:
            return 1.0

    # RULE 2: If bases differ, bypass veto
    return 1.0


unit_match_udf = F.udf(_unit_match_score, T.FloatType())


# ==========================================
# 3. Address Unit Match Score
# ==========================================
def _address_unit_match_score(addr_a: str, addr_b: str, postal_a: str = None, postal_b: str = None) -> float:
    """
    Address-specific unit scorer.

    Returns:
    - 0.0 when both addresses contain explicit numeric unit identifiers and they conflict
      (e.g., house/building no 4 vs 121).
        - 0.5 when only one side has explicit unit identifiers (partial evidence).
        - 1.0 otherwise (neutral/no contradiction).
    """
    if not addr_a or not addr_b:
        return 1.0

    # Capture house/building/unit numbers, including ordinal-style tokens like "4rd".
    # Postal codes are removed explicitly using dedicated postal columns.
    num_pattern = r"\b(\d{1,6})(?:st|nd|rd|th)?\b"
    nums_a = set(re.findall(num_pattern, addr_a.lower()))
    nums_b = set(re.findall(num_pattern, addr_b.lower()))

    # Remove known postal code numeric components so postal mismatches don't
    # leak into this signal; postal consistency is handled by postal_code_match.
    if postal_a:
        nums_a -= set(re.findall(r"\d+", str(postal_a).lower()))
    if postal_b:
        nums_b -= set(re.findall(r"\d+", str(postal_b).lower()))

    # Partial mismatch: one side has a unit-like number and the other does not.
    if (nums_a and not nums_b) or (nums_b and not nums_a):
        return 0.5

    # Hard contradiction only when both sides have numbers and none overlap.
    if nums_a and nums_b and nums_a.isdisjoint(nums_b):
        return 0.0

    return 1.0


address_unit_match_udf = F.udf(_address_unit_match_score, T.FloatType())
