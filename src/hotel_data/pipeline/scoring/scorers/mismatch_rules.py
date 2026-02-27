import re
import pyspark.sql.functions as F
import pyspark.sql.types as T


# ==========================================
# 1. Property Type Match Score
# ==========================================
def _type_match_score(type_a: str, type_b: str) -> float:
    # 1.0 = No conflict (Missing data gets benefit of the doubt)
    if not type_a or not type_b:
        return 1.0

    a, b = type_a.strip().lower(), type_b.strip().lower()
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
