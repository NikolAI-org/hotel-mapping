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

    unit_pattern = r'\b(phase|tower|unit|block|apt|apartment|bldg|building|suite|room)?\s*(#)?\s*([0-9]+|[ivx]+|[a-z])\b$'

    match_a = re.search(unit_pattern, name_a.strip().lower())
    match_b = re.search(unit_pattern, name_b.strip().lower())

    token_a = match_a.group() if match_a else None
    token_b = match_b.group() if match_b else None

    if token_a and token_b:
        # 0.0 = Explicit Conflict (Phase 1 vs Phase 2)
        if token_a != token_b:
            return 0.0
            # 1.0 = Explicit Match (Phase 1 vs Phase 1)
        else:
            return 1.0

    elif token_a or token_b:
        # 0.8 = Missing from one side (Flag for review)
        return 0.8

        # 1.0 = Neither has a unit identifier
    return 1.0


unit_match_udf = F.udf(_unit_match_score, T.FloatType())