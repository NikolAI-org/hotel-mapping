import re
from typing import Set

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

    # Catches "hotel" in "aparthotel" or "resort" in "golf resort"
    if a in b or b in a:
        return 0.8

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
_ROMAN_TOKEN_RE = re.compile(r"^(?=[ivxlcdm]+$)m{0,4}(cm|cd|d?c{0,3})(xc|xl|l?x{0,3})(ix|iv|v?i{0,3})$")
_NAME_UNIT_CONTEXT_RE = re.compile(
    r"\b(?:phase|ph|block|blk|tower|twr|wing|unit|flat|apt|apartment|suite|room|villa|floor|flr|building|bldg)\s*[-#:]?\s*"
    r"([a-z]|\d{1,4}(?:st|nd|rd|th)?|[ivxlcdm]{1,7})\b"
)
_NAME_UNIT_STANDALONE_RE = re.compile(r"\b(\d{1,4}(?:st|nd|rd|th)?|[ivxlcdm]{1,7})\b")


def _roman_to_int(token: str) -> int:
    values = {"i": 1, "v": 5, "x": 10, "l": 50, "c": 100, "d": 500, "m": 1000}
    total = 0
    previous = 0
    for char in reversed(token.lower()):
        value = values[char]
        if value < previous:
            total -= value
        else:
            total += value
            previous = value
    return total


def _normalize_unit_token(token: str) -> str:
    token = token.strip().lower()
    if not token:
        return ""

    # Normalize ordinal numbers (e.g., 4th -> 4).
    ordinal_match = re.match(r"^(\d{1,4})(?:st|nd|rd|th)$", token)
    if ordinal_match:
        return ordinal_match.group(1)

    if token.isdigit():
        return token

    if _ROMAN_TOKEN_RE.match(token):
        return str(_roman_to_int(token))

    if len(token) == 1 and token.isalpha():
        return token.upper()

    return ""


def _extract_name_units(name: str) -> Set[str]:
    text = name.lower()
    tokens = []

    # Contextual extraction captures single-letter units safely (e.g., "tower A").
    tokens.extend(_NAME_UNIT_CONTEXT_RE.findall(text))

    # Standalone extraction catches numeric/roman suffixes (e.g., "phase ii").
    tokens.extend(_NAME_UNIT_STANDALONE_RE.findall(text))

    normalized = {_normalize_unit_token(token) for token in tokens}
    return {token for token in normalized if token}


def _unit_match_score(name_a: str, name_b: str) -> float:
    if not name_a or not name_b:
        return 1.0

    units_a = _extract_name_units(name_a)
    units_b = _extract_name_units(name_b)

    # No unit evidence on either side.
    if not units_a and not units_b:
        return 1.0

    # One-sided unit evidence is ambiguous, not contradictory.
    if (units_a and not units_b) or (units_b and not units_a):
        return 0.9

    # Exact agreement.
    if units_a == units_b:
        return 1.0

    # Hard conflict.
    if units_a.isdisjoint(units_b):
        return 0.0

    # Subset relation.
    if units_a.issubset(units_b) or units_b.issubset(units_a):
        return 0.85

    # Partial overlap.
    intersection = len(units_a & units_b)
    union = len(units_a | units_b)
    if union == 0:
        return 1.0
    return (float(intersection) / float(union)) * 0.9


unit_match_udf = F.udf(_unit_match_score, T.FloatType())


# ==========================================
# 3. Address Unit Match Score
# ==========================================
def _address_unit_match_score(addr_a: str, addr_b: str, postal_a: str = None, postal_b: str = None) -> float:
    """
    Address-specific unit scorer.

        Returns score in [0.0, 1.0] with explicit ambiguity handling.

        Rules:
        - {} vs {}: 1.0
        - one-sided numeric evidence: 0.9
        - both non-empty:
            - exact match: 1.0
            - disjoint: 0.0
            - strict subset: 0.85
            - otherwise partial overlap: Jaccard(nums_a, nums_b) * 0.9
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

    # No numeric evidence on either side.
    if not nums_a and not nums_b:
        return 1.0

    # One-sided numeric evidence: ambiguous, but not a hard conflict.
    if (nums_a and not nums_b) or (nums_b and not nums_a):
        return 0.9

    # Exact agreement.
    if nums_a == nums_b:
        return 1.0

    # Hard conflict.
    if nums_a.isdisjoint(nums_b):
        return 0.0

    # Subset relation: one side is a strict subset of the other.
    if nums_a.issubset(nums_b) or nums_b.issubset(nums_a):
        return 0.85

    # Mixed partial overlap.
    intersection = len(nums_a & nums_b)
    union = len(nums_a | nums_b)
    if union == 0:
        return 1.0
    return (float(intersection) / float(union)) * 0.9


address_unit_match_udf = F.udf(_address_unit_match_score, T.FloatType())
