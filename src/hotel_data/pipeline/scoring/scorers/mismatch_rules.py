import re
from typing import Set

import pyspark.sql.functions as F
import pyspark.sql.types as T

# Matches: pure integers ("42"), ordinals ("4th", "3rd"), and alphanumeric house
# numbers ("1800b", "1775a").  The capture group keeps the full raw token so that
# "1850b" ≠ "1850" (distinct buildings) while "4th" is post-processed to "4" to
# align with plain "4" in another address.
_RAW_HOUSE_NUM_RE = re.compile(r"\b(\d+(?:st|nd|rd|th|[a-z])?)\b")
_ORDINAL_RE = re.compile(r"^(\d+)(st|nd|rd|th)$")


def _extract_house_nums(text: str) -> Set[str]:
    """Extract house/unit numbers from an address, normalising ordinal suffixes."""
    nums: Set[str] = set()
    for raw in _RAW_HOUSE_NUM_RE.findall(text.lower()):
        m = _ORDINAL_RE.match(raw)
        # Strip ordinal suffix ("4th" → "4") but keep alpha suffix ("1800b" → "1800b")
        nums.add(m.group(1) if m else raw)
    return nums


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
        {"hostel", "dormitory"},
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
_ROMAN_TOKEN_RE = re.compile(
    r"^(?=[ivxlcdm]+$)m{0,4}(cm|cd|d?c{0,3})(xc|xl|l?x{0,3})(ix|iv|v?i{0,3})$"
)

# TYPED Inventory
# Catches: "2 bhk", "ii bath", "1 bedroom", "bed 2", "bath iv", "1bd", "1ba"
# Handles number first OR type first; also compact no-space abbreviations.
# 1A. Value-First Inventory (e.g., "2 bhk", "ii bath") - PRIORITY
_TYPED_VAL_FIRST_RE = re.compile(
    r"\b(\d{1,4}|[ivxlcdm]{1,7})\s*[-:]?\s*(bhk|beds?|bedrooms?|baths?|bathrooms?|bd|ba|br)\b"
)

# 1B. Type-First Inventory (e.g., "bed 2", "bath iv") - FALLBACK
_TYPED_TYPE_FIRST_RE = re.compile(
    r"\b(bhk|beds?|bedrooms?|baths?|bathrooms?|bd|ba|br)\s*[-:]?\s*(\d{1,4}|[ivxlcdm]{1,7})\b"
)

# 1C. Compact concatenated abbreviations (e.g., "1bd1ba", "2ba") — no space between
# successive unit specs so \b would not fire between them. Lookahead allows matching
# back-to-back tokens like the "1ba" in "1bd1ba".
_TYPED_COMPACT_ABBREV_RE = re.compile(
    r"(\d{1,4})(bd|ba|br)(?=\d|\s|$|[^a-z])"
)

# General Context (UNTYPED)
# \b is added after the keyword to prevent matching plural forms as units
# (e.g., "suites" would previously match "suite" and capture "s" as the unit).
_NAME_UNIT_CONTEXT_RE = re.compile(
    r"\b(?:phase|ph|block|blk|tower|twr|wing|unit|flat|apt|apartment|suite|room|villa|floor|flr|building|bldg)\b\s*[-#:]?\s*"
    r"([a-z]|\d{1,4}(?:st|nd|rd|th)?|[ivxlcdm]{1,7})\b"
)

# Standalone (UNTYPED)
_NAME_UNIT_STANDALONE_RE = re.compile(r"\b(\d{1,4}(?:st|nd|rd|th)?|[ivxlcdm]{1,7})\b")
_NAME_UNIT_COMPACT_RE = re.compile(
    r"\b(\d{1,4}|[ivxlcdm]{1,7})\s*(?:bhk|bed|bedroom|bath|beds|bedrooms|baths|bathrooms|bd|ba|br)\b"
)


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


def _normalize_unit_token(token: str, allow_articles_as_units: bool = False) -> str:
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
        # Exclude article-like letters by default for standalone extraction,
        # but allow them when they are captured from contextual unit phrases.
        if token in {"a", "i"} and not allow_articles_as_units:
            return ""
        return token.upper()

    return ""


def _normalize_inventory_type(raw_type: str) -> str:
    """Maps synonymous inventory terms to a single standard type to avoid false mismatch."""
    t = raw_type.lower()
    if "bath" in t or t == "ba":
        return "bath"
    if t in {"bhk", "bed", "beds", "bedroom", "bedrooms", "bd", "br"}:
        return "bed"
    return "unknown"


def _extract_name_units(name: str) -> Set[str]:
    text = name.lower()
    tokens = []
    typed_tokens = []
    consumed_spans = []

    # 1A. Extract Value-First Typed Inventory (Highest Priority)
    for match in _TYPED_VAL_FIRST_RE.finditer(text):
        val, typ = match.groups()
        norm_val = _normalize_unit_token(val)
        norm_type = _normalize_inventory_type(typ)

        if norm_val and norm_type != "unknown":
            typed_tokens.append(f"{norm_type}:{norm_val}")
            consumed_spans.append(match.span())

    # 1B. Type-First Typed Inventory (Fallback)
    for match in _TYPED_TYPE_FIRST_RE.finditer(text):
        # Only process if this text wasn't already consumed by Value-First!
        # (This prevents "bedroom ii" from stealing the "ii" from "ii bath")
        span = match.span()
        overlap = any(
            start <= span[0] < end or start < span[1] <= end
            for start, end in consumed_spans
        )

        if not overlap:
            typ, val = match.groups()
            norm_val = _normalize_unit_token(val)
            norm_type = _normalize_inventory_type(typ)
            if norm_val and norm_type != "unknown":
                typed_tokens.append(f"{norm_type}:{norm_val}")
                consumed_spans.append(match.span())

    # 1C. Compact concatenated abbreviations (e.g., "1bd1ba" → bed:1, bath:1)
    # These have no \b between adjacent specs so the spaced patterns above miss them.
    for match in _TYPED_COMPACT_ABBREV_RE.finditer(text):
        span = match.span()
        overlap = any(
            start <= span[0] < end or start < span[1] <= end
            for start, end in consumed_spans
        )
        if not overlap:
            val, typ = match.groups()
            norm_val = _normalize_unit_token(val)
            norm_type = _normalize_inventory_type(typ)
            if norm_val and norm_type != "unknown":
                typed_tokens.append(f"{norm_type}:{norm_val}")
                consumed_spans.append(span)

    # 2. Contextual extraction (Captures letters like "Tower A")
    context_tokens = _NAME_UNIT_CONTEXT_RE.findall(text)
    tokens.extend(context_tokens)

    # 3. Standalone extraction (Naturally captures raw numbers like the "2" from "2 bhk")
    standalone_tokens = _NAME_UNIT_STANDALONE_RE.findall(text)
    tokens.extend(standalone_tokens)

    # 4. Compact extraction keeps untyped numeric evidence for forms like "2bhk".
    compact_tokens = _NAME_UNIT_COMPACT_RE.findall(text)
    tokens.extend(compact_tokens)

    # Normalize contextual tokens with article-like letters allowed (e.g., tower a).
    normalized_context = {
        _normalize_unit_token(token, allow_articles_as_units=True)
        for token in context_tokens
    }

    # Normalize standalone + compact tokens with strict filtering.
    normalized_other = {
        _normalize_unit_token(token) for token in (standalone_tokens + compact_tokens)
    }

    normalized_untyped = normalized_context.union(normalized_other)

    # Combine both sets. (Empty strings are dropped)
    final_set = {t for t in normalized_untyped if t}.union(set(typed_tokens))

    return final_set


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
def _address_unit_match_score(
    addr_a: str, addr_b: str, postal_a: str = None, postal_b: str = None
) -> float:
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

    # Capture house/building/unit numbers including alphanumeric tokens (e.g. "1800b").
    # Ordinal suffixes (st/nd/rd/th) are stripped; alpha suffixes are kept so that
    # "1850b" and "1850" are treated as distinct tokens.
    # Postal codes are removed explicitly using dedicated postal columns.
    nums_a = _extract_house_nums(addr_a)
    nums_b = _extract_house_nums(addr_b)

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
