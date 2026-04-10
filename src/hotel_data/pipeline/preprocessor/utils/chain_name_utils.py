"""
Chain name (branding) scoring utility.

Score interpretation
--------------------
  1.0  — both sides share a chain name and they match well
  0.5  — one or both sides have no known chain (neutral, no penalty)
  0.0  — both sides have a chain name but they clearly differ (veto)

This mirrors the phone_match_score / postal_code_match pattern:
missing data is neutral so sparse coverage doesn't penalise good pairs.

Matching strategy
-----------------
After normalization we apply a two-tier check:
  1. Exact match after lowercasing/stripping              → 1.0
  2. Strong containment (one name fully contains other)  → 0.9
  3. High token overlap (Jaccard on word tokens ≥ 0.5)   → linearly scaled in [0.5, 0.9]
  4. Below that                                           → 0.0  (different brands)

The 0.5 floor for token-Jaccard means a score in [0.5, 0.9] is only
returned when there is meaningful word overlap — low overlap goes straight
to 0.0 so a weak fuzzy hit never looks like a neutral "missing" score.
"""

# Sentinel values that mean "no chain" — treated the same as null.
_UNKNOWN_CHAINS = {"no chain", "none", "n/a", "independent", "unknown", ""}


def _normalize(name: str) -> str:
    """Lowercase, strip whitespace."""
    return name.strip().lower() if name else ""


def _is_missing(name) -> bool:
    """True when the chain name carries no useful information."""
    return name is None or _normalize(name) in _UNKNOWN_CHAINS


def _token_jaccard(a: str, b: str) -> float:
    """Jaccard similarity over whitespace-split word tokens."""
    tokens_a = set(a.split())
    tokens_b = set(b.split())
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)


def chain_name_score(chain_a, chain_b) -> float:
    """
    Compute a branding similarity score for a hotel pair.

    Parameters
    ----------
    chain_a, chain_b : str or None
        Raw ``chainName`` values from two hotel records (may be None).

    Returns
    -------
    float in {0.0, 0.5, 0.9, 1.0} or a linear interpolation thereof.
    """
    # Either side unknown → neutral
    if _is_missing(chain_a) or _is_missing(chain_b):
        return 0.5

    norm_a = _normalize(chain_a)
    norm_b = _normalize(chain_b)

    # Exact match
    if norm_a == norm_b:
        return 1.0

    # Strong containment (one brand name is a substring of the other)
    # e.g. "hilton" in "hilton worldwide" → 0.9
    if norm_a in norm_b or norm_b in norm_a:
        return 0.9

    # Token-level Jaccard for partial brand overlaps
    # e.g. "taj hotels resorts palaces" vs "taj hotels" → high overlap
    jaccard = _token_jaccard(norm_a, norm_b)
    if jaccard >= 0.5:
        # Scale [0.5, 1.0) jaccard → [0.5, 0.9) score
        return round(0.5 + 0.4 * (jaccard - 0.5) / 0.5, 4)

    # Clearly different brands
    return 0.0
