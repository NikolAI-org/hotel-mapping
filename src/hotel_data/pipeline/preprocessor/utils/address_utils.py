# --- address_utils.py ---

from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from fuzzywuzzy import fuzz


def token_sort_score(s1: str, s2: str) -> float:
    """
    Calculates Token Sort Ratio similarity (0.0 to 1.0).
    Best for matching addresses with word order differences.
    """
    if s1 is None or s2 is None:
        return 0.0

    # fuzz.token_sort_ratio returns an integer percentage (0-100), so divide by 100
    score = fuzz.token_sort_ratio(s1.strip(), s2.strip())

    return float(score / 100.0)
