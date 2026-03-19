from typing import List
import re

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, trim, regexp_replace, udf
from pyspark.sql.types import StringType

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor
from hotel_data.pipeline.preprocessor.processors.smart_cleaner import (
    smart_suffix_udf,
    normalize_real_estate_udf,
)
from hotel_data.config.scoring_config import ScoringConstants


def _make_dynamic_cleaner(address_count: int):
    """
    Create a Python function to be wrapped as a UDF.
    It accepts (name, *addresses) and removes each address (literal, case-insensitive).
    """

    def dynamic_cleaner(name: str, *addresses: str) -> str:
        if not name:
            return None

        s = str(name)
        original_s = s  # Keep backup

        # Filter valid addresses and SORT by length (Longest first).
        # This prevents "New Delhi" being partially removed by "Delhi".
        valid_addresses = [
            str(a) for a in (addresses[:address_count] if addresses else []) if a
        ]
        valid_addresses.sort(key=len, reverse=True)

        for a in valid_addresses:
            # 1. LENGTH GUARD: If address component matches the ENTIRE name, skip.
            # (Prevents Name="Mumbai", City="Mumbai" -> Name="")
            if s.lower().strip() == a.lower().strip():
                continue

            try:
                # 2. WORD BOUNDARY LOGIC (\b)
                # Escape the address string to handle parens/special chars safely
                escaped_a = re.escape(a)

                # Apply word boundaries only if the address starts/ends with alphanumerics
                # This ensures we match " Pune " but not the "Pune" inside "Puneet"
                pattern_str = escaped_a
                if a[0].isalnum():
                    pattern_str = r"\b" + pattern_str
                if a[-1].isalnum():
                    pattern_str = pattern_str + r"\b"

                pattern = re.compile(pattern_str, flags=re.IGNORECASE)
                s = pattern.sub("", s)
            except re.error:
                # Fallback to simple replace if regex fails
                s = s.replace(a, "")

        # collapse multiple spaces into one and trim
        s = re.sub(r"\s+", " ", s).strip()

        # 1. ULTIMATE SAFETY: If we stripped everything, revert to original
        if len(s) == 0 and len(original_s) > 0:
            return original_s

        # 2. If the result consists ONLY of Low Info Terms (e.g., "Hotel", "Residency"),
        # it means we stripped the distinguishing location name.
        # Example: "Hotel Colaba" -> stripped "Colaba" -> "Hotel".
        # Action: Revert to original. "Hotel Colaba" is better than "Hotel".
        tokens = s.lower().split()
        if tokens and all(t in ScoringConstants.LOW_INFO_TERMS for t in tokens):
            return original_s

        return s

    return dynamic_cleaner


class NameFormatterProcessor(BaseProcessor[DataFrame]):
    def __init__(
        self,
        address_fields: List[str],
        name_col: str = "name",
        output_col: str = "normalized_name",
    ):
        self.address_fields = address_fields
        self.name_col = name_col
        self.output_col = output_col

        # prepare UDF with correct arity
        self._cleaner_pyfunc = _make_dynamic_cleaner(len(address_fields))
        self.dynamic_cleaner_udf = udf(self._cleaner_pyfunc, StringType())

    def process(self, df: DataFrame, prefix: str = "") -> DataFrame:
        """
        Remove address substrings from the name column on a per-row basis using a UDF,
        then apply smart_suffix_udf and normalize whitespace.
        """
        # Build column list to pass into the UDF: (name_col, addr1, addr2, ...)
        udf_args: List[Column] = [col(self.name_col)] + [
            col(f) for f in self.address_fields
        ]

        # 1) Row-wise cleaning using UDF
        cleaned_col: Column = self.dynamic_cleaner_udf(*udf_args)

        # 2) Apply existing smart_suffix_udf
        cleaned_col = smart_suffix_udf(cleaned_col, col("contact_address_line1"))

        # 3) Normalize real-estate tokens (e.g., "2bhk" -> "2 bhk")
        cleaned_col = normalize_real_estate_udf(cleaned_col)

        # 4) Final whitespace normalization
        cleaned_col = trim(regexp_replace(cleaned_col, r"\s+", " "))

        return df.withColumn(self.output_col, cleaned_col)
