from typing import List
import re

from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import col, trim, regexp_replace, udf
from pyspark.sql.types import StringType

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor
from hotel_data.pipeline.preprocessor.processors.smart_cleaner import smart_suffix_udf


def _make_dynamic_cleaner(address_count: int):
    """
    Create a Python function to be wrapped as a UDF.
    It accepts (name, *addresses) and removes each address (literal, case-insensitive).
    """
    def dynamic_cleaner(name: str, *addresses: str) -> str:
        if name is None:
            return None

        s = str(name)
        # remove each provided address literal in a case-insensitive manner
        for a in (addresses[:address_count] if addresses else []):
            if a is None or a == "":
                continue
            try:
                # escape to treat address as a literal substring
                pattern = re.compile(re.escape(str(a)), flags=re.IGNORECASE)
                s = pattern.sub("", s)
            except re.error:
                # conservative fallback to plain replace if regex fails for any reason
                s = s.replace(str(a), "")

        # collapse multiple spaces into one and trim
        s = re.sub(r"\s+", " ", s).strip()
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
        udf_args: List[Column] = [col(self.name_col)] + [col(f) for f in self.address_fields]

        # 1) Row-wise cleaning using UDF (avoids passing Column into regexp_replace pattern)
        cleaned_col: Column = self.dynamic_cleaner_udf(*udf_args)

        # 2) Apply existing smart_suffix_udf (assumes it accepts (Column, Column) -> Column)
        cleaned_col = smart_suffix_udf(cleaned_col, col("contact_address_line1"))

        # 3) Final whitespace normalization (in case smart_suffix_udf introduced spaces)
        cleaned_col = trim(regexp_replace(cleaned_col, r"\s+", " "))

        return df.withColumn(self.output_col, cleaned_col)
