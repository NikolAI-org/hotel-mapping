import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from typing import List
from hotel_data.pipeline.preprocessor.utils.constants import STOP_WORDS, STOP_PHRASES

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor

# Reuse the same stop words list from your scorer to ensure consistency
from hotel_data.pipeline.preprocessor.utils.name_utils import enhanced_name_scorer


def remove_stop_words(text: str) -> str:
    if not text:
        return None

        # 1. Lowercase
    text = text.lower()

    # 2. Remove PHRASES (Ordered)
    for phrase in STOP_PHRASES:
        pattern = r"\b" + re.escape(phrase) + r"\b"
        text = re.sub(pattern, " ", text)

    # 3. Tokenize
    tokens = text.split()

    # 4. Filter single stop words
    filtered_tokens = [t for t in tokens if t not in STOP_WORDS]

    if not filtered_tokens:
        return ""
    return " ".join(filtered_tokens)


class StopWordProcessor(BaseProcessor[DataFrame]):
    def __init__(
        self, input_col: str = "normalized_name", output_col: str = "normalized_name"
    ):
        self.input_col = input_col
        self.output_col = output_col
        self.stop_word_udf = F.udf(remove_stop_words, StringType())

    def process(self, df: DataFrame, prefix: str = "") -> DataFrame:
        return df.withColumn(self.output_col, self.stop_word_udf(F.col(self.input_col)))
