from abc import ABC, abstractmethod
from pyspark.sql import DataFrame


class BaseBlocker(ABC):
    @abstractmethod
    def block(self, anchor_df: DataFrame, challenger_df: DataFrame) -> DataFrame:
        """
        Takes an anchor dataset (e.g., Expedia) and a challenger dataset (e.g., Booking.com)
        and returns a DataFrame of candidate pairs.
        """
        pass
