from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import TypeVar, Generic, Tuple

T = TypeVar("T")


class BaseChallengeProcessor(ABC, Generic[T]):
    @abstractmethod
    def process(self, challenger_df: DataFrame, anchor_df: DataFrame) -> T:
        pass
