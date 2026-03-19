from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import TypeVar, Generic

T = TypeVar("T")


class BaseProcessor(ABC, Generic[T]):
    @abstractmethod
    def process(self, df: DataFrame, prefix: str = "") -> T:
        pass
