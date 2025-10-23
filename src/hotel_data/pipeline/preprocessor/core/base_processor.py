from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseProcessor(ABC):
    @abstractmethod
    def process(self, df: DataFrame, prefix: str = "") -> DataFrame:
        pass
