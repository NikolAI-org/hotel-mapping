from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession

class BaseReader(ABC):
    @abstractmethod
    def read(self, spark: SparkSession) -> DataFrame:
        pass
