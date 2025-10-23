from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseWriter(ABC):
    @abstractmethod
    def write(self, df: DataFrame) -> None:
        pass
