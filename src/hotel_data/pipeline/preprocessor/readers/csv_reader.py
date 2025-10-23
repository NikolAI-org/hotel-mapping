from pyspark.sql import SparkSession, DataFrame
from hotel_data.pipeline.preprocessor.core.base_reader import BaseReader

class CSVReader(BaseReader):
    def __init__(self, path: str, header: bool = True):
        self.path = path
        self.header = header

    def read(self, spark: SparkSession) -> DataFrame:
        return (spark.read
                .option("header", str(self.header).lower())
                .csv(self.path))
