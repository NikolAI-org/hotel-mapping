from pyspark.sql import SparkSession, DataFrame
from hotel_data.pipeline.preprocessor.core.base_reader import BaseReader
from pathlib import Path

class JSONReader(BaseReader):
    def __init__(self, folder_path: str):
        self.folder_path = folder_path

    def read(self, spark: SparkSession) -> DataFrame:
        # Resolve absolute path
        path = Path(self.folder_path).resolve()
        return spark.read.option("recursiveFileLookup", "true").json(str(path))
