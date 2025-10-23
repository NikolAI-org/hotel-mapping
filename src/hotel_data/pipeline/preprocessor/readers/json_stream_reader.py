from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from hotel_data.pipeline.preprocessor.core.base_reader import BaseReader


import os

class JSONStreamReader(BaseReader):
    def __init__(self, folder_path: str, schema: StructType = None):
        if os.path.exists(folder_path):
            self.folder_path = os.path.abspath(folder_path)
        else:
            self.folder_path = folder_path  # Assume remote (S3, etc.)
        self.schema = schema

    def read(self, spark: SparkSession) -> DataFrame:
        reader = spark.readStream
        if self.schema:
            reader = reader.schema(self.schema)
        print(f"Reading stream from {self.folder_path}")
        return reader.option("recursiveFileLookup", "true").json(self.folder_path)

