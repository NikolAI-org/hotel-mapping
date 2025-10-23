from pyspark.sql import DataFrame
from hotel_data.pipeline.preprocessor.core.base_writer import BaseWriter

class DeltaWriter(BaseWriter):
    def __init__(self, path: str, mode: str = "append"):
        self.path = path
        self.mode = mode

    def write(self, df: DataFrame) -> None:
        (df.write
           .format("delta")
           .mode(self.mode)
           .option("mergeSchema", "true")
           .save(self.path))
