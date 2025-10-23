# pipeline.py
from typing import List
from pyspark.sql import DataFrame

class DataProcessingPipeline:
    def __init__(self, processors: List):
        self.processors = processors

    def run(self, df: DataFrame):
        for processor in self.processors:
            print(f"➡️ Running: {processor.__class__.__name__}")
            df = processor.process(df)
        return df
