from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, ArrayType


class DataFrameFlattener:
    """
    A lightweight, stream-safe flattener for PySpark Structured Streaming.
    Flattens nested StructType and optionally ArrayType columns recursively.
    """

    def __init__(self, explode_arrays: bool = True):
        """
        :param explode_arrays: Whether to explode arrays of structs during flattening.
        """
        self.explode_arrays = explode_arrays

    def flatten(self, df: DataFrame, prefix: str = "") -> DataFrame:
        """
        Flatten nested struct and array-of-struct fields for streaming DataFrame.
        NOTE: This method is stateless and safe to call inside foreachBatch.
        """
        flat_cols = []

        for field in df.schema.fields:
            field_name = f"{prefix}{field.name}"
            dtype = field.dataType

            if isinstance(dtype, StructType):
                # Flatten nested struct directly via select(col("field.*"))
                nested_cols = [
                    F.col(f"{field.name}.{subfield.name}").alias(f"{field_name}_{subfield.name}")
                    for subfield in dtype.fields
                ]
                flat_cols.extend(nested_cols)

            elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
                # Explode array of structs if enabled
                if self.explode_arrays:
                    exploded_col = F.explode_outer(F.col(field.name)).alias(field_name)
                    df = df.withColumn(field_name, exploded_col)
                    nested_cols = [
                        F.col(f"{field_name}.{subfield.name}").alias(f"{field_name}_{subfield.name}")
                        for subfield in dtype.elementType.fields
                    ]
                    flat_cols.extend(nested_cols)
                else:
                    flat_cols.append(F.col(field.name).alias(field_name))

            else:
                flat_cols.append(F.col(field.name).alias(field_name))

        return df.select(*flat_cols)
