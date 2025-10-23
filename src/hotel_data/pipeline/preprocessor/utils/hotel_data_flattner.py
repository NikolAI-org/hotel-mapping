from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, ArrayType

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor

class GenericFlattener:
    """
    Generic flattener for PySpark DataFrames:
    - Flattens nested structs recursively
    - Optionally explodes arrays of structs
    - Preserves arrays of primitives
    """

    def __init__(self, explode_arrays: bool = False):
        """
        :param explode_arrays: If True, arrays of structs are exploded
        """
        self.explode_arrays = explode_arrays

    def flatten(self, df: DataFrame, prefix: str = "", is_recursive: bool = False) -> DataFrame:
        """
        Recursively flatten a DataFrame, retaining the original JSON.
        :param df: input DataFrame
        :param prefix: prefix for nested columns
        :param is_recursive: internal flag to prevent re-adding 'original_message'
        :return: flattened DataFrame
        """
        # ✅ Add original JSON only once (top-level)
        if not is_recursive and "_original_message" not in df.columns:
            df = df.withColumn("_original_message", F.to_json(F.struct(*df.columns)))

        flat_cols = []
        nested_cols_to_flatten = []

        # Traverse schema
        for field in df.schema.fields:
            field_name = f"{prefix}{field.name}" if prefix else field.name
            dtype = field.dataType

            if isinstance(dtype, StructType):
                nested_cols_to_flatten.append((field_name, field.name, dtype))

            elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
                if self.explode_arrays:
                    exploded_col = F.explode_outer(F.col(field.name)).alias(field_name)
                    df = df.withColumn(field_name, exploded_col)
                    nested_cols_to_flatten.append((field_name, field_name, dtype.elementType))
                else:
                    flat_cols.append(F.col(field.name).alias(field_name))

            else:
                flat_cols.append(F.col(field.name).alias(field_name))

        # ✅ Include _original_message only once in selection
        select_cols = [*flat_cols, *[F.col(name) for _, name, _ in nested_cols_to_flatten]]
        if "_original_message" in df.columns and not any(
            c._jc.toString().endswith("_original_message") for c in select_cols if hasattr(c, "_jc")
        ):
            select_cols.append(F.col("_original_message"))

        # ✅ Drop any duplicate column references before selecting
        seen = set()
        unique_cols = []
        for c in select_cols:
            alias_name = str(c._jc.toString()) if hasattr(c, "_jc") else str(c)
            if alias_name not in seen:
                seen.add(alias_name)
                unique_cols.append(c)

        df = df.select(*unique_cols)

        # Recursively flatten nested structs
        for new_prefix, col_name, struct_type in nested_cols_to_flatten:
            for subfield in struct_type.fields:
                subfield_name = f"{new_prefix}_{subfield.name}"
                df = df.withColumn(subfield_name, F.col(f"{col_name}.{subfield.name}"))
            df = df.drop(col_name)

        # Continue until no structs remain
        remaining_nested = [f for f in df.schema.fields if isinstance(f.dataType, StructType)]
        if remaining_nested:
            return self.flatten(df, prefix="", is_recursive=True)

        # ✅ Rename only once at the end
        df = df.withColumnRenamed("_original_message", "original_message")
        
        return df
