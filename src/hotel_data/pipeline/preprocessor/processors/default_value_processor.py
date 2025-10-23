# address_combiner_processor.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import concat_ws, regexp_replace, col
from pyspark.sql.types import StringType, NumericType, BooleanType, TimestampType, DateType

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor

class DefaultValueProcessor(BaseProcessor):
    def __init__(self, critical_fields=None, type_defaults=None):
        """
        critical_fields: list of field names to skip null replacement.
        type_defaults: optional dict mapping data types to default values.
                       Example:
                       {
                           StringType: "N/A",
                           NumericType: -1,
                           BooleanType: True
                       }
        """
        self.critical_fields = set(critical_fields or [])

        # Default type-based values
        self.default_values_by_type = type_defaults or {
            StringType: "Unknown",
            NumericType: 0,
            BooleanType: False,
            TimestampType: F.current_timestamp(),
            DateType: F.current_date()
        }

    def process(self, df):
        for field in df.schema.fields:
            col_name = field.name

            # Skip critical fields
            if col_name in self.critical_fields:
                continue

            col_type = type(field.dataType)
            default = None

            # Find the matching default value
            for dtype, dval in self.default_values_by_type.items():
                if issubclass(col_type, dtype):
                    default = dval
                    break

            if default is not None:
                if isinstance(default, F.Column):
                    df = df.withColumn(
                        col_name,
                        F.when(F.col(col_name).isNull(), default).otherwise(F.col(col_name))
                    )
                else:
                    df = df.withColumn(
                        col_name,
                        F.when(F.col(col_name).isNull() | (F.col(col_name) == ""), F.lit(default))
                         .otherwise(F.col(col_name))
                    )
        return df
