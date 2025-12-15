from pathlib import Path
import traceback
import time
from datetime import datetime

from py4j.protocol import Py4JNetworkError, Py4JError
from pyspark.sql import SparkSession

from hotel_data.config.paths import DERBY_HOME, INPUT_FILE_PATH, CATALOG_NAME, SCHEMA_NAME, BASE_DELTA_PATH, TABLE_HOTELS, \
    TABLE_HOTELS_FAILED, TABLE_HOTELS_NAME, TABLE_HOTELS_FAILED_NAME, WAREHOUSE_DIR
from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.pipeline.preprocessor.processors.address_combiner_processor import (
    AddressCombinerProcessor,
)
from hotel_data.pipeline.preprocessor.processors.data_processing_pipeline import (
    DataProcessingPipeline,
)
from hotel_data.pipeline.preprocessor.processors.default_value_processor import (
    DefaultValueProcessor,
)
from hotel_data.pipeline.preprocessor.processors.geo_hash_processor import GeoHashProcessor
from hotel_data.pipeline.preprocessor.processors.hotel_data_processor import (
    HotelFlattenerProcessor,
)
from hotel_data.pipeline.preprocessor.processors.lowercase_processor import (
    LowercaseProcessor,
)
from hotel_data.pipeline.preprocessor.processors.mandatory_fields_processor import (
    MandatoryFieldsFilterProcessor,
)
from hotel_data.pipeline.preprocessor.processors.name_formatter_processor import NameFormatterProcessor
from hotel_data.pipeline.preprocessor.processors.sbert_vectorizer import add_sbert_vectors
from hotel_data.pipeline.preprocessor.processors.timestamp_processor import (
    TimestampAppenderProcessor,
)
from hotel_data.pipeline.preprocessor.readers.json_stream_reader import JSONStreamReader
from hotel_data.pipeline.preprocessor.utils.hotel_data_flattner import GenericFlattener
from hotel_data.schema.delta.hotel_bronze import flattened_hotel_schema
from hotel_data.schema.input.preprocessor_schema import hotel_schema
import time


# processors = [
#     NullHandler({"starRating": 0, "name": "Unknown"}),
#     SpecialCharCleaner(["name", "category"]),
#     AddressFormatter(["line1", "city", "state", "postalCode"]),
# ]

# BASE_PATH="/home/akshay/workspace/python_workspace/hotel_data/data/delta"
SUCCESS_COMMENT = "Raw data ingested for valid hotel input"
FAILURE_COMMENT = "Raw data ingested for invalid hotel input"

CRITICAL_FIELDS = [
    "geoCode_lat",
    "geoCode_long",
    "name",
    "providerId",
    "contact_address_line1",
    "contact_address_city_name",
    "contact_address_country_name",
]

NAME_FORMATTER_FIELDS = [
    "contact_address_city_name",
    "contact_address_state_name",
    "contact_address_country_name"
]

ADDRESS_FIELDS = [
    "contact_address_line1",
    "contact_address_city_name",
    "contact_address_state_name",
    "contact_address_country_name",
    "contact_address_postalCode",
]

EXCLUDE_LOWERCASE_FIELDS = ["original_message"]

genericFlattner = GenericFlattener(explode_arrays=True)
Path(DERBY_HOME).mkdir(parents=True, exist_ok=True)

def main():
    spark = (
        SparkSession.builder.appName("HotelsPipelineWrite")
        .config("spark.jars.packages", ",".join([
            "io.delta:delta-spark_2.13:4.0.0",
            "org.apache.hadoop:hadoop-aws:3.4.1",
        ]))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        # ---- S3/MinIO config ----
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://172.16.16.152:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ---- Hive metastore (Derby) for local prod-like testing ----
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:{DERBY_HOME}/metastore_db;create=true",
        )
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionDriverName",
            "org.apache.derby.jdbc.EmbeddedDriver",
        )
        .config("spark.hadoop.datanucleus.autoCreateSchema", "true")
        .config("spark.hadoop.datanucleus.fixedDatastore", "true")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "4g")
        .config("spark.hadoop.fs.s3a.connection.maximum", "64")
        .config("spark.hadoop.fs.s3a.threads.max", "64")
        .config("spark.sql.shuffle.partitions", "8")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    # spark.sql("SHOW DATABASES").show()

    # 1. Read
    reader = JSONStreamReader(
        INPUT_FILE_PATH,
        schema=hotel_schema,
    )
    df_raw = reader.read(spark)

    manager = DeltaTableManager(
        spark=spark,
        catalog_name=CATALOG_NAME,
        schema_name=SCHEMA_NAME,
        base_path=BASE_DELTA_PATH,
    )

    create_table(
        manager=manager,
        spark=spark,
        tableName=TABLE_HOTELS_NAME,
        comment=SUCCESS_COMMENT,
    )
    create_table(
        manager=manager,
        spark=spark,
        tableName=TABLE_HOTELS_FAILED_NAME,
        comment=FAILURE_COMMENT,
    )

    query = (
        df_raw.writeStream.foreachBatch(
            lambda batch_df, batch_id: process_batch(batch_df, batch_id, manager)
        ).option(
            "maxFilesPerTrigger", 1
        )  # 1 file per micro-batch
        # .trigger(processingTime="1 second")  # optional, default micro-batch
        .trigger(once=True)  # process all available files and then stop
        # .outputMode("update")  # or "append"
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Streaming interrupted by user.")
    except Exception as e:
        print(f"Streaming query failed: {e}")
    finally:
        print("Stopping Spark session gracefully...")
        stop_spark_gracefully(spark)


def stop_spark_gracefully(spark: SparkSession, wait_seconds: int = 5):
    """
    Safely stop Spark session and active streaming queries.
    Ignores Py4J/JVM disconnect errors.
    """
    try:
        # Stop all active streaming queries
        for query in spark.streams.active:
            try:
                print(f"Stopping streaming query: {query.name}")
                query.stop()
            except Exception as e:
                print(f"Warning: Failed to stop query {query.name}: {e}")

        # Small wait for threads to terminate
        time.sleep(wait_seconds)

        # Stop Spark session
        try:
            print("Stopping Spark session...")
            spark.stop()
        except Exception as e:
            print(f"Warning: Spark stop failed (likely JVM already dead): {e}")

    except Exception as e:
        # Catch any other unexpected errors
        print(f"Warning: Error during Spark shutdown: {e}")


# Apply flattening inside foreachBatch
def process_batch(batch_df, batch_id, manager):
    if batch_df.head(1) == []:
        return

    flatten = HotelFlattenerProcessor(explode_arrays=True)
    mandatory = MandatoryFieldsFilterProcessor(CRITICAL_FIELDS)

    df = flatten.process(batch_df)
    valid_df, invalid_df = mandatory.process(df)

    pipeline = DataProcessingPipeline([
        AddressCombinerProcessor(ADDRESS_FIELDS),
        LowercaseProcessor(EXCLUDE_LOWERCASE_FIELDS),
        TimestampAppenderProcessor(),
        DefaultValueProcessor(CRITICAL_FIELDS),
        NameFormatterProcessor(ADDRESS_FIELDS),
        GeoHashProcessor()
    ])

    valid_df = pipeline.run(valid_df).persist()
    valid_df.count()  # materialize once

    safe_write_table(manager, TABLE_HOTELS_NAME, valid_df)
    safe_write_table(manager, TABLE_HOTELS_FAILED_NAME, invalid_df)



def safe_write_table(manager, table_name, df, mode="append", path=None):
    """Wrap write in robust error handling to avoid unhandled exceptions in foreachBatch."""
    try:
        if df is None or df.rdd.isEmpty():
            return
        # keep the actual write in try/except
        try:
            manager.write_table(table_name, df)  # your existing API
        except Exception as write_exc:
            # inspect and log
            print(
                f"[ERROR] write_table failed for {table_name}: {write_exc}", flush=True
            )
            traceback.print_exc()
            # decide whether to re-raise or swallow depending on semantics
            # For streaming stability, swallow after logging so JVM-side thread doesn't die
    except (Py4JNetworkError, Py4JError, EOFError, ConnectionRefusedError) as py4j_exc:
        # JVM not reachable — log and let outer control handle termination
        print(
            "[CRITICAL] Py4J error inside safe_write_table: JVM likely dead.",
            flush=True,
        )
        traceback.print_exc()
        # do not call any more Py4J bound ops here
    except Exception as e:
        print("[ERROR] Unexpected error in safe_write_table:", e, flush=True)
        traceback.print_exc()


def create_table(
    manager: DeltaTableManager, spark: SparkSession, tableName: str, comment: str
):
    empty_df = spark.createDataFrame([], schema=flattened_hotel_schema)
    # Create table (if not exists)
    manager.create_table(tableName, empty_df, comment=comment)


def datetime_handler(x):
    if isinstance(x, datetime):
        return x.isoformat()
    raise TypeError(f"Type {type(x)} not serializable")


if __name__ == "__main__":
    main()
