import traceback
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, col

from hotel_data.config.paths import (
    INPUT_FILE_PATH, CATALOG_NAME, SCHEMA_NAME, BASE_DELTA_PATH, 
    TABLE_HOTELS_NAME, TABLE_HOTELS_FAILED_NAME
)
from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.pipeline.preprocessor.processors.address_combiner_processor import AddressCombinerProcessor
from hotel_data.pipeline.preprocessor.processors.data_processing_pipeline import DataProcessingPipeline
from hotel_data.pipeline.preprocessor.processors.default_value_processor import DefaultValueProcessor
from hotel_data.pipeline.preprocessor.processors.geo_hash_processor import GeoHashProcessor
from hotel_data.pipeline.preprocessor.processors.hotel_data_processor import HotelFlattenerProcessor
from hotel_data.pipeline.preprocessor.processors.lowercase_processor import LowercaseProcessor
from hotel_data.pipeline.preprocessor.processors.mandatory_fields_processor import MandatoryFieldsFilterProcessor
from hotel_data.pipeline.preprocessor.processors.name_formatter_processor import NameFormatterProcessor
from hotel_data.pipeline.preprocessor.processors.stop_word_processor import StopWordProcessor
from hotel_data.pipeline.preprocessor.processors.timestamp_processor import TimestampAppenderProcessor
from hotel_data.pipeline.preprocessor.processors.uid_processor import UIDProcessor
from hotel_data.pipeline.preprocessor.readers.json_stream_reader import JSONStreamReader
from hotel_data.schema.delta.hotel_bronze import flattened_hotel_schema
from hotel_data.schema.input.preprocessor_schema import hotel_schema

# Import the new combined UDF
from hotel_data.pipeline.preprocessor.processors.sbert_vectorizer import compute_all_embeddings

CRITICAL_FIELDS = ["geoCode_lat", "geoCode_long", "name", "providerId", "contact_address_line1", "contact_address_city_name", "contact_address_country_name"]
ADDRESS_FIELDS = ["contact_address_line1", "contact_address_city_name", "contact_address_state_name", "contact_address_country_name", "contact_address_postalCode"]
EXCLUDE_LOWERCASE_FIELDS = ["original_message"]

def main():
    spark = (
        SparkSession.builder.appName("HotelsPipeline")
        .master("local[1]")  # CRITICAL: Prevent RAM clashing by using 1 worker
        # ... your other configs ...
        # 1. Limit the size of Arrow batches sent to Python
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "100") 
        # 2. Give the Python worker a specific memory limit (e.g., 2GB)
        .config("spark.python.worker.memory", "2g")
        # 3. Enable the faulthandler as suggested by your error log
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.4.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "6g")    # Increased driver memory
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.1.4:9000") # Updated to your target IP
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    reader = JSONStreamReader(INPUT_FILE_PATH, schema=hotel_schema)
    df_raw = reader.read(spark)

    manager = DeltaTableManager(spark=spark, catalog_name=CATALOG_NAME, schema_name=SCHEMA_NAME, base_path=BASE_DELTA_PATH)

    # Initialize tables
    empty_df = spark.createDataFrame([], schema=flattened_hotel_schema)
    manager.create_table(TABLE_HOTELS_NAME, empty_df, comment="Valid hotels")
    manager.create_table(TABLE_HOTELS_FAILED_NAME, empty_df, comment="Invalid hotels")

    query = (
        df_raw.writeStream.foreachBatch(
            lambda batch_df, batch_id: process_batch(batch_df, batch_id, manager)
        )
        .option("maxFilesPerTrigger", 1)
        .trigger(once=True)
        .start()
    )

    try:
        query.awaitTermination()
    except Exception as e:
        print(f"Query failed: {e}")
    finally:
        spark.stop()

def process_batch(batch_df, batch_id, manager):
    # REMOVED: batch_df.rdd.isEmpty() call to avoid extra actions
    print(f"--- Processing Micro-batch {batch_id} ---")

    # Initial Processors
    flat_df = HotelFlattenerProcessor(explode_arrays=True).process(batch_df)
    valid_df, invalid_df = MandatoryFieldsFilterProcessor(CRITICAL_FIELDS).process(flat_df)

    # Transformation Pipeline
    transformation_pipeline = DataProcessingPipeline([
        AddressCombinerProcessor(ADDRESS_FIELDS),
        LowercaseProcessor(EXCLUDE_LOWERCASE_FIELDS),
        TimestampAppenderProcessor(),
        DefaultValueProcessor(critical_fields=CRITICAL_FIELDS),
        NameFormatterProcessor(ADDRESS_FIELDS),
        StopWordProcessor(input_col="normalized_name", output_col="normalized_name"),
        UIDProcessor()
    ])
    
    valid_df = transformation_pipeline.run(valid_df)
    valid_df = GeoHashProcessor().process(valid_df)

    # COMBINED SBERT STEP: Run UDF once for all columns
    valid_df = valid_df.withColumn(
        "all_vecs", 
        compute_all_embeddings(struct("name", "normalized_name", "combined_address")) # type: ignore
    ).select(
        "*",
        col("all_vecs.name_embedding"),
        col("all_vecs.normalized_name_embedding"),
        col("all_vecs.address_embedding")
    ).drop("all_vecs")

    # Write results (The actual Action)
    safe_write_table(manager, TABLE_HOTELS_NAME, valid_df)
    safe_write_table(manager, TABLE_HOTELS_FAILED_NAME, invalid_df)
    print(f"✅ Batch {batch_id} completed.")

def safe_write_table(manager, table_name, df):
    try:
        # Avoid .isEmpty() check here; Delta will handle empty writes gracefully
        manager.write_table(table_name, df)
    except Exception as e:
        print(f"Error writing to {table_name}: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()