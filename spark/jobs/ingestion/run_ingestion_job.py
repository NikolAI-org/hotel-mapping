from hotel_data.pipeline.preprocessor.processors.text_preprocessor_processor import (
    TextPreprocessorProcessor,
)
from hotel_data.pipeline.preprocessor.processors.sbert_vectorizer import (
    compute_all_embeddings,
)
from hotel_data.pipeline.preprocessor.processors.geo_hash_processor import (
    GeoHashProcessor,
)
from hotel_data.pipeline.preprocessor.processors.uid_processor import UIDProcessor
from hotel_data.pipeline.preprocessor.processors.stop_word_processor import (
    StopWordProcessor,
)
from hotel_data.pipeline.preprocessor.processors.name_formatter_processor import (
    NameFormatterProcessor,
)
from hotel_data.pipeline.preprocessor.processors.default_value_processor import (
    DefaultValueProcessor,
)
from hotel_data.pipeline.preprocessor.processors.timestamp_processor import (
    TimestampAppenderProcessor,
)
from hotel_data.pipeline.preprocessor.processors.lowercase_processor import (
    LowercaseProcessor,
)
from hotel_data.pipeline.preprocessor.processors.address_combiner_processor import (
    AddressCombinerProcessor,
)
from hotel_data.pipeline.preprocessor.processors.data_processing_pipeline import (
    DataProcessingPipeline,
)
from hotel_data.pipeline.preprocessor.processors.mandatory_fields_processor import (
    MandatoryFieldsFilterProcessor,
)
from hotel_data.pipeline.preprocessor.processors.hotel_flattener_processor import (
    HotelFlattenerProcessor,
)
from hotel_data.pipeline.preprocessor.readers.json_stream_reader import JSONStreamReader
from hotel_data.schema.input.preprocessor_schema import hotel_array_schema
from hotel_data.schema.input.preprocessor_schema import hotel_struct_schema
from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.config.paths import (
    BASE_DELTA_PATH,
    CATALOG_NAME,
    SCHEMA_NAME,
    TABLE_HOTELS_NAME,
    TABLE_HOTELS_FAILED_NAME,
)
import sys
import traceback
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import struct, col

# Ensure we can import custom modules
sys.path.append("/opt/airflow")

# from spark.jobs.ingestion.preprocessing_pipeline import PreprocessingPipeline


CRITICAL_FIELDS = [
    "geoCode_lat",
    "geoCode_long",
    "name",
    "providerId",
    "contact_address_line1",
    "contact_address_city_name",
    "contact_address_country_name",
]
ADDRESS_FIELDS = [
    "contact_address_line1",
    "contact_address_city_name",
    "contact_address_state_name",
    "contact_address_country_name",
    "contact_address_postalCode",
]
EXCLUDE_LOWERCASE_FIELDS = ["original_message"]


def detect_hotel_schema(spark: SparkSession, source_path: str):
    """
    Peek at the first few lines of one file in source_path to decide which
    top-level JSON shape is present:
      - hotel_struct_schema : root is {"hotels": [...], "curatedHotels": [...]}
      - hotel_array_schema  : root IS the hotel object (flat, per-file)
    Falls back to hotel_array_schema on any error.
    """
    try:
        snippet = " ".join(
            row.value for row in spark.read.text(source_path).limit(10).collect()
        )
        if '"hotels"' in snippet:
            print("Schema auto-detected: hotel_struct_schema")
            return hotel_struct_schema
    except Exception as e:
        print(f"Schema auto-detection failed ({e}). Defaulting to hotel_array_schema.")
    print("Schema auto-detected: hotel_array_schema")
    return hotel_array_schema


def create_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def run_job():
    # 1. Parse Arguments (Decoupling logic from code)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source", required=True, help="Input S3/MinIO path for JSON files"
    )
    args, unknown = parser.parse_known_args()
    source_path = args.source

    spark = create_spark_session("HotelDataIngestion")
    spark.sparkContext.setLogLevel("WARN")

    print(f"--- Starting Ingestion from: {source_path} ---")

    # 2. Read Raw JSON — auto-detect top-level schema shape
    hotel_schema = detect_hotel_schema(spark, source_path)
    reader = JSONStreamReader(source_path, schema=hotel_schema)
    raw_df = reader.read(spark)

    # 3. Run Pipeline (Flattening, Vectorizing, etc.)
    # pipeline = PreprocessingPipeline()
    # processed_df = pipeline.run(raw_df)

    # 4. Enforce Final Bronze Schema (The Robust Way)
    # We select columns explicitly. If a processor didn't generate a column, we fill it with NULL.
    # print("--- Enforcing Bronze Schema ---")
    # select_exprs = []
    #
    # for field in flattened_hotel_schema:
    #     if field.name in processed_df.columns:
    #         # Cast ensures we don't crash on slight type mismatches (e.g. int vs long)
    #         select_exprs.append(F.col(field.name).cast(field.dataType))
    #     else:
    #         # Safe Fallback: Create null column with correct type
    #         print(f"   WARNING: Column '{field.name}' missing. Filling with NULL.")
    #         select_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))
    #
    # final_df = processed_df.select(*select_exprs)

    # 5. Write to Delta
    manager = DeltaTableManager(
        spark=spark,
        catalog_name=CATALOG_NAME,
        schema_name=SCHEMA_NAME,
        base_path=BASE_DELTA_PATH,
    )

    query = (
        raw_df.writeStream.foreachBatch(
            lambda batch_df, batch_id: process_batch(batch_df, batch_id, manager)
        )
        .option("maxFilesPerTrigger", 1)
        .trigger(once=True)
        .start()
    )

    # print(f"--- Writing to Delta Table: {TABLE_HOTELS_NAME} ---")
    # manager.write_table(
    #     table_name=TABLE_HOTELS_NAME,
    #     df=final_df,
    #     mode="append",  # Append allows adding new suppliers over time
    #     merge_schema="true"  # Updates schema if you add new fields later
    # )

    # print("Ingestion Job Completed Successfully!")
    # spark.stop()
    try:
        query.awaitTermination()
    except Exception as e:
        print(f"Query failed: {e}")
    finally:
        spark.stop()


def process_batch(batch_df, batch_id, manager):
    # REMOVED: batch_df.rdd.isEmpty() call to avoid extra actions
    print(f"--- Processing Micro-batch {batch_id} ---")
    raw_count = batch_df.count()
    print(f"🐛 DEBUG [1 - Raw Input]: Read {raw_count} records from JSON files.")
    # Initial Processors
    flat_df = HotelFlattenerProcessor(explode_arrays=True).process(batch_df)
    flat_count = flat_df.count()
    print(
        f"🐛 DEBUG [2 - Flattened]: {flat_count} records exist after flattening/coalescing."
    )
    valid_df, invalid_df = MandatoryFieldsFilterProcessor(CRITICAL_FIELDS).process(
        flat_df
    )
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    print(
        f"🐛 DEBUG [3 - Mandatory Filter]: {valid_count} passed, {invalid_count} failed."
    )

    if invalid_count > 0:
        print(
            "🐛 DEBUG [4 - Why did they fail?]: Here are 3 rejected records. Look for NULLs in critical fields!"
        )
        # We select the critical fields so you can visually see which one is causing the rejection
        invalid_df.select("id", "providerHotelId", *CRITICAL_FIELDS).show(
            3, truncate=False
        )

    # Transformation Pipeline
    transformation_pipeline = DataProcessingPipeline(
        [
            AddressCombinerProcessor(ADDRESS_FIELDS),
            LowercaseProcessor(EXCLUDE_LOWERCASE_FIELDS),
            TextPreprocessorProcessor(),
            TimestampAppenderProcessor(),
            DefaultValueProcessor(critical_fields=CRITICAL_FIELDS),
            NameFormatterProcessor(ADDRESS_FIELDS),
            StopWordProcessor(
                input_col="normalized_name", output_col="normalized_name"
            ),
            UIDProcessor(),
        ]
    )

    valid_df = transformation_pipeline.run(valid_df)
    valid_df = GeoHashProcessor().process(valid_df)

    valid_df = valid_df.repartition(60)

    # COMBINED SBERT STEP: Run UDF once for all columns
    valid_df = (
        valid_df.withColumn(
            "all_vecs",
            compute_all_embeddings(
                struct("name", "normalized_name", "combined_address")
            ),  # type: ignore
        )
        .select(
            "*",
            col("all_vecs.name_embedding"),
            col("all_vecs.normalized_name_embedding"),
            col("all_vecs.address_embedding"),
        )
        .drop("all_vecs")
    )

    # Write results (The actual Action)
    safe_write_table(
        manager, TABLE_HOTELS_NAME, valid_df, key_columns=["providerHotelId"]
    )
    safe_write_table(manager, TABLE_HOTELS_FAILED_NAME, invalid_df)
    print(f"✅ Batch {batch_id} completed.")


def safe_write_table(manager, table_name, df, key_columns=None):
    try:
        # Use MERGE for upserts when key columns are provided.
        if key_columns:
            merge_df = df
            for key_col in key_columns:
                merge_df = merge_df.filter(F.col(key_col).isNotNull())
            merge_df = merge_df.dropDuplicates(key_columns)
            manager.merge_table(table_name, merge_df, key_columns)
            return

        # Fallback to standard write for tables without a stable business key.
        manager.write_table(table_name, df)
    except Exception as e:
        print(f"Error writing to {table_name}: {e}")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    run_job()
