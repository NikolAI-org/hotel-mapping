import json
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    concat_ws,
    lower,
    regexp_replace,
    current_timestamp,
    lit,
)
from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.pipeline.preprocessor.flatten_data import DataFrameFlattener
from hotel_data.pipeline.preprocessor.processors.data_processing_pipeline import (
    DataProcessingPipeline,
)
from hotel_data.pipeline.preprocessor.processors.default_value_processor import (
    DefaultValueProcessor,
)
from hotel_data.pipeline.preprocessor.processors.hotel_data_processor import (
    HotelFlattenerProcessor,
)
from hotel_data.pipeline.preprocessor.processors.lowercase_processor import (
    LowercaseProcessor,
)
from hotel_data.pipeline.preprocessor.processors.mandatory_fields_processor import (
    MandatoryFieldsFilterProcessor,
)
from hotel_data.pipeline.preprocessor.processors.timestamp_processor import (
    TimestampAppenderProcessor,
)
from hotel_data.pipeline.preprocessor.utils.hotel_data_flattner import GenericFlattener
from hotel_data.pipeline.preprocessor.readers.json_reader import JSONReader
from hotel_data.pipeline.preprocessor.readers.csv_reader import CSVReader
from hotel_data.pipeline.preprocessor.processors.null_handler import NullHandler
from hotel_data.pipeline.preprocessor.processors.special_char_cleaner import (
    SpecialCharCleaner,
)
from hotel_data.pipeline.preprocessor.processors.address_combiner_processor import (
    AddressCombinerProcessor,
)
from hotel_data.pipeline.preprocessor.readers.json_stream_reader import JSONStreamReader
from hotel_data.pipeline.preprocessor.writers.delta_writer import DeltaWriter
from hotel_data.schema.input.preprocessor_schema import hotel_schema
from hotel_data.schema.delta.hotel_bronze import flattened_hotel_schema
from datetime import datetime

# processors = [
#     NullHandler({"starRating": 0, "name": "Unknown"}),
#     SpecialCharCleaner(["name", "category"]),
#     AddressFormatter(["line1", "city", "state", "postalCode"]),
# ]

# WAREHOUSE_DIR = "/home/akshay/workspace/python_workspace/hotel_data/data/delta"
WAREHOUSE_DIR = "s3a://warehouse"

SUCCESS_TABLE_NAME = "hotels"
FAILURE_TABLE_NAME = "hotels_err"
CATALOG_NAME = "spark_catalog"
SCHEMA_NAME = "bronze"
# BASE_PATH="/home/akshay/workspace/python_workspace/hotel_data/data/delta"
BASE_PATH = "s3a://delta-bucket/hotel_data/delta"
SUCCESS_COMMENT = "Raw data ingested for valid hotel input"
FAILURE_COMMENT = "Raw data ingested for invalid hotel input"

# INPUT_FILE_PATH = "/home/akshay/Documents/hotel_data/data"
INPUT_FILE_PATH = "s3a://input-files"

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

processors = [NullHandler({"hotels_starRating": 0, "hotels_name": "Unknown"})]
genericFlattner = GenericFlattener(explode_arrays=True)


def main():
    spark = (
        SparkSession.builder.appName("HotelsPipeline")
        # ✅ JARs for Delta + Hadoop AWS + AWS SDK
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "io.delta:delta-spark_2.13:4.0.0",
                    "org.apache.hadoop:hadoop-aws:3.4.1",
                ]
            ),
        )
        # ✅ Delta SQL extensions
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ✅ S3A implementation
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # .config(
        #     "spark.hadoop.fs.s3a.endpoint",
        #     "http://seaweedfs-s3.delta-store.svc.cluster.local:8333",
        # )
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.mkdirs.enabled", "true")
        .config("spark.hadoop.fs.s3a.connection.maximum", "100")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "10")
        .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "sequential")
        .config("spark.hadoop.fs.s3a.fail.on.empty.path", "false")
        .config(
            "spark.databricks.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        )  # .config(
        #     "spark.hadoop.fs.s3a.aws.credentials.provider",
        #     "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        # )
        # ✅ S3A timeouts & retries (milliseconds)
        # .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        # .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
        # .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000")
        # .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
        # .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000")
        # .config("spark.hadoop.fs.s3a.log.events", "true")
        # ✅ Multipart uploads (values in bytes)
        # .config(
        #     "spark.hadoop.fs.s3a.multipart.threshold", str(128 * 1024 * 1024)
        # )  # 128 MB
        # .config("spark.hadoop.fs.s3a.multipart.size", str(64 * 1024 * 1024))  # 64 MB
        # .config("spark.hadoop.fs.s3a.multipart.purge", "false")
        # .config(
        #     "spark.hadoop.fs.s3a.multipart.purge.age", str(24 * 60 * 60 * 1000)
        # )  # 24h in ms
        # .config(
        #     "spark.hadoop.fs.s3a.multipart.purge.age.seconds", str(24 * 60 * 60)
        # )  # 24h in seconds
        # ✅ Delta + Hive compatibility
        # .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        # .config("spark.sql.catalogImplementation", "hive")
        # .enableHiveSupport()
        .getOrCreate()
    )

    spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    spark.conf.set(
        "spark.delta.logStore.class",
        "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
    )
    spark.sparkContext.setLogLevel("WARN")

    hadoopConf = spark._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", "http://seaweedfs-s3.seaweedfs.svc.cluster.local:8333")
    hadoopConf.set("fs.s3a.access.key", "admin")
    hadoopConf.set("fs.s3a.secret.key", "password")
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoopConf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    hadoopConf.set("fs.s3a.mkdirs.enabled", "true")

    # Create a Delta database
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
    spark.sql("SHOW DATABASES").show()

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
        base_path=BASE_PATH,
    )

    create_table(
        manager=manager,
        spark=spark,
        tableName=SUCCESS_TABLE_NAME,
        comment=SUCCESS_COMMENT,
    )
    create_table(
        manager=manager,
        spark=spark,
        tableName=FAILURE_TABLE_NAME,
        comment=FAILURE_COMMENT,
    )

    # query = (
    #     df_raw.writeStream.foreachBatch(
    #         lambda batch_df, batch_id: process_batch(batch_df, batch_id, manager)
    #     ).option(
    #         "maxFilesPerTrigger", 1
    #     )  # 1 file per micro-batch
    #     # .trigger(processingTime="1 second")  # optional, default micro-batch
    #     .trigger(once=True)  # process all available files and then stop
    #     # .outputMode("update")  # or "append"
    #     .start()
    # )

    # query.awaitTermination()

    # spark.stop()


def debug_s3a_conf(spark):
    conf = spark._jsc.hadoopConfiguration()
    iterator = conf.iterator()
    while iterator.hasNext():
        entry = iterator.next()
        k, v = entry.getKey(), entry.getValue()
        if "s3a" in k.lower():
            if isinstance(v, str) and any(ch.isalpha() for ch in v):  # contains h,m,s
                print(f"⚠️ Problematic config: {k} = {v}")
            else:
                print(f"OK: {k} = {v}")


# Apply flattening inside foreachBatch
def process_batch(batch_df, batch_id, manager: DeltaTableManager):
    if batch_df.rdd.isEmpty():
        print(f"⚠️ Skipping empty batch {batch_id}")
        return

    print(f"----- Micro-batch {batch_id} -----")

    # Define processors
    flatten_processor = HotelFlattenerProcessor(explode_arrays=True)
    critical_processor = MandatoryFieldsFilterProcessor(CRITICAL_FIELDS)
    address_processor = AddressCombinerProcessor(ADDRESS_FIELDS)
    lowercase_processor = LowercaseProcessor(EXCLUDE_LOWERCASE_FIELDS)
    timestamp_processor = TimestampAppenderProcessor()
    default_value_processor = DefaultValueProcessor(critical_fields=CRITICAL_FIELDS)

    # Pipeline step 1: flatten
    flat_df = flatten_processor.process(batch_df)

    # Pipeline step 2: filter valid/invalid
    valid_df, invalid_df = critical_processor.process(flat_df)

    # Pipeline step 3: transform valid records
    transformation_pipeline = DataProcessingPipeline(
        [
            address_processor,
            lowercase_processor,
            timestamp_processor,
            default_value_processor,
        ]
    )
    valid_df = transformation_pipeline.run(valid_df)
    # for row in valid_df.limit(1).collect():
    #     print(json.dumps(row.asDict(recursive=True), indent=2, default=datetime_handler))

    # Pipeline step 4: write results
    manager.write_table(SUCCESS_TABLE_NAME, valid_df)
    manager.write_table(FAILURE_TABLE_NAME, invalid_df)

    print(f"✅ Written valid records for batch {batch_id}, Invalid: {valid_df.count()}")


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
