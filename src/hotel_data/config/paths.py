import os

# Root base directory for all delta tables
# Or for local dev: "/mnt/data/delta"
#BASE_DELTA_PATH = os.getenv("DELTA_BASE_PATH", "s3://your-bucket/hotel-data/delta")
BASE_DELTA_PATH = os.getenv("DELTA_BASE_PATH", "s3a://delta-bucket/hotel_data/delta")
WAREHOUSE_DIR = os.getenv("WAREHOUSE_DIR", "s3a://warehouse")
DERBY_HOME = "/home/akshay/spark_home/spark_derby_metastore" # Use the same path as in the reader

INPUT_FILE_PATH = "s3a://input-files"

CATALOG_NAME = "spark_catalog"
SCHEMA_NAME = "bronze"


#table names
TABLE_HOTELS_NAME = "hotels"
TABLE_HOTELS_FAILED_NAME = "hotels_err"
TABLE_HOTELS_PAIRS_NAME = "hotel_pairs"

# Individual tables
TABLE_HOTELS = f"{BASE_DELTA_PATH}/{TABLE_HOTELS_NAME}"
TABLE_HOTELS_FAILED = f"{BASE_DELTA_PATH}/{TABLE_HOTELS_FAILED_NAME}"
TABLE_HOTELS_PAIRS = f"{BASE_DELTA_PATH}/{TABLE_HOTELS_PAIRS_NAME}"
