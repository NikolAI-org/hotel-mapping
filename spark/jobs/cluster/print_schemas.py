"""Print column names for all pipeline Delta tables."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("PrintSchemas").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

BASE = "s3a://delta-lake/bronze"
tables = ["hotels", "hotel_pairs", "cluster_mappings", "canonical_registry", "comparison_audit_log"]

for t in tables:
    try:
        df = spark.read.format("delta").load(f"{BASE}/{t}")
        print(f"\n=== {t} ===")
        for c in df.columns:
            print(f"  {c}")
    except Exception as e:
        print(f"\n=== {t} — ERROR: {e}")

spark.stop()
