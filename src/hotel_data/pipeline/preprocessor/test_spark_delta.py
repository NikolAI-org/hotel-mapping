from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("CheckDelta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # 👇 Force Spark to download the correct Delta JAR (Scala 2.13 build)
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.0.0")
    .getOrCreate()
)

print("✅ Spark Version:", spark.version)
print("✅ Delta Loaded Successfully!")

spark.stop()
