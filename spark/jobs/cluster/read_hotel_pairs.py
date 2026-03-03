# /opt/airflow/spark/jobs/read_hotel_pairs.py

from pyspark.sql import SparkSession
import sys
import pyspark.sql.functions as F


def main():
    if len(sys.argv) < 2:
        print("ERROR: Full S3 path argument is required.")
        print("Usage: spark-submit read_hotel_pairs.py <s3_path>")
        sys.exit(1)

    full_path = sys.argv[1]

    spark = SparkSession.builder.appName(
        f"Read Delta Table - {full_path}"
    ).getOrCreate()

    print("\n==============================")
    print("DELTA TABLE INSPECTION JOB")
    print("==============================")
    print(f"Input Path: {full_path}")

    try:
        df = spark.read.format("delta").load(full_path)

        print("\n=== BASIC METRICS ===")
        print(f"Total Record Count: {df.count()}")

        print("\n=== SAMPLE DATA (Top 20 Rows) ===")
        df.show(20, truncate=False)

        print("\n=== Distinct Provider Names ===")
        df.filter((F.col("name") == "oyo rooms andheri station") | 
            (F.col("name") == "oyo rooms andheri station 2") |
            (F.col("name") == "hotel ascot neo a k palace") |
            (F.col("name") == "hotel ascot neo a k palace by oyo")
        ).show(truncate=False)
        
        # print("\n=== Distinct Status Count ===")
        # df.select("status").distinct().count()

        # print("\n=== Status Distribution ===")
        # df.groupBy("status").count().orderBy("count", ascending=False).show()

        # print("\n=== Sample Records Per Status ===")
        # from pyspark.sql.window import Window
        # from pyspark.sql.functions import row_number

        # w = Window.partitionBy("status").orderBy("status")
        # df.withColumn("rn", row_number().over(w)) \
        # .filter("rn <= 5") \
        # .drop("rn") \
        # .show(truncate=False)
        
        

        print("\n=== SCHEMA ===")
        df.printSchema()

        print("\n=== DELTA METADATA (DESCRIBE DETAIL) ===")
        spark.sql(f"DESCRIBE DETAIL delta.`{full_path}`").show(truncate=False)

        print("\n=== DELTA HISTORY (Last 5 Versions) ===")
        spark.sql(f"DESCRIBE HISTORY delta.`{full_path}` LIMIT 5").show(truncate=False)

    except Exception as e:
        print(f"Error reading Delta table: {str(e)}")
        sys.exit(1)

    spark.stop()


if __name__ == "__main__":
    main()
