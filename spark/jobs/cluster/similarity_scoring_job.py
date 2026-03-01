# /opt/airflow/spark/jobs/cluster_logic.py
import json
import os
import sys
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

class ClusterIDGenerator:
    def assign_ids(self, df):
        # We use a window to propagate the ID. 
        # Crucial: We must partition by the 'source' hotel to find its clusters
        window_group = Window.partitionBy("id_i")
        
        # Only consider MAX_PROBABILITY for actual grouping
        df_matched = df.withColumn(
            "temp_cluster_id", 
            F.when(F.col("probability_label") == "MAX_PROBABILITY", 
                   F.min("id_i").over(window_group))
             .otherwise(None)
        )
        
        # Fallback to self ID
        return df_matched.withColumn(
            "cluster_id", 
            F.coalesce(F.col("temp_cluster_id"), F.col("id_i"))
        ).drop("temp_cluster_id")

class ClusterAggregator:
    def transform_to_nested(self, df):
        # Explicitly define fields to avoid 'tuple' formatting
        comparison_struct = F.struct(
            F.col("id_j").alias("target_hotel_id"),
            F.col("name_j").alias("target_name"),
            F.col("providerName_j").alias("target_provider"),
            F.col("composite_score").alias("score"),
            F.col("probability_label").alias("classification")
        )

        return df.groupBy(
            "cluster_id", "id_i", "name_i", "uid_i", 
            "providerId_i", "providerName_i", "normalized_name_i", "combined_address_i"
        ).agg(
            F.collect_list(comparison_struct).alias("comparisons_list")
        ).withColumn(
            "comparisons_json", F.to_json(F.col("comparisons_list"))
        )

def main():
    spark = SparkSession.builder.appName("HotelClustering").getOrCreate()
    
    # Minio Configuration
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
    sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
    sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

    try:
        # Load Config
        weights = json.loads(os.getenv('WEIGHTS', '{}'))
        t_high = float(os.getenv('THRESHOLD_HIGH', 0.90))
        t_low = float(os.getenv('THRESHOLD_LOW', 0.60))

        print(f"DEBUG: Processing with weights: {weights}")

        # 1. Read
        df = spark.read.format("delta").load("s3a://data-lake/hotel_data/hotel_pairs")
        
        # 2. Score
        weighted_expr = "+".join([f"(coalesce({k}, 0) * {v})" for k, v in weights.items()])
        total_weight = sum(weights.values())
        df = df.withColumn("composite_score", F.expr(f"({weighted_expr}) / {total_weight}"))

        # 3. Classify
        df = df.withColumn("probability_label", 
            F.when(F.col("composite_score") >= t_high, "MAX_PROBABILITY")
             .when(F.col("composite_score") <= t_low, "LOW_PROBABILITY")
             .otherwise("NOT_SURE")
        )

        # 4. Cluster ID
        id_gen = ClusterIDGenerator()
        df = id_gen.assign_ids(df)

        # 5. Aggregate
        aggregator = ClusterAggregator()
        final_df = aggregator.transform_to_nested(df)

        # 6. Final Select and Write
        # Renaming i-columns to clean names
        output = final_df.select(
            "cluster_id",
            F.col("id_i").alias("id"),
            F.col("name_i").alias("name"),
            F.col("uid_i").alias("uid"),
            F.col("providerId_i").alias("providerId"),
            F.col("providerName_i").alias("providerName"),
            F.col("normalized_name_i").alias("normalized_name"),
            F.col("combined_address_i").alias("combined_address"),
            "comparisons_json"
        )

        print(f"DEBUG: Writing {output.count()} clustered records to Minio...")
        
        output.write.format("delta") \
              .mode("overwrite") \
              .option("overwriteSchema", "true") \
              .save("s3a://data-lake/hotel_data/final_clusters")

        print("SUCCESS: Job completed.")

    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}", file=sys.stderr)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()