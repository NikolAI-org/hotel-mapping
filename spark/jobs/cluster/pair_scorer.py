from pyspark.sql import functions as F

class PairScorer:
    def __init__(self, weights: dict, t_high: float, t_low: float):
        self.weights = weights
        self.t_high = t_high
        self.t_low = t_low
        self.total_weight = sum(weights.values()) if weights else 1.0

    def process(self, pairs_df):
        weighted_expr = "+".join([f"(coalesce({k}, 0) * {v})" for k, v in self.weights.items()])
        scored_df = pairs_df.withColumn("composite_score", F.expr(f"({weighted_expr}) / {self.total_weight}"))
        return scored_df.withColumn("classification", 
            F.when(F.col("composite_score") >= self.t_high, "MATCHED")
             .when(F.col("composite_score") >= self.t_low, "MANUAL_REVIEW")
             .otherwise("UNMATCHED")
        )