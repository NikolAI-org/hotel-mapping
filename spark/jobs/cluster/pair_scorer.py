from pyspark.sql import functions as F


class PairScorer:
    def __init__(self, weights: dict, t_high: float, t_low: float):
        self.weights = weights
        self.t_high = t_high
        self.t_low = t_low
        self.total_weight = sum(weights.values()) if weights else 1.0

    def process(self, pairs_df):
        available_cols = set(pairs_df.columns)

        # Startup diagnostics to make config/schema mismatches obvious in Airflow logs.
        print(f"🔎 PairScorer weights: {self.weights}")
        print(
            f"🔎 PairScorer available columns ({len(pairs_df.columns)}): {pairs_df.columns}"
        )

        missing = [k for k in self.weights.keys() if k not in available_cols]
        if missing:
            raise ValueError(
                "PairScorer config contains unknown weight columns. "
                f"missing={missing}, available_columns={pairs_df.columns}"
            )

        resolved_terms = []
        resolved_weight_sum = 0.0

        for key, weight in self.weights.items():
            resolved_terms.append(
                F.coalesce(F.col(key).cast("double"), F.lit(0.0)) * F.lit(float(weight))
            )
            resolved_weight_sum += float(weight)

        if not resolved_terms or resolved_weight_sum <= 0:
            raise ValueError(
                "PairScorer requires at least one positive weight in config. "
                f"weights={self.weights}"
            )

        composite_expr = resolved_terms[0]
        for term in resolved_terms[1:]:
            composite_expr = composite_expr + term

        denom = resolved_weight_sum if resolved_weight_sum > 0 else 1.0
        scored_df = pairs_df.withColumn(
            "composite_score", (composite_expr / F.lit(denom)).cast("double")
        )

        return scored_df.withColumn(
            "classification",
            F.when(F.col("composite_score") >= self.t_high, "MATCHED")
            .when(F.col("composite_score") >= self.t_low, "MANUAL_REVIEW")
            .otherwise("UNMATCHED"),
        )
