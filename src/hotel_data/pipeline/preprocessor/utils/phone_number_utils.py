from pyspark.sql import functions as F


def normalize_phone_expr(c, n=10):
    # 1. Strip all non-digits
    normalized_col = F.transform(c, lambda x: F.regexp_replace(x, "[^0-9]", ""))

    # 2. Take the rightmost N digits (e.g., 10) for every element in the array
    # F.transform applies a transformation to every element 'x' of array 'c'
    return F.transform(
        normalized_col,
        lambda x: F.when(
            F.length(x) > n,
            # Substring starts at length - 10 + 1 (to get the last 10 characters)
            # F.substring(x, F.length(x) - n + 1, n)
            # lambda x: x.substr(F.length(x) - n + 1, F.lit(n))
            x.substr(F.length(x) - n + 1, F.lit(n)),
        ).otherwise(x),
        # If the number is already < 10 digits, we keep it as is.
    )


# 2. Overlap Check: Returns TRUE if the array intersection size > 0.
def arrays_overlap_check(col_a, col_b):
    return F.size(F.array_intersect(F.col(col_a), F.col(col_b))) > 0
