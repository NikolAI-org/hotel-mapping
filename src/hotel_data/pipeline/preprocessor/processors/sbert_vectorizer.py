import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType
from typing import Iterator


# 1. Define the Generator UDF
# Using Iterator[pd.Series] -> Iterator[pd.Series] ensures the model loads ONCE per partition
@pandas_udf(ArrayType(FloatType()))
def compute_sbert_embeddings(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    # Import inside function to avoid serialization issues on driver
    from sentence_transformers import SentenceTransformer
    import torch

    # Load model once per partition (worker task)
    # 'all-MiniLM-L6-v2' is small, fast, and great for short text like hotel names
    # Note: Ensure workers have internet access to download from HuggingFace,
    # or broadcast the model folder using SparkFiles if offline.
    model = SentenceTransformer('all-MiniLM-L6-v2')

    # Optional: Enable GPU if available on workers
    if torch.cuda.is_available():
        print("cuda model available")
        model = model.to('cuda')
    else:
        print("cuda model not available")

    for series in iterator:
        # Batch encode the entire pandas series
        # normalize_embeddings=True ensures dot_product == cosine_similarity later
        embeddings = model.encode(
            series.tolist(),
            batch_size=128,
            show_progress_bar=False,
            normalize_embeddings=True
        )
        yield pd.Series(embeddings.tolist())


# 2. Integration Function
def add_sbert_vectors(df, input_col="name", output_col="name_embedding"):
    """
    Applies the SBERT UDF to the dataframe.
    """
    return df.withColumn(output_col, compute_sbert_embeddings(input_col))