import os
import fcntl
import pandas as pd
from typing import Iterator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, col, struct
from pyspark.sql.types import ArrayType, FloatType, StructType, StructField

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor

# Path used to serialise concurrent torch/SBERT loads across Python workers.
# libtorch_cpu.so is ~1.8 GB; two workers calling dlopen() simultaneously
# exhausts mmap resources and raises ImportError: failed to map segment.
_TORCH_LOAD_LOCK = "/tmp/spark_torch_load.lock"

# 1. Define the schema for the returned embeddings
embedding_schema = StructType(
    [
        StructField("name_embedding", ArrayType(FloatType())),
        StructField("normalized_name_embedding", ArrayType(FloatType())),
        StructField("address_embedding", ArrayType(FloatType())),
    ]
)


# 2. Define your highly optimized Pandas UDF
@pandas_udf(embedding_schema, "map_iter")
def compute_all_embeddings(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    # Reduce thread contention
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["HF_HOME"] = "/tmp/hf_cache"
    os.environ["SENTENCE_TRANSFORMERS_HOME"] = "/opt/spark/hf_cache"

    # Acquire an exclusive file lock before dlopen-ing libtorch_cpu.so.
    # This serialises concurrent Python workers so only one maps the ~1.8 GB
    # shared library at a time, preventing mmap ENOMEM when executor-cores > 1.
    #
    # Also set umask=0 so every file/dir created by HuggingFace's cache
    # (model weights, lock files, blobs) is world-writable (0o666/0o777).
    # Without this, files written by the first worker (e.g. spark UID 999)
    # are 0o644/0o755 and subsequent workers from different PIDs get
    # PermissionError when trying to create .lock files inside the cache.
    import stat

    _old_umask = os.umask(0)
    try:
        with open(_TORCH_LOAD_LOCK, "w") as _lock:
            fcntl.flock(_lock, fcntl.LOCK_EX)
            from sentence_transformers import SentenceTransformer
            import torch

            # Singleton Model
            model = SentenceTransformer("all-MiniLM-L6-v2")
            device = "cuda" if torch.cuda.is_available() else "cpu"
            model = model.to(device)
        # Lock released — next worker can now safely load its own model copy.
    finally:
        os.umask(_old_umask)

    for pdf in iterator:
        # We use a very small internal batch size to keep memory stable
        encoding_kwargs = {
            "batch_size": 16,
            "show_progress_bar": False,
            "convert_to_numpy": True,
        }

        name_vecs = model.encode(
            pdf["name"].fillna("").tolist(), **encoding_kwargs
        ).tolist()
        norm_vecs = model.encode(
            pdf["normalized_name"].fillna("").tolist(), **encoding_kwargs
        ).tolist()
        addr_vecs = model.encode(
            pdf["combined_address"].fillna("").tolist(), **encoding_kwargs
        ).tolist()

        yield pd.DataFrame(
            {
                "name_embedding": name_vecs,
                "normalized_name_embedding": norm_vecs,
                "address_embedding": addr_vecs,
            }
        )

        # Explicitly clear internal cache if using GPU
        if device == "cuda":
            torch.cuda.empty_cache()


# 3. The Class Wrapper expected by Airflow
class SbertVectorizer(BaseProcessor[DataFrame]):
    """
    Applies the Pandas UDF to the DataFrame seamlessly without triggering a Shuffle/Join.
    """

    def process(self, df: DataFrame, prefix: str = "") -> DataFrame:
        print("➡️ Running SbertVectorizer (Fast Pandas UDF)...")

        # Apply the optimized Pandas UDF to all three columns at once
        df_with_vecs = df.withColumn(
            "all_vecs",
            compute_all_embeddings(
                struct("name", "normalized_name", "combined_address")
            ),
        )

        # Flatten the struct into top-level columns and drop the struct
        return df_with_vecs.select(
            "*",
            col("all_vecs.name_embedding"),
            col("all_vecs.normalized_name_embedding"),
            col("all_vecs.address_embedding"),
        ).drop("all_vecs")
