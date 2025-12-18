from pyspark.sql.functions import struct

import pandas as pd
import torch
from sentence_transformers import SentenceTransformer
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, FloatType, StructType, StructField
from typing import Iterator
from pyspark.sql.functions import PandasUDFType



# Define the schema for the returned embeddings
embedding_schema = StructType([
    StructField("name_embedding", ArrayType(FloatType())),
    StructField("normalized_name_embedding", ArrayType(FloatType())),
    StructField("address_embedding", ArrayType(FloatType()))
])

_model_cache = None

@pandas_udf(embedding_schema, "map_iter") # type: ignore
def compute_all_embeddings(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    import os
    # Reduce thread contention
    os.environ["OMP_NUM_THREADS"] = "1" 
    
    from sentence_transformers import SentenceTransformer
    import torch
    
    # Singleton Model
    model = SentenceTransformer('all-MiniLM-L6-v2')
    # Use CPU if your local GPU RAM is low (GPU OOMs often cause connection resets)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = model.to(device)

    for pdf in iterator:
        # We use a very small internal batch size to keep memory stable
        encoding_kwargs = {
            "batch_size": 16, 
            "show_progress_bar": False,
            "convert_to_numpy": True
        }
        
        name_vecs = model.encode(pdf['name'].fillna("").tolist(), **encoding_kwargs).tolist()
        norm_vecs = model.encode(pdf['normalized_name'].fillna("").tolist(), **encoding_kwargs).tolist()
        addr_vecs = model.encode(pdf['combined_address'].fillna("").tolist(), **encoding_kwargs).tolist()
        
        yield pd.DataFrame({
            "name_embedding": name_vecs,
            "normalized_name_embedding": norm_vecs,
            "address_embedding": addr_vecs
        })
        
        # Explicitly clear internal cache if using GPU
        if device == "cuda":
            torch.cuda.empty_cache()