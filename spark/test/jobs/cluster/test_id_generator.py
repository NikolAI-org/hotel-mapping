from pyspark.sql import functions as F
from spark.jobs.cluster.engine import CanonicalIdGenerator

def test_canonical_id_generation(spark):
    generator = CanonicalIdGenerator()
    
    # 1. Arrange: Mix of dirty data, nulls, and clean data
    data = [
        ("uid_1", "US", "New York", "NY"),           # Standard
        ("uid_2", " us ", " los angeles ", None),    # Dirty spaces & null state
        ("uid_3", None, None, None)                  # Complete fallback
    ]
    columns = ["uid", "contact_address_country_code", "contact_address_city_name", "contact_address_state_name"]
    df = spark.createDataFrame(data, columns)
    
    # 2. Act
    result_df = generator.generate(df, uid_col="uid")
    results = {row['uid']: row['cluster_id'] for row in result_df.collect()}
    
    # 3. Assert
    # Extract the SHA256 hashes manually to verify
    import hashlib
    hash_1 = hashlib.sha256(b"uid_1").hexdigest()[:12]
    hash_2 = hashlib.sha256(b"uid_2").hexdigest()[:12]
    hash_3 = hashlib.sha256(b"uid_3").hexdigest()[:12]

    # Check normalization and hash appending
    assert results["uid_1"] == f"US-NEW_YORK-NY-{hash_1}"
    
    # Check dirty data cleaning and fallback for null state
    assert results["uid_2"] == f"US-LOS_ANGELES-UNKNOWN-{hash_2}"
    
    # Check total fallback
    assert results["uid_3"] == f"XX-UNKNOWN-UNKNOWN-{hash_3}"