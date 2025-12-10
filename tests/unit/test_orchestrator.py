# tests/unit/test_orchestrator.py

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from hotel_data.config.scoring_config import ClusteringConfig, HotelClusteringConfig, LoggingConfig, ScoringConfig, StorageConfig, StreamingConfig
from hotel_data.pipeline.clustering.infrastructure.dependency_container import DependencyContainer

@pytest.fixture
def spark():
    return SparkSession.builder \
        .appName("test_orchestrator") \
        .master("local") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()

@pytest.fixture
def config():
    """Create minimal test config"""
    return HotelClusteringConfig(
        storage=StorageConfig(
            minio_endpoint="http://minio:9000",
            minio_access_key="minioadmin",
            minio_secret_key="minioadmin",
            minio_bucket="test",
            delta_path="s3a://test/delta"
        ),
        scoring=ScoringConfig(
            weights={
                'geo_distance_km': 0.15,
                'name_score_sbert': 0.25,
                'normalized_name_score_sbert': 0.20,
                'address_sbert_score': 0.15,
                'phone_match_score': 0.10,
                'postal_code_match': 0.05,
                'country_match': 0.05,
                'email_match_score': 0.05
            },
            thresholds={
                'high_confidence': 0.85,
                'medium_confidence': 0.70,
                'low_confidence': 0.55
            },
            exclusion_rules={
                'max_geo_distance_km': 5.0,
                'min_country_match': 1.0,
                'min_normalized_name_score': 0.3
            }
        ),
        clustering=ClusteringConfig(
            min_score_threshold=0.70,
            max_cluster_size=None,
            enable_black_hole_prevention=True,
            black_hole_max_threshold=0.95
        ),
        streaming=StreamingConfig(
            enabled=False,
            checkpoint_path="s3a://test/checkpoints",
            trigger_interval="5 minutes",
            max_batch_size=10000
        ),
        logging=LoggingConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            output_file=None
        )
    )

def test_orchestrator_initialization(spark, config):
    """Test orchestrator can be initialized"""
    DependencyContainer.configure(config, spark)
    
    orchestrator = DependencyContainer.get_orchestrator()
    
    assert orchestrator is not None
    assert orchestrator.health_check()
    
    DependencyContainer.reset()

def test_orchestrator_run_batch(spark, config):
    """Test orchestrator can run full pipeline"""
    DependencyContainer.configure(config, spark)
    
    orchestrator = DependencyContainer.get_orchestrator()
    logger = DependencyContainer.get_logger()
    
    # Create test data
    pairs_schema = StructType([
        StructField("id_i", StringType(), False),
        StructField("id_j", StringType(), False),
        StructField("geo_distance_km", DoubleType(), False),
        StructField("name_score_sbert", DoubleType(), False),
        StructField("normalized_name_score_sbert", DoubleType(), False),
        StructField("address_sbert_score", DoubleType(), False),
        StructField("phone_match_score", DoubleType(), False),
        StructField("postal_code_match", DoubleType(), False),
        StructField("country_match", DoubleType(), False),
        StructField("email_match_score", DoubleType(), False)
    ])
    
    pairs_data = [
        ("h1", "h2", 0.9, 0.95, 0.93, 0.87, 1.0, 1.0, 1.0, 0.8),
        ("h3", "h4", 0.2, 0.45, 0.42, 0.38, 0.0, 0.0, 1.0, 0.0),
    ]
    
    pairs_df = spark.createDataFrame(pairs_data, pairs_schema)
    
    hotels_schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False)
    ])
    
    hotels_data = [("h1", "Hotel One"), ("h2", "Hotel Two")]
    hotels_df = spark.createDataFrame(hotels_data, hotels_schema)
    
    # Run pipeline
    results = orchestrator.run_batch(hotels_df, pairs_df)
    
    # Verify results
    assert results['status'] == 'SUCCESS'
    assert results['scored_pairs'] is not None
    assert results['clusters'] is not None
    assert results['metadata'] is not None
    
    logger.info("Test passed: Orchestrator pipeline executed successfully")
    
    DependencyContainer.reset()

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
