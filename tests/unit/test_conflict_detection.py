# tests/unit/test_conflict_detection.py

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
from hotel_data.pipeline.clustering.services.conflict_service import TransitiveConflictDetector
from hotel_data.pipeline.clustering.infrastructure.logging.logger import ConsoleLogger

@pytest.fixture
def spark():
    return SparkSession.builder \
        .appName("test_conflict") \
        .master("local") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()

@pytest.fixture
def logger():
    return ConsoleLogger(name="TestConflict", level="INFO")

@pytest.fixture
def detector(logger):
    return TransitiveConflictDetector(
        logger=logger,
        confidence_threshold=0.70,
        conflict_tolerance=0.15,
        max_chain_length=3,
        resolution_strategy="remove_weakest"
    )

def test_conflict_detection_triplet(spark, detector):
    """Test detection of simple triplet conflict"""
    
    # Create pairs: A-B (0.9), B-C (0.85), but A-C (0.3)
    pairs_schema = StructType([
        StructField("id_i", StringType(), False),
        StructField("id_j", StringType(), False),
        StructField("composite_score", DoubleType(), False),
        StructField("confidence_level", StringType(), False),
        StructField("meets_exclusion_rules", BooleanType(), False)
    ])
    
    pairs_data = [
        ("A", "B", 0.9, "HIGH", True),
        ("B", "C", 0.85, "HIGH", True),
        ("A", "C", 0.3, "LOW", True),
    ]
    
    pairs_df = spark.createDataFrame(pairs_data, pairs_schema)
    
    # Detect conflicts
    result = detector.detect_conflicts(pairs_df)
    
    # Check: A-C pair should be marked as conflict
    conflict_count = result.filter(col("has_conflict")).count()
    
    assert conflict_count > 0, "Should detect conflict in A-C pair"
    
    print("✅ Conflict detection test passed")

def test_chain_detection(spark, detector):
    """Test detection of chains (A-B-C-D-E...)"""
    
    # Create chain: A-B-C-D (4 hotels)
    pairs_schema = StructType([
        StructField("id_i", StringType(), False),
        StructField("id_j", StringType(), False),
        StructField("composite_score", DoubleType(), False),
        StructField("confidence_level", StringType(), False),
        StructField("meets_exclusion_rules", BooleanType(), False)
    ])
    
    pairs_data = [
        ("A", "B", 0.8, "HIGH", True),
        ("B", "C", 0.8, "HIGH", True),
        ("C", "D", 0.8, "HIGH", True),
    ]
    
    pairs_df = spark.createDataFrame(pairs_data, pairs_schema)
    
    # Detect conflicts
    result = detector.detect_conflicts(pairs_df)
    
    # Check statistics
    stats = detector.get_statistics()
    
    assert stats['chains_found'] > 0, "Should detect chains"
    assert stats['longest_chain'] >= 3, "Chain should be at least 3 hotels"
    
    print(f"✅ Chain detection test passed: {stats}")

def test_conflict_resolution(spark, detector):
    """Test resolution of conflicts"""
    
    pairs_schema = StructType([
        StructField("id_i", StringType(), False),
        StructField("id_j", StringType(), False),
        StructField("composite_score", DoubleType(), False),
        StructField("confidence_level", StringType(), False),
        StructField("meets_exclusion_rules", BooleanType(), False),
        StructField("has_conflict", BooleanType(), False),
        StructField("conflict_reason", StringType(), False)
    ])
    
    pairs_data = [
        ("A", "B", 0.9, "HIGH", True, False, ""),
        ("B", "C", 0.85, "HIGH", True, False, ""),
        ("A", "C", 0.3, "LOW", True, True, "Transitive violation"),
    ]
    
    pairs_df = spark.createDataFrame(pairs_data, pairs_schema)
    
    # Resolve conflicts
    resolved = detector.resolve_conflicts(pairs_df)
    
    # Check: no more conflicts
    remaining_conflicts = resolved.filter(col("has_conflict")).count()
    
    assert remaining_conflicts == 0, "All conflicts should be resolved"
    
    print("✅ Conflict resolution test passed")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])


# Run tests
# poetry run pytest tests/unit/test_conflict_detection.py -v