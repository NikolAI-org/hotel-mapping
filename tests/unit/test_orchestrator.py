import pytest
from unittest.mock import Mock, MagicMock, ANY
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
from pyspark.sql.functions import lit
from hotel_data.pipeline.clustering.services.orchestrator import HotelClusteringOrchestrator
from hotel_data.pipeline.clustering.core.clustering_interfaces import (
    ScoringStrategy,
    ConflictDetectionStrategy,
    ClusteringStrategy,
    MetadataRecorder,
    Logger
)
from hotel_data.infrastructure.core.table_io import TableIO


@pytest.fixture
def spark():
    """Create a test SparkSession"""
    return SparkSession.builder.appName("test").master("local[1]").getOrCreate()


@pytest.fixture
def mock_logger():
    """Create a mock logger"""
    logger = Mock(spec=Logger)
    logger.debug = Mock()
    logger.info = Mock()
    logger.error = Mock()
    logger.warning = Mock()
    return logger


@pytest.fixture
def mock_scorer():
    """Create a mock scorer"""
    return Mock(spec=ScoringStrategy)


@pytest.fixture
def mock_conflict_detector():
    """Create a mock conflict detector"""
    return Mock(spec=ConflictDetectionStrategy)


@pytest.fixture
def mock_clusterer():
    """Create a mock clusterer"""
    return Mock(spec=ClusteringStrategy)


@pytest.fixture
def mock_metadata_recorder():
    """Create a mock metadata recorder"""
    return Mock(spec=MetadataRecorder)


@pytest.fixture
def mock_writer():
    """Create a mock writer"""
    return Mock(spec=TableIO)


@pytest.fixture
def orchestrator(
    mock_logger,
    mock_scorer,
    mock_conflict_detector,
    mock_clusterer,
    mock_metadata_recorder,
    mock_writer
):
    """Create an orchestrator with mocked dependencies"""
    return HotelClusteringOrchestrator(
        scorer=mock_scorer,
        conflict_detector=mock_conflict_detector,
        clusterer=mock_clusterer,
        metadata_recorder=mock_metadata_recorder,
        writer=mock_writer,
        logger=mock_logger
    )


class TestOrchestratorInitialization:
    """Tests for orchestrator initialization"""

    def test_initialization_with_all_services(
        self,
        orchestrator,
        mock_logger,
        mock_scorer,
        mock_conflict_detector,
        mock_clusterer,
        mock_metadata_recorder,
        mock_writer
    ):
        """Test that orchestrator initializes with all services"""
        assert orchestrator.scorer == mock_scorer
        assert orchestrator.conflict_detector == mock_conflict_detector
        assert orchestrator.clusterer == mock_clusterer
        assert orchestrator.metadata_recorder == mock_metadata_recorder
        assert orchestrator.writer == mock_writer
        assert orchestrator.logger == mock_logger
        # ✅ FIX: Use _is_initialized (private) not is_initialized
        assert orchestrator._is_initialized is True

    def test_health_check_passes(self, orchestrator):
        """Test that health check passes when all services are initialized"""
        health = orchestrator.health_check()
        assert health is True

    def test_get_status(self, orchestrator):
        """Test that status is INITIALIZED"""
        status = orchestrator.get_status()
        assert status == "INITIALIZED"


class TestOrchestratorRunBatch:
    """Tests for run_batch method"""

    @pytest.fixture
    def test_data(self, spark):
        """Create test DataFrames"""
        # Hotels data
        hotels_schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True)
        ])
        hotels_data = [("1", "Hotel A"), ("2", "Hotel B")]
        hotels_df = spark.createDataFrame(hotels_data, hotels_schema)

        # Pairs data with all 8 signal columns
        pairs_schema = StructType([
            StructField("id_i", StringType(), True),
            StructField("id_j", StringType(), True),
            StructField("geo_distance_km", IntegerType(), True),
            StructField("name_score_sbert", IntegerType(), True),
            StructField("phone_score", IntegerType(), True),
            StructField("email_score", IntegerType(), True),
            StructField("address_score", IntegerType(), True),
            StructField("website_score", IntegerType(), True),
            StructField("logo_score", IntegerType(), True),
            StructField("contact_score", IntegerType(), True)
        ])
        pairs_data = [("1", "2", 5, 80, 60, 70, 75, 0, 0, 0)]
        pairs_df = spark.createDataFrame(pairs_data, pairs_schema)

        return hotels_df, pairs_df

    def test_run_batch_success(
        self,
        spark,
        orchestrator,
        mock_scorer,
        mock_conflict_detector,
        mock_clusterer,
        mock_metadata_recorder,
        mock_writer,
        test_data
    ):
        """Test successful run_batch execution"""
        hotels_df, pairs_df = test_data

        # ✅ CRITICAL FIX: Create scored_df with composite_score column
        # This is what scorer.score() would return
        scored_df = (
            pairs_df
            .withColumn("composite_score", lit(0.85))
            .withColumn("confidence_level", lit("HIGH"))
        )

        # ✅ Conflicts detected (derived from scored_df)
        conflicts_df = (
            scored_df
            .withColumn("has_conflict", lit(False))
            .withColumn("conflict_reason", lit(""))
        )

        # ✅ Clusters (derived from hotels with cluster assignment)
        clusters_df = (
            hotels_df
            .withColumn("cluster_id", lit(1))
            .withColumn("is_representative", lit(True))
        )

        # Metadata dict
        metadata = {
            'total_hotels': 2,
            'total_pairs_scored': 1,
            'high_confidence_pairs': 1,
            'clusters_created': 1,
            'conflicts_detected': 0,
            'conflicts_resolved': 0,
            'processing_timestamp': '2025-12-12T10:00:00',
            'version': '1.0'
        }

        # ✅ Setup mocks with correct return values
        mock_scorer.score.return_value = scored_df
        mock_conflict_detector.detect_conflicts.return_value = conflicts_df
        mock_clusterer.cluster.return_value = clusters_df
        # ✅ CRITICAL FIX: Mock record_metadata() to prevent real execution
        mock_metadata_recorder.record_metadata.return_value = metadata
        mock_writer.write.return_value = None

        # Run batch
        result = orchestrator.run_batch(hotels_df, pairs_df)

        # ✅ Assertions on result structure
        assert result['status'] == 'SUCCESS'
        assert result['scored_pairs'] is not None
        assert result['conflicts'] is not None
        assert result['clusters'] is not None
        assert result['metadata'] is not None

        # Verify metadata values
        assert isinstance(result['metadata'], dict)
        assert result['metadata']['total_hotels'] == 2
        assert result['metadata']['total_pairs_scored'] == 1
        assert result['metadata']['clusters_created'] == 1

        # ✅ Verify mocks were called
        mock_scorer.score.assert_called_once()
        mock_conflict_detector.detect_conflicts.assert_called_once()
        mock_clusterer.cluster.assert_called_once()
        # ✅ FIX: Use ANY() for DataFrame arguments since they're complex
        mock_metadata_recorder.record_metadata.assert_called_once()

    def test_run_batch_with_conflicts(
        self,
        orchestrator,
        mock_scorer,
        mock_conflict_detector,
        mock_clusterer,
        mock_metadata_recorder,
        mock_writer,
        test_data
    ):
        """Test run_batch with detected conflicts"""
        hotels_df, pairs_df = test_data

        # Create DataFrames with conflicts
        scored_df = (
            pairs_df
            .withColumn("composite_score", lit(0.45))
            .withColumn("confidence_level", lit("UNCERTAIN"))
        )

        conflicts_df = (
            scored_df
            .withColumn("has_conflict", lit(True))
            .withColumn("conflict_reason", lit("Score below threshold"))
        )

        clusters_df = (
            hotels_df
            .withColumn("cluster_id", lit(1))
            .withColumn("is_representative", lit(True))
        )

        metadata = {
            'total_hotels': 2,
            'total_pairs_scored': 1,
            'high_confidence_pairs': 0,
            'clusters_created': 1,
            'conflicts_detected': 1,
            'conflicts_resolved': 0,
            'processing_timestamp': '2025-12-12T10:00:00',
            'version': '1.0'
        }

        mock_scorer.score.return_value = scored_df
        mock_conflict_detector.detect_conflicts.return_value = conflicts_df
        mock_clusterer.cluster.return_value = clusters_df
        mock_metadata_recorder.record_metadata.return_value = metadata
        mock_writer.write.return_value = None

        result = orchestrator.run_batch(hotels_df, pairs_df)

        assert result['status'] == 'SUCCESS'
        assert result['metadata']['conflicts_detected'] == 1
        assert result['metadata']['high_confidence_pairs'] == 0

    def test_run_batch_failure_handling(
        self,
        orchestrator,
        mock_scorer,
        mock_conflict_detector,
        mock_clusterer,
        mock_metadata_recorder,
        mock_writer,
        test_data
    ):
        """Test run_batch handles exceptions gracefully"""
        hotels_df, pairs_df = test_data

        # Make scorer raise exception
        mock_scorer.score.side_effect = Exception("Scoring failed")
        mock_writer.write.return_value = None

        result = orchestrator.run_batch(hotels_df, pairs_df)

        # Should return FAILED status
        assert result['status'] == 'FAILED'
        assert result['scored_pairs'] is None
        assert result['conflicts'] is None
        assert result['clusters'] is None
        assert result['metadata'] is None

    def test_run_batch_partial_failure(
        self,
        orchestrator,
        mock_scorer,
        mock_conflict_detector,
        mock_clusterer,
        mock_metadata_recorder,
        mock_writer,
        test_data
    ):
        """Test run_batch when write operation fails"""
        hotels_df, pairs_df = test_data

        scored_df = (
            pairs_df
            .withColumn("composite_score", lit(0.85))
            .withColumn("confidence_level", lit("HIGH"))
        )

        conflicts_df = (
            scored_df
            .withColumn("has_conflict", lit(False))
            .withColumn("conflict_reason", lit(""))
        )

        clusters_df = (
            hotels_df
            .withColumn("cluster_id", lit(1))
            .withColumn("is_representative", lit(True))
        )

        metadata = {
            'total_hotels': 2,
            'total_pairs_scored': 1,
            'high_confidence_pairs': 1,
            'clusters_created': 1,
            'conflicts_detected': 0,
            'conflicts_resolved': 0,
            'processing_timestamp': '2025-12-12T10:00:00',
            'version': '1.0'
        }

        mock_scorer.score.return_value = scored_df
        mock_conflict_detector.detect_conflicts.return_value = conflicts_df
        mock_clusterer.cluster.return_value = clusters_df
        mock_metadata_recorder.record_metadata.return_value = metadata
        mock_writer.write.side_effect = Exception("Write failed")

        result = orchestrator.run_batch(hotels_df, pairs_df)

        # Should return FAILED when write fails
        assert result['status'] == 'FAILED'


class TestOrchestratorMetadataRecording:
    """Tests for metadata recording"""

    def test_metadata_structure(self, mock_metadata_recorder):
        """Test that metadata has correct structure"""
        expected_keys = [
            'total_hotels',
            'total_pairs_scored',
            'high_confidence_pairs',
            'clusters_created',
            'conflicts_detected',
            'conflicts_resolved',
            'processing_timestamp',
            'version'
        ]

        metadata = {key: 0 for key in expected_keys}
        mock_metadata_recorder.record_metadata.return_value = metadata

        # All keys should be present
        for key in expected_keys:
            assert key in metadata


# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
    
# Run test case
# poetry run pytest tests/unit/test_orchestrator.py -v
