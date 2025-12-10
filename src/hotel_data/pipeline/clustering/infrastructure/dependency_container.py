# infrastructure/dependency_container.py

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

from hotel_data.config.scoring_config import HotelClusteringConfig
from hotel_data.pipeline.clustering.core.clustering_interfaces import Logger, ScoringStrategy
from hotel_data.pipeline.clustering.infrastructure.logging.logger import ConsoleLogger
from hotel_data.pipeline.clustering.services.scoring_service import CompositeScoringStrategy

class DependencyContainer:
    """
    Dependency Injection Container (Service Locator Pattern)
    
    Manages all singletons and factory methods
    Must be configured once at startup
    """
    
    # Class variables (static storage) - shared across all instances
    _instances: Dict[str, Any] = {}
    _is_configured: bool = False
    
    # ========================================================================
    # CONFIGURATION (MUST CALL ONCE AT STARTUP)
    # ========================================================================
    
    @staticmethod
    def configure(
        config: HotelClusteringConfig,
        spark: SparkSession
    ) -> None:
        """
        Configure DependencyContainer (MUST CALL THIS ONCE AT STARTUP!)
        
        Args:
            config: HotelClusteringConfig loaded from YAML
            spark: SparkSession already created
        
        Raises:
            RuntimeError: If called multiple times
        
        Example:
            >>> config = ConfigLoader.load_from_yaml('config.yaml')
            >>> spark = SparkSession.builder.appName("hotel").master("local").getOrCreate()
            >>> DependencyContainer.configure(config, spark)  # ← CRITICAL!
        """
        if DependencyContainer._is_configured:
            raise RuntimeError(
                "DependencyContainer already configured! "
                "Call configure() only once at startup."
            )
        
        # 1. Store config (singleton)
        DependencyContainer._instances['config'] = config
        
        # 2. Store Spark session (singleton)
        DependencyContainer._instances['spark'] = spark
        
        # 3. Create and store logger (singleton)
        logger = ConsoleLogger(
            name="HotelClustering",
            level=config.logging.level
        )
        DependencyContainer._instances['logger'] = logger
        
        # Mark as configured
        DependencyContainer._is_configured = True
        
        # Log initialization
        logger.info(
            "DependencyContainer configured successfully",
            config_class=type(config).__name__,
            spark_app_name=spark.sparkContext.appName
        )
    
    # ========================================================================
    # GETTER METHODS - Retrieve singletons
    # ========================================================================
    
    @staticmethod
    def get_config() -> HotelClusteringConfig:
        """
        Get the HotelClusteringConfig singleton
        
        Returns:
            HotelClusteringConfig instance
        
        Raises:
            RuntimeError: If container not configured
        
        Example:
            >>> config = DependencyContainer.get_config()
            >>> print(config.scoring.weights)
        """
        if not DependencyContainer._is_configured:
            raise RuntimeError(
                "DependencyContainer not configured! "
                "Call configure(config, spark) first!"
            )
        
        return DependencyContainer._instances['config']
    
    @staticmethod
    def get_logger() -> Logger:
        """
        Get the Logger singleton
        
        Returns:
            Logger instance
        
        Raises:
            RuntimeError: If container not configured
        
        Example:
            >>> logger = DependencyContainer.get_logger()
            >>> logger.info("Processing started")
        """
        if not DependencyContainer._is_configured:
            raise RuntimeError(
                "DependencyContainer not configured! "
                "Call configure(config, spark) first!"
            )
        
        return DependencyContainer._instances['logger']
    
    @staticmethod
    def get_spark() -> SparkSession:
        """
        Get the SparkSession singleton
        
        Returns:
            SparkSession instance
        
        Raises:
            RuntimeError: If container not configured
        """
        if not DependencyContainer._is_configured:
            raise RuntimeError(
                "DependencyContainer not configured! "
                "Call configure(config, spark) first!"
            )
        
        return DependencyContainer._instances['spark']
    
    # ========================================================================
    # FACTORY METHODS - Create fully wired services
    # ========================================================================
    
    @staticmethod
    def get_scoring_service() -> ScoringStrategy:
        """
        Factory method: Creates ScoringStrategy with all dependencies injected
        
        Returns:
            CompositeScoringStrategy (fully initialized)
        
        Raises:
            RuntimeError: If container not configured
        
        Internally injects:
            - config.scoring (ScoringConfig)
            - logger (Logger)
        
        Example:
            >>> scorer = DependencyContainer.get_scoring_service()
            >>> scored_df = scorer.score(pairs_df)
        """
        config = DependencyContainer.get_config()
        logger = DependencyContainer.get_logger()
        
        # All dependencies injected via constructor
        return CompositeScoringStrategy(
            config=config.scoring,  # Extract ScoringConfig
            logger=logger
        )
    
    @staticmethod
    def get_conflict_detector():
        """Factory method for conflict detector"""
        config = DependencyContainer.get_config()
        logger = DependencyContainer.get_logger()
        
        # from hotel_data.pipeline.clustering.services import TransitiveConflictDetector
        from hotel_data.pipeline.clustering.services.stub_services import StubConflictDetector
        
        return StubConflictDetector(
            match_threshold=config.scoring.thresholds.get('medium_confidence', 0.70),
            logger=logger
        )
    
    @staticmethod
    def get_clusterer():
        """Factory method for clusterer"""
        config = DependencyContainer.get_config()
        logger = DependencyContainer.get_logger()
        
        # from services.clustering_service import UnionFindClusteringStrategy
        
        from hotel_data.pipeline.clustering.services.stub_services import StubClusterer
        
        return StubClusterer(
            config=config.clustering,
            logger=logger
        )
    
    @staticmethod
    def get_metadata_recorder():
        """Factory method for metadata recorder"""
        config = DependencyContainer.get_config()
        logger = DependencyContainer.get_logger()
        
        # from services.metadata_service import ComprehensiveMetadataRecorder
        from hotel_data.pipeline.clustering.services.stub_services import StubMetadataRecorder
        
        return StubMetadataRecorder(
            config=config,
            logger=logger
        )
    
    @staticmethod
    def get_cluster_writer():
        """Factory method for cluster writer"""
        config = DependencyContainer.get_config()
        logger = DependencyContainer.get_logger()
        spark = DependencyContainer.get_spark()
        
        
        # return DeltaLakeWriter(
        #     config=config.storage,
        #     spark=spark,
        #     logger=logger
        # )
        
        from hotel_data.pipeline.clustering.services.stub_services import StubClusterWriter
        return StubClusterWriter(
            logger=logger,
            config=config.storage
        )

    
    @staticmethod
    def get_orchestrator():
        """
        Factory method: Creates fully wired HotelClusteringOrchestrator
        
        Returns all service dependencies:
            - scorer (ScoringStrategy)
            - conflict_detector (ConflictDetectionStrategy)
            - clusterer (ClusteringStrategy)
            - metadata_recorder (MetadataRecorder)
            - writer (ClusterWriter)
            - logger (Logger)
        """
        logger = DependencyContainer.get_logger()
        
        from hotel_data.pipeline.clustering.services.orchestrator import HotelClusteringOrchestrator
        
        logger.info("Creating HotelClusteringOrchestrator...")
        
        orchestrator = HotelClusteringOrchestrator(
            scorer=DependencyContainer.get_scoring_service(),
            conflict_detector=DependencyContainer.get_conflict_detector(),
            clusterer=DependencyContainer.get_clusterer(),
            metadata_recorder=DependencyContainer.get_metadata_recorder(),
            writer=DependencyContainer.get_cluster_writer(),
            logger=logger
        )
        
        logger.info("Orchestrator created successfully")
        return orchestrator
    
    # ========================================================================
    # TESTING UTILITIES
    # ========================================================================
    
    @staticmethod
    def reset() -> None:
        """
        Reset container to unconfigured state
        
        ⚠️ WARNING: ONLY FOR TESTING - Never call in production!
        
        Example:
            >>> DependencyContainer.reset()  # in test teardown
        """
        DependencyContainer._instances.clear()
        DependencyContainer._is_configured = False
    
    @staticmethod
    def is_configured() -> bool:
        """Check if container is configured"""
        return DependencyContainer._is_configured
