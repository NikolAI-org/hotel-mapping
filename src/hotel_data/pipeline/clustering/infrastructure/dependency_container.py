# infrastructure/dependency_container.py

from typing import Dict, Any, Optional
from pyspark.sql import SparkSession

from hotel_data.config.scoring_config import HotelClusteringConfig
from hotel_data.pipeline.clustering.core.clustering_interfaces import Logger, ScoringStrategy
from hotel_data.pipeline.clustering.infrastructure.logging.logger import ConsoleLogger
from hotel_data.pipeline.clustering.services.scoring_service import ThresholdScoringStrategy

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
        return ThresholdScoringStrategy(
            config=config.scoring,  # Extract ScoringConfig
            logger=logger
        )
    
    # @staticmethod
    # def get_conflict_detector():
    #     """Factory method for conflict detector"""
    #     config = DependencyContainer.get_config()
    #     logger = DependencyContainer.get_logger()
        
    #     from hotel_data.pipeline.clustering.services.conflict_service import TransitiveConflictDetector
        
    #     return TransitiveConflictDetector(
    #         logger=logger,
    #         confidence_threshold=config.scoring.thresholds.get('medium_confidence', 0.70),
    #         conflict_tolerance=0.15,  # 15% score drop tolerance
    #         max_chain_length=3,  # Prevent chains longer than 3
    #         resolution_strategy="remove_weakest"  # Default strategy
    #     )
    
    @staticmethod
    def get_clusterer():
        """Factory method for clusterer"""
        config = DependencyContainer.get_config()
        logger = DependencyContainer.get_logger()
        spark = DependencyContainer.get_spark()
        
        
        DEFAULT_ALGORITHM = "unionfind"

        raw_algorithm = getattr(config.clustering, "algorithm", None)

        algorithm = (
            raw_algorithm.strip().casefold()
            if isinstance(raw_algorithm, str) and raw_algorithm.strip()
            else DEFAULT_ALGORITHM
        )
        if algorithm == 'labelpropagation':
            from hotel_data.pipeline.clustering.services.clustering_service import FixedLabelPropagationClustering
            return FixedLabelPropagationClustering(
                    spark=spark,
                    logger=logger
                )
        else:
            from hotel_data.pipeline.clustering.services.clustering_service_driversideclustering import DriverSideUnionFindClustering
            return DriverSideUnionFindClustering(
                    spark=spark,
                    logger=logger
                )
        
    
    @staticmethod
    def get_metadata_recorder():
        """Factory method for metadata recorder"""
        spark = DependencyContainer.get_spark()
        logger = DependencyContainer.get_logger()
        writer = DependencyContainer.get_output_writer("all")
        
        from hotel_data.pipeline.clustering.services.metadata_recorder import ComprehensiveMetadataRecorder
        
        return ComprehensiveMetadataRecorder(
            writer=writer,
            spark=spark,
            logger=logger
        )

    
    @staticmethod
    def get_orchestrator():
        """Factory for HotelClusteringOrchestrator"""
        logger = DependencyContainer.get_logger()
        
        from hotel_data.pipeline.clustering.services.orchestrator import HotelClusteringOrchestrator
        
        orchestrator = HotelClusteringOrchestrator(
            scorer=DependencyContainer.get_scoring_service(),
            # conflict_detector=DependencyContainer.get_conflict_detector(),
            clusterer=DependencyContainer.get_clusterer(),
            metadata_recorder=DependencyContainer.get_metadata_recorder(),
            writer=DependencyContainer.get_output_writer("all"),  # ← Unified!
            logger=logger
        )
        
        logger.info(
            "HotelClusteringOrchestrator created",
            writer_type="all"
        )
        
        return orchestrator
    
    @staticmethod
    def get_delta_table_manager():
        """Factory for DeltaTableManager"""
        
        spark = DependencyContainer.get_spark()
        config = DependencyContainer.get_config()
        logger = DependencyContainer.get_logger()
        
        from hotel_data.delta.delta_table_manager import DeltaTableManager
        
        # Access config attributes, not dict keys
        catalog_name = getattr(config.delta_lake, 'catalog_name', 'spark_catalog')
        schema_name = getattr(config.delta_lake, 'schema_name', 'default')
        base_path = getattr(config.delta_lake, 'base_path', 'delta_lake/')
        
        logger.debug(
            "Creating DeltaTableManager",
            catalog=catalog_name,
            schema=schema_name,
            base_path=base_path
        )
        
        return DeltaTableManager(
            spark=spark,
            catalog_name=catalog_name,
            schema_name=schema_name,
            base_path=base_path
        )
    
    @staticmethod
    def get_output_writer(writer_type: str = "all", level: str = "delta"):
        """
        Factory for output writers with composition
        
        Args:
            writer_type: "delta", "debug", "cluster", or "all"
            level: "delta" (default) or "direct" (use manager directly)
        
        Returns:
            TableIO implementation (possibly decorated)
        
        Examples:
            get_output_writer("all") → DeltaTableIO + DebugIO + ClusterIO
            get_output_writer("debug") → DeltaTableIO + DebugIO
            get_output_writer("delta") → DeltaTableIO only
        """
        logger = DependencyContainer.get_logger()
        
        # Get DeltaTableManager
        delta_manager = DependencyContainer.get_delta_table_manager()
        
        # Create base adapter
        from hotel_data.infrastructure.adapter.delta_table_io import DeltaTableIO
        base_writer = DeltaTableIO(
            delta_manager=delta_manager,
            logger=logger,
            metadata_table="_write_metadata"
        )
        
        # Apply decorators based on type
        if writer_type == "delta":
            return base_writer
        
        elif writer_type == "debug":
            from hotel_data.infrastructure.decorator.debug_io import DebugIO
            return DebugIO(base_writer, logger)
        
        elif writer_type == "cluster":
            from hotel_data.infrastructure.decorator.cluster_io import ClusterIO
            return ClusterIO(base_writer, logger)
        
        elif writer_type == "all":
            from hotel_data.infrastructure.decorator.debug_io import DebugIO
            from hotel_data.infrastructure.decorator.cluster_io import ClusterIO
            
            # Chain: DeltaTableIO → DebugIO → ClusterIO
            debug_writer = DebugIO(base_writer, logger)
            cluster_writer = ClusterIO(debug_writer, logger)
            return cluster_writer
        
        else:
            logger.warning(f"Unknown writer_type: {writer_type}, using delta")
            return base_writer
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
