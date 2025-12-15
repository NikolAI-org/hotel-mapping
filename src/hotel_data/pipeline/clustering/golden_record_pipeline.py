from pathlib import Path
from pyspark.sql import SparkSession
from hotel_data.config.config_loader import ConfigLoader
from hotel_data.config.paths import BASE_DELTA_PATH, CATALOG_NAME, DERBY_HOME, SCHEMA_NAME, WAREHOUSE_DIR
from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.pipeline.clustering.infrastructure.dependency_container import DependencyContainer
from hotel_data.pipeline.clustering.infrastructure.logging.logger import ConsoleLogger
from hotel_data.pipeline.clustering.services.golden_record_transformer import GoldenRecordTransformer


def build_golden_records():
    """Standalone Golden Record pipeline"""
    
    print("=" * 70)
    print("GOLDEN RECORD TRANSFORMATION PIPELINE")
    print("=" * 70)
    
    # Setup
    config = ConfigLoader.load_from_yaml('config.yaml')
    Path(DERBY_HOME).mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName("HotelsPipelineWrite")
        .config("spark.jars.packages", ",".join([
            "io.delta:delta-spark_2.13:4.0.0",
            "org.apache.hadoop:hadoop-aws:3.4.1",
        ]))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        # ---- S3/MinIO config ----
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://192.168.1.4:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # ---- Hive metastore (Derby) for local prod-like testing ----
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:{DERBY_HOME}/metastore_db;create=true",
        )
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionDriverName",
            "org.apache.derby.jdbc.EmbeddedDriver",
        )
        .config("spark.hadoop.datanucleus.autoCreateSchema", "true")
        .config("spark.hadoop.datanucleus.fixedDatastore", "true")
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    logger = ConsoleLogger(name="GoldenRecord")
    
    # Read Delta tables
    logger.info("[PHASE 1] Reading source tables...")
    
    
    manager = DeltaTableManager(
        spark=spark,
        catalog_name=CATALOG_NAME,
        schema_name=SCHEMA_NAME,
        base_path=BASE_DELTA_PATH
    )
    
    hotels_df = manager.read_table("hotels")
    clusters_df = manager.read_table("06_final_clusters")  # or read from path
    if clusters_df is not None:
        logger.info("Cluster DF schema")
    logger.info(f"  Hotels: {hotels_df.count()}")
    logger.info(f"  Clusters: {clusters_df.count()}")
    
    # Build Golden Records
    logger.info("[PHASE 2] Building Golden Records...")
    
    config = ConfigLoader.load_from_yaml('config.yaml')
    DependencyContainer.configure(config, spark)
    writer = DependencyContainer.get_output_writer("all")
    transformer = GoldenRecordTransformer(logger=logger, spark=spark, writer=writer)
    golden_records, stats = transformer.build_from_delta(
        hotels_df=hotels_df,
        clusters_df=clusters_df
    )
    
    # Validate
    logger.info("[PHASE 3] Validating output...")
    
    if stats['validation_passed']:
        logger.info(f"✓ Validation PASSED")
        logger.info(f"  Total records: {stats['total_records']}")
        logger.info(f"  Avg cluster size: {stats['avg_cluster_size']:.2f}")
        logger.info(f"  Max cluster size: {stats['max_cluster_size']}")
        logger.info(f"  Avg providers per cluster: {stats['providers_distribution']:.2f}")
    else:
        logger.warning(f"✗ Validation FAILED")
        for issue in stats['issues']:
            logger.warning(f"  - {issue}")
        return
    
    # Write to Delta
    logger.info("[PHASE 4] Writing to Delta Lake...")

    
    golden_records_location = f"{BASE_DELTA_PATH}/09_golden_records"
    transformer.write_to_delta(
        golden_records,
        golden_records_location,
        mode="overwrite"
    )
    
    # Register table
    # transformer.create_managed_table(
    #     spark=spark,
    #     location=golden_records_location,
    #     table_name="hotel_golden_records"
    # )
    
    # Summary
    logger.info("[PHASE 5] Pipeline completed successfully!")
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"✓ Golden Records: {stats['total_records']}")
    print(f"✓ Validation: {'PASSED' if stats['validation_passed'] else 'FAILED'}")
    print(f"✓ Location: {golden_records_location}")
    print(f"✓ Table: hotel_golden_records")
    print("=" * 70)


if __name__ == "__main__":
    build_golden_records()
