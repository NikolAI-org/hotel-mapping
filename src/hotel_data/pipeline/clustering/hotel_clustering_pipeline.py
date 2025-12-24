# main.py - Updated Example

from pathlib import Path
from pyspark.sql import SparkSession
from hotel_data.config.config_loader import ConfigLoader
from hotel_data.config.paths import BASE_DELTA_PATH, CATALOG_NAME, DERBY_HOME, SCHEMA_NAME, WAREHOUSE_DIR
from hotel_data.delta.delta_table_manager import DeltaTableManager
from hotel_data.pipeline.clustering.infrastructure.dependency_container import DependencyContainer

def main():
    
    print("=" * 70)
    print("HOTEL CLUSTERING PIPELINE WITH ORCHESTRATOR")
    print("=" * 70)
    
    # ═══════════════════════════════════════════════════════════════════════
    # PHASE 1: STARTUP
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n[PHASE 1] Loading configuration...")
    config = ConfigLoader.load_from_yaml('config.yaml')
    print(f"✅ Config loaded")
    
    # ═══════════════════════════════════════════════════════════════════════
    # PHASE 2: INITIALIZATION
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n[PHASE 2] Initializing Spark and DependencyContainer...")
    Path(DERBY_HOME).mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName("HotelsPipelineWrite")
        .config("spark.jars.packages", ",".join([
            "io.delta:delta-spark_2.13:4.0.0",
            "org.apache.hadoop:hadoop-aws:3.4.1",
            "io.graphframes:graphframes-spark4_2.13:0.10.0"
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
        # .config("spark.executor.memory", "8g")
        # .config("spark.driver.memory", "4g")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setCheckpointDir("/tmp/checkpoints")
    
    # CRITICAL: Configure DependencyContainer
    DependencyContainer.configure(config, spark)
    print("✅ DependencyContainer configured")
    
    # ═══════════════════════════════════════════════════════════════════════
    # PHASE 3: GET ORCHESTRATOR
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n[PHASE 3] Getting Orchestrator...")
    logger = DependencyContainer.get_logger()
    orchestrator = DependencyContainer.get_orchestrator()
    print("✅ Orchestrator obtained")
    
    # Health check
    if not orchestrator.health_check():
        print("❌ Orchestrator health check failed!")
        return
    
    # ═══════════════════════════════════════════════════════════════════════
    # PHASE 4: LOAD DATA
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n[PHASE 4] Loading data...")
    # hotels_df = spark.read.parquet("s3a://bucket/hotels.parquet")
    # pairs_df = spark.read.parquet("s3a://bucket/pairs.parquet")
    
    manager = DeltaTableManager(
        spark=spark,
        catalog_name=CATALOG_NAME,
        schema_name=SCHEMA_NAME,
        base_path=BASE_DELTA_PATH,
    )
    
    hotels_df = manager.read_table("hotels")
    
    pairs_df = manager.read_table("hotel_pairs")
    
    logger.info(
        "Data loaded",
        hotels=hotels_df.count(),
        pairs=pairs_df.count()
    )
    print(f"✅ Hotels: {hotels_df.count()}, Pairs: {pairs_df.count()}")
    
    # ═══════════════════════════════════════════════════════════════════════
    # PHASE 5: RUN PIPELINE
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n[PHASE 5] Running clustering pipeline...")
    results = orchestrator.run_batch(hotels_df, pairs_df)

    
    if results['status'] == 'SUCCESS':
        print("✅ Pipeline completed successfully!")
        # print(f"\n   Clusters created: {results['clusters'].count()}")
        # print(f"   Conflicts found: {results['conflicts'].filter('has_conflict').count()}")
        # print(f"   Metadata: {results['metadata']}")
    else:
        print(f"❌ Pipeline failed: {results['error']}")
        return
    
    # ═══════════════════════════════════════════════════════════════════════
    # PHASE 6: DISPLAY RESULTS
    # ═══════════════════════════════════════════════════════════════════════
    
    print("\n[PHASE 6] Displaying results...")
    
    print("\n--- SCORED PAIRS SAMPLE ---")
    results['scored_pairs'].select(
        "id_i", "id_j", "name_i", "name_j",
        "match_status", "match_score",
        "is_matched"
    ).show(10)

    print("\n--- CLUSTERS SAMPLE ---")
    results['clusters'].select(
        "name", "id",
        "cluster_id"
    ).show(10)
    
    print("\n--- CLUSTER STATISTICS ---")
    print(f"Total unique clusters: {results['clusters'].select('cluster_id').distinct().count()}")
    print(f"Total pairs in clusters: {results['clusters'].count()}")
    # print(f"Metadata: {results['metadata']}")

    print("\n" + "=" * 70)
    print("✅ PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 70)


if __name__ == "__main__":
    main()
