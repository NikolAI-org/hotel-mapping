from __future__ import annotations

import os
from typing import Any, Optional, cast

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.errors import AnalysisException


class DeltaTableManager:
    """
    Delta Table Manager for a production-like setup.

    - Tables are registered in a catalog/schema (Hive-style in OSS Spark).
    - Data is always stored in Delta format on object storage (MinIO/S3).
    - Code can use table *names* in SQL, but everything is still backed by paths.
    """

    def __init__(
        self,
        spark: SparkSession,
        catalog_name: str,
        schema_name: str,
        base_path: str,
    ) -> None:
        self.spark = spark
        self.catalog_name = catalog_name  # e.g. "spark_catalog" in UC world, logical in OSS
        self.schema_name = schema_name    # e.g. "bronze"
        self.base_path = base_path.rstrip("/")  # e.g. "s3a://hotel-lake/delta"
        self.use_catalog = True  # we will probe UC, then fall back to OSS

        self._ensure_catalog_and_schema()

    # ----------------------------------------------------------------------
    # Internal helpers
    # ----------------------------------------------------------------------

    def _ensure_catalog_and_schema(self) -> None:
        """
        Ensure catalog+schema exist.

        - If Unity Catalog is available, create catalog+schema.
        - Otherwise, create a Hive/DB-style database with name = schema_name.

        In local OSS Spark, this will typically land in the Hive metastore
        configured via DERBY_HOME / MySQL / Postgres.
        """
        print(f"🧭 Ensuring catalog '{self.catalog_name}' and schema '{self.schema_name}' exist...")

        try:
            # Try Unity Catalog first (Databricks-like behavior)
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog_name}")
            self.spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS {self.catalog_name}.{self.schema_name}"
            )
            self.use_catalog = True
            print(f"✅ Using Unity Catalog: {self.catalog_name}.{self.schema_name}")
        except Exception as e:
            msg = str(e)
            if "PARSE_SYNTAX_ERROR" in msg or "REQUIRES_SINGLE_PART_NAMESPACE" in msg:
                print("⚙️ Unity Catalog not available. Falling back to OSS Spark (Hive/Database mode).")
                try:
                    # Classic Hive DB (single catalog: spark_catalog)
                    self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.schema_name}")
                    self.use_catalog = False
                    print(f"✅ OSS schema/database '{self.schema_name}' ensured.")
                except Exception as hive_e:
                    # In a real prod setup this would be a hard failure with alerts
                    print(f"⚠️ Could not persist schema '{self.schema_name}': {hive_e}")
                    print("   -> Check Hive metastore configuration.")
                    self.use_catalog = False
            else:
                # Unexpected failure -> don't silently swallow
                raise

    def _namespace(self) -> str:
        """
        Namespace used in SQL (for SHOW TABLES, CREATE TABLE, etc.).

        - With catalog:   catalog.schema
        - Without:        schema
        """
        if self.use_catalog:
            return f"{self.catalog_name}.{self.schema_name}"
        return self.schema_name

    def _get_table_identifier(self, table_name: str) -> str:
        """
        Fully qualified table name.

        - With catalog:   catalog.schema.table
        - Without:        schema.table
        """
        if self.use_catalog:
            return f"{self.catalog_name}.{self.schema_name}.{table_name}"
        return f"{self.schema_name}.{table_name}"

    def _get_table_path(self, table_name: str) -> str:
        """
        Physical storage location for this Delta table.
        Example: s3a://bucket/delta/bronze/hotels
        """
        return os.path.join(self.base_path, self.schema_name, table_name)


    def _table_exists(self, table_name: str) -> bool:
        path = self._get_table_path(table_name)
        try:
            return DeltaTable.isDeltaTable(self.spark, path)
        except Exception as e:
            print(f"⚠️ _table_exists: Delta check failed for {path}: {e}")
            return False


    # ----------------------------------------------------------------------
    # Public table operations
    # ----------------------------------------------------------------------

    def create_table(
        self,
        table_name: str,
        df: Optional[DataFrame] = None,
        comment: str = "",
    ) -> None:
        """
        Create a Delta table if it doesn't exist.

        - Writes an initial Delta log at the physical path (schema from df).
        - Registers the table in the catalog pointing to that path.
        """
        fq_name = self._get_table_identifier(table_name)
        path = self._get_table_path(table_name)

        if self._table_exists(table_name):
            print(f"✅ Table '{fq_name}' already exists.")
            return

        print(f"📦 Creating Delta table '{fq_name}' at {path}")

        if df is not None and df.columns:
            df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(path)
        else:
            # In practice, you should always pass a schema'd empty DF here.
            df = df if df is not None else self.spark.createDataFrame([], "id STRING")
            df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(path)

        # Register in catalog
        try:
            self.spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {fq_name}
                USING DELTA
                LOCATION '{path}'
                COMMENT '{comment}'
                """
            )
            print(f"✅ Table '{fq_name}' registered in catalog.")
        except Exception as e:
            # In real prod, treat this seriously; here we keep it non-fatal for local.
            print(
                f"⚠️ Could not register '{fq_name}' in catalog: {e}\n"
                f"   -> Data is still available at {path}."
            )

    def read_table(self, table_name: str) -> DataFrame:
        fq_name = self._get_table_identifier(table_name)
        path = self._get_table_path(table_name)

        # Try catalog (prod-style)
        try:
            print(f"📖 Reading '{fq_name}' from catalog...")
            return self.spark.table(fq_name)
        except Exception as e:
            print(f"⚠️ Catalog read failed for '{fq_name}': {e}")
            print(f"   -> Falling back to Delta path: {path}")

        # Fallback: direct path read
        return self.spark.read.format("delta").load(path)


    def write_table(
        self,
        table_name: str,
        df: DataFrame,
        mode: str = "append",
        merge_schema: str = "true",
        overwrite_schema: str = "true",
    ) -> None:
        """
        Write data into the Delta table.

        - Ensures table exists.
        - Writes to the physical Delta path.
        """
        fq_name = self._get_table_identifier(table_name)
        path = self._get_table_path(table_name)

        if not self._table_exists(table_name):
            self.create_table(table_name, df=df)

        print(
            f"📝 Writing data to '{fq_name}' "
            f"(path={path}, mode={mode}, mergeSchema={merge_schema}, overwriteSchema={overwrite_schema})..."
        )

        df.write.format("delta") \
            .mode(mode) \
            .option("mergeSchema", merge_schema) \
            .option("overwriteSchema", overwrite_schema) \
            .save(path)

        print(f"✅ Data written to '{fq_name}' successfully.")

    def merge_table(self, table_name: str, df: DataFrame, key_columns: list[str]) -> None:
        """
        Merge new data into Delta table using the given key columns.
        """
        fq_name = self._get_table_identifier(table_name)
        path = self._get_table_path(table_name)

        if not self._table_exists(table_name):
            self.create_table(table_name, df=df)

        delta_table = DeltaTable.forPath(self.spark, path)
        condition = " AND ".join([f"target.{k} = source.{k}" for k in key_columns])

        print(f"🔄 Merging into '{fq_name}' on keys {key_columns}...")
        delta_table.alias("target").merge(
            df.alias("source"), condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        print(f"✅ Merge completed for '{fq_name}'.")

    def delete_data(self, table_name: str, condition: str) -> None:
        """
        Delete records from a Delta table based on a condition.
        """
        fq_name = self._get_table_identifier(table_name)
        path = self._get_table_path(table_name)

        if not self._table_exists(table_name):
            print(f"⚠️ Table '{fq_name}' does not exist. Skipping delete.")
            return

        print(f"🧹 Deleting from '{fq_name}' where {condition} ...")
        delta_table = DeltaTable.forPath(self.spark, path)
        delta_table.delete(condition)
        print(f"✅ Data deleted from '{fq_name}'.")

    def drop_table(self, table_name: str, delete_data: bool = True) -> None:
        """
        Drop table metadata (catalog) and optionally underlying data.
        """
        fq_name = self._get_table_identifier(table_name)
        path = self._get_table_path(table_name)

        if not self._table_exists(table_name):
            print(f"⚠️ Table '{fq_name}' does not exist. Nothing to drop.")
            return

        print(f"💣 Dropping table '{fq_name}' (delete_data={delete_data}) ...")

        try:
            self.spark.sql(f"DROP TABLE IF EXISTS {fq_name}")
            print(f"✅ Table '{fq_name}' dropped from catalog.")
        except Exception as e:
            print(f"⚠️ Could not drop from catalog: {e}")

        if not delete_data:
            return

        # Delete physical data via Hadoop FS
        jsc = getattr(self.spark, "_jsc", None)
        jvm = getattr(self.spark, "_jvm", None)
        if jsc is None or jvm is None:
            print("⚠️ JVM/JSC not available; skipping data deletion.")
            return

        jsc = cast(Any, jsc)
        jvm = cast(Any, jvm)

        try:
            hadoop_conf = jsc.hadoopConfiguration()
            uri = jvm.java.net.URI(path)
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
            hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
            deleted = fs.delete(hadoop_path, True)
            print(f"🗑️ Deleted underlying data at {path}: {deleted}")
        except Exception as e:
            print(f"⚠️ Failed to delete data path {path}: {e}")

    # Convenience for debugging in local
    def list_tables(self) -> None:
        ns = self._namespace()
        try:
            print(f"📋 SHOW TABLES IN {ns}")
            self.spark.sql(f"SHOW TABLES IN {ns}").show(truncate=False)
        except Exception as e:
            print(f"⚠️ list_tables failed for {ns}: {e}")
