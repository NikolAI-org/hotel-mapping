from pyspark.sql import SparkSession, DataFrame
from delta.tables import DeltaTable
import os


class DeltaTableManager:
    """
    Delta Table Manager for both OSS Spark and Unity Catalog.
    Provides table creation, read, write, and merge capabilities.
    """

    def __init__(
        self, spark: SparkSession, catalog_name: str, schema_name: str, base_path: str
    ):
        self.spark = spark
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.base_path = base_path
        self.use_catalog = True  # Assume Unity Catalog initially

        self._ensure_catalog_and_schema()

    # ------------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------------

    def _ensure_catalog_and_schema(self):
        """Ensure catalog and schema exist, compatible with Unity Catalog and OSS Spark."""
        print(
            f"🧭 Ensuring catalog '{self.catalog_name}' and schema '{self.schema_name}' exist..."
        )

        try:
            # Try Unity Catalog first
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog_name}")
            self.spark.sql(
                f"CREATE SCHEMA IF NOT EXISTS {self.catalog_name}.{self.schema_name}"
            )
            self.use_catalog = True
            print(f"✅ Using Unity Catalog: {self.catalog_name}.{self.schema_name}")
        except Exception as e:
            msg = str(e)
            if "PARSE_SYNTAX_ERROR" in msg or "REQUIRES_SINGLE_PART_NAMESPACE" in msg:
                print(
                    f"⚙️ Unity Catalog not available. Falling back to OSS Spark (Hive/Database mode)."
                )

                # Fallback to Hive/DB
                try:
                    self.spark.sql("CREATE DATABASE IF NOT EXISTS default")
                    self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.schema_name}")
                    self.use_catalog = False
                    print(f"✅ OSS schema '{self.schema_name}' ensured.")
                except Exception as hive_e:
                    print(f"⚠️ Could not persist schema '{self.schema_name}': {hive_e}")
                    print(
                        "   -> Consider enabling Hive support for persistent catalogs."
                    )
                    self.use_catalog = False
            else:
                raise e

    def _namespace(self) -> str:
        """Return the namespace for SQL queries."""
        return (
            f"{self.catalog_name}.{self.schema_name}"
            if self.use_catalog
            else self.schema_name
        )

    def _get_table_identifier(self, table_name: str) -> str:
        """Fully qualified table name."""
        return (
            f"{self.catalog_name}.{self.schema_name}.{table_name}"
            if self.use_catalog
            else f"{self.schema_name}.{table_name}"
        )

    def _get_table_path(self, table_name: str) -> str:
        """Physical path to store Delta table."""
        return os.path.join(self.base_path, self.schema_name, table_name)

    def _table_exists(self, table_name: str) -> bool:
        """Check if table exists in the current namespace."""
        df = self.spark.sql(f"SHOW TABLES IN {self._namespace()}")
        return any(row.tableName == table_name for row in df.collect())

    # ------------------------------------------------------------------------
    # Table operations
    # ------------------------------------------------------------------------

    def create_table(self, table_name: str, df: DataFrame = None, comment: str = ""):
        """
        Create Delta table if it doesn't exist.
        If df is provided, writes initial data; otherwise creates empty table.
        """
        fq_name = self._get_table_identifier(table_name)
        path = self._get_table_path(table_name)

        if self._table_exists(table_name):
            print(f"✅ Table '{fq_name}' already exists.")
            return
        print(f"Writing to path {path}")
        if df is not None and df.columns:
            df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(path)
            print(f"📦 Written initial data to {path}")
        else:
            # Write empty DataFrame with schema to avoid empty schema errors
            df = (
                df if df is not None else self.spark.createDataFrame([], "id STRING")
            )  # default column
            df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(path)
            print(f"📦 Created empty table at {path}")

        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {fq_name}
            USING DELTA
            LOCATION '{path}'
            COMMENT '{comment}'
        """
        )
        print(f"✅ Table '{fq_name}' created at {path}")

    def read_table(self, table_name: str) -> DataFrame:
        """Read Delta table into a DataFrame."""
        path = self._get_table_path(table_name)
        print(f"------- Reading from path: {path}")

        df = self.spark.read.format("delta").load(path)
        print(f"------- Rows: {df.count()}")
        return df

    def write_table(self, table_name: str, df: DataFrame, mode: str = "append"):
        """
        Write data into Delta table.
        Supports append/overwrite with schema evolution.
        """
        fq_name = self._get_table_identifier(table_name)
        path = self._get_table_path(table_name)

        if not self._table_exists(table_name):
            self.create_table(table_name, df=df)

        print(
            f"📝 Writing data to '{fq_name}' Path: {path} (mode={mode}, mergeSchema=true)..."
        )
        df.write.format("delta").mode(mode).option("mergeSchema", "true").option("overwriteSchema", "true").save(path)
        # df.write.format("delta").mode(mode).option("mergeSchema", "true").saveAsTable(fq_name)
        print(f"✅ Data written to '{fq_name}' successfully.")

    def merge_table(self, table_name: str, df: DataFrame, key_columns: list[str]):
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

    def delete_data(self, table_name: str, condition: str):
        """
        Delete records from a Delta table based on a condition.
        Example:
            manager.delete_data("hotels", "providerId = 'Hobse'")
        """
        path = self._get_table_path(table_name)
        fq_name = self._get_table_identifier(table_name)

        if not self._table_exists(table_name):
            print(f"⚠️ Table '{fq_name}' does not exist. Skipping delete.")
            return

        print(f"🧹 Deleting data from '{fq_name}' where {condition} ...")
        delta_table = DeltaTable.forPath(self.spark, path)
        delta_table.delete(condition)
        print(f"✅ Data deleted from '{fq_name}' successfully.")

    def drop_table(self, table_name: str, delete_data: bool = True):
        """
        Drop the Delta table metadata and optionally delete the underlying data files.
        Works for both local and S3-backed Delta locations.
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

        if delete_data:
            try:
                # Get the Hadoop FileSystem object
                hadoop_conf = self.spark._jsc.hadoopConfiguration()
                fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                    self.spark._jvm.java.net.URI(path), hadoop_conf
                )
                # Delete the path recursively
                fs.delete(self.spark._jvm.org.apache.hadoop.fs.Path(path), True)
                print(f"🗑️ Deleted underlying data at {path}")
            except Exception as e:
                print(f"⚠️ Failed to delete data path {path}: {e}")

    # ------------------------------------------------------------------------
    # Utilities: List catalogs, schemas, and tables
    # ------------------------------------------------------------------------

    def list_catalogs(self):
        try:
            return self.spark.sql("SHOW CATALOGS")
        except Exception:
            return self.spark.sql("SELECT 'spark_catalog' AS catalog")

    def list_schemas(self, catalog: str = None):
        try:
            cat = (
                catalog
                if catalog
                else (self.catalog_name if self.use_catalog else None)
            )
            if cat:
                return self.spark.sql(f"SHOW SCHEMAS IN {cat}")
            return self.spark.sql("SHOW DATABASES")
        except Exception:
            return None

    def list_tables(self, schema: str = None):
        ns = schema if schema else self._namespace()
        try:
            print(f"SHOW TABLES IN spark_catalog.{ns}")
            return self.spark.sql(f"SHOW TABLES IN {ns}")
        except Exception as e:
            print(f"Error: {e}")
            return None
