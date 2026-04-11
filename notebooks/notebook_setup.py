"""
Common setup for hotel-mapping notebooks.

Run this at the top of every notebook with:
    %run ./notebook_setup.py
"""

import datetime
import os

import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    expr,
)

# ── Spark session ─────────────────────────────────────────────────────────────

os.environ.pop("HADOOP_CONF_DIR", None)
os.environ.pop("YARN_CONF_DIR", None)

spark = (
    SparkSession.builder.appName("Analyze Hotel Pairs")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
        "io.delta:delta-spark_2.13:4.0.0",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
    .config("spark.hadoop.fs.s3a.socket.send.buffer", "8192")
    .config("spark.hadoop.fs.s3a.socket.recv.buffer", "8192")
    .config("spark.hadoop.fs.s3a.attempts.maximum", "3")
    .config("spark.hadoop.fs.s3a.retry.limit", "3")
    .config("spark.hadoop.fs.s3a.retry.interval", "500")
    .config("spark.hadoop.fs.s3a.retry.throttle.limit", "3")
    .config("spark.hadoop.fs.s3a.retry.throttle.interval", "1000")
    .config("spark.hadoop.fs.s3a.connection.maximum", "50")
    .config("spark.hadoop.fs.s3a.threads.max", "10")
    .config("spark.hadoop.fs.s3a.threads.core", "5")
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60")
    .config("spark.hadoop.fs.s3a.max.total.tasks", "5")
    .config("spark.hadoop.fs.s3a.readahead.range", "65536")
    .config("spark.hadoop.fs.s3a.paging.maximum", "5")
    .config("spark.hadoop.fs.s3a.list.version", "2")
    .config("spark.hadoop.fs.s3a.committer.threads", "4")
    .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/tmp/staging")
    .config("spark.hadoop.fs.s3a.buffer.dir", "/tmp")
    .config("spark.hadoop.fs.s3a.multipart.size", "104857600")
    .config("spark.hadoop.fs.s3a.multipart.threshold", "2147483647")
    .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
    .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "4")
    .config("spark.hadoop.fs.s3a.block.size", "33554432")
    .config("spark.hadoop.fs.s3a.metadatastore.authoritative", "false")
    .config("spark.sql.files.maxPartitionBytes", "134217728")
    .config("spark.driver.memory", "2g")
    .config("spark.ui.port", "4050")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("✓ Spark UI available at: http://localhost:4050")
print("✓ Spark session created successfully")

# ── Paths & directories ───────────────────────────────────────────────────────
# BASE_PREFIX = "bronze_usa_ean_hotelbeds"
# BASE_PREFIX = "bronze_mumbai_ean_bookingcom"
BASE_PREFIX = "bronze"
S3_BASE = f"s3a://delta-lake/{BASE_PREFIX}"

CLUSTERS_PATH = f"{S3_BASE}/final_clusters/"

REPORTS_DIR = os.path.expanduser(f"~/Downloads/hotel_mapping_reports/{BASE_PREFIX}")
os.makedirs(REPORTS_DIR, exist_ok=True)

if "RUN_TIMESTAMP" not in globals():
    RUN_TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

# ── Load cluster_data ─────────────────────────────────────────────────────────

cluster_data = spark.read.format("delta").load(CLUSTERS_PATH)

print(f"✓ Loaded clusters from: {CLUSTERS_PATH}")
print(f"  Total rows : {cluster_data.count():,}")
print(f"  Columns    : {len(cluster_data.columns)}")
print()
cluster_data.printSchema()

_total = cluster_data.count()
_distinct = cluster_data.select("uid").distinct().count()
_dup = _total - _distinct
print(
    f"Duplicate check — total: {_total:,}  |  distinct uid: {_distinct:,}  |  duplicate uid rows: {_dup:,}"
)
if _dup > 0:
    print("  Top duplicate uids:")
    cluster_data.groupBy("uid").agg(count("*").alias("cnt")).filter(
        col("cnt") > 1
    ).orderBy(col("cnt").desc()).show(10, truncate=False)

cluster_data = cluster_data.dropDuplicates(["uid"])
print(f"After dedup — {cluster_data.count():,} distinct cluster rows carried forward.")

# ── Load hotel_data ───────────────────────────────────────────────────────────

mapped_path = f"{S3_BASE}/hotels"
print(f"Reading from: {mapped_path}")

hotel_data = spark.read.format("delta").load(mapped_path)

print("\n✓ Data loaded successfully")
print(f"Total records : {hotel_data.count():,}")
print(f"Total columns : {len(hotel_data.columns)}")

hotel_data.printSchema()
hotel_data.show(20, truncate=False)

_total = hotel_data.count()
_distinct = hotel_data.select("uid").distinct().count()
_dup = _total - _distinct
print(
    f"\nDuplicate check — total: {_total:,}  |  distinct uid: {_distinct:,}  |  duplicate uid rows: {_dup:,}"
)
if _dup > 0:
    print("  Top duplicate uids:")
    hotel_data.groupBy("uid").agg(count("*").alias("cnt")).filter(
        col("cnt") > 1
    ).orderBy(col("cnt").desc()).show(10, truncate=False)

hotel_data = hotel_data.dropDuplicates(["uid"])
print(f"After dedup — {hotel_data.count():,} distinct hotels carried forward.")

# ── Load hotel_pairs ──────────────────────────────────────────────────────────

HOTEL_PAIRS_PATH = f"{S3_BASE}/hotel_pairs/"
print(f"Reading from: {HOTEL_PAIRS_PATH}")

hotel_pairs = spark.read.format("delta").load(HOTEL_PAIRS_PATH)

print("\n✓ hotel_pairs loaded successfully")
print(f"Total records : {hotel_pairs.count():,}")
print(f"Total columns : {len(hotel_pairs.columns)}")
hotel_pairs.printSchema()

_total = hotel_pairs.count()
_distinct = hotel_pairs.select("pair_key_left", "pair_key_right").distinct().count()
_dup = _total - _distinct
print(
    f"\nDuplicate check — total: {_total:,}  |  distinct pairs: {_distinct:,}  |  duplicates: {_dup:,}"
)

hotel_pairs = hotel_pairs.dropDuplicates(["pair_key_left", "pair_key_right"])
print(f"After dedup — {hotel_pairs.count():,} distinct pairs carried forward.")

# ── Load config & build WHERE clause expressions ──────────────────────────────

CONFIG_PATH = (
    "/Users/nakul.patil/Documents/hotel-mapping/src/hotel_data/config/config.yaml"
)

CMP_MAP = {"gte": ">=", "lte": "<=", "gt": ">", "lt": "<", "eq": "=", "neq": "!="}


def build_readable(node, indent=0):
    """Recursively build an indented human-readable expression."""
    pad = "  " * indent
    if "signal" in node:
        cmp = CMP_MAP.get(node["comparator"], node["comparator"])
        return f"{pad}{node['signal']} {cmp} {node['threshold']}"
    op = node["operator"].upper()
    parts = [build_readable(r, indent + 1) for r in node["rules"]]
    joiner = f"\n{pad}{op}\n"
    inner = joiner.join(parts)
    return f"{pad}(\n{inner}\n{pad})"


def build_spark_expr(node):
    """Build a flat Spark SQL expression string."""
    if "signal" in node:
        cmp = CMP_MAP.get(node["comparator"], node["comparator"])
        return f"({node['signal']} {cmp} {node['threshold']})"
    op = f" {node['operator'].upper()} "
    parts = [build_spark_expr(r) for r in node["rules"]]
    return "(" + op.join(parts) + ")"


with open(CONFIG_PATH) as f:
    cfg = yaml.safe_load(f)

match_logic = cfg["scoring"]["match_logic"]
human_readable = build_readable(match_logic)
spark_filter = build_spark_expr(match_logic)

print("=" * 70)
print("WHERE CLAUSE (human-readable)")
print("=" * 70)
print(human_readable)
print()

total_pairs = hotel_pairs.count()
matched_pairs = hotel_pairs.filter(expr(spark_filter)).count()
pct_matched = matched_pairs / total_pairs * 100 if total_pairs > 0 else 0.0

print("=" * 70)
print("WHERE CLAUSE YIELD")
print("=" * 70)
print(f"  Total pairs in table           : {total_pairs:>10,}")
print(f"  Pairs passing WHERE clause     : {matched_pairs:>10,}  ({pct_matched:.2f}%)")
print()

if match_logic.get("operator", "").upper() == "AND":
    print("  Per-AND-segment pass rate (all pairs as denominator):")
    print(f"  {'Segment':<55} {'Pass':>8}  {'%':>6}")
    print(f"  {'-' * 55}  {'-' * 8}  {'-' * 6}")
    for i, rule in enumerate(match_logic["rules"]):
        seg_expr = build_spark_expr(rule)
        seg_label = build_readable(rule).strip().replace("\n", " ")[:55]
        seg_count = hotel_pairs.filter(expr(seg_expr)).count()
        seg_pct = seg_count / total_pairs * 100 if total_pairs > 0 else 0.0
        marker = (
            "  ← ⚠️ ZERO" if seg_count == 0 else ("  ← tight" if seg_pct < 5 else "")
        )
        print(f"  {seg_label:<55} {seg_count:>8,}  {seg_pct:>5.1f}%{marker}")
    print()

# ── Enrich hotel_pairs with hotel_data and cluster_data columns ───────────────

HOTEL_JOIN_COLS = [
    "geoCode_lat",
    "geoCode_long",
    "combined_address",
    "contact_phones",
    "contact_emails",
    "starRating",
]
CLUSTER_JOIN_COLS = ["cluster_id"]

original_cols = list(hotel_pairs.columns)
existing_cols = set(original_cols)

hotel_pairs.createOrReplaceTempView("hp_base")
hotel_data.createOrReplaceTempView("hotel_data_v")
cluster_data.createOrReplaceTempView("cluster_data_v")

extra_parts = (
    [f"h_i.{c} AS {c}_i" for c in HOTEL_JOIN_COLS if f"{c}_i" not in existing_cols]
    + [f"h_j.{c} AS {c}_j" for c in HOTEL_JOIN_COLS if f"{c}_j" not in existing_cols]
    + [f"c_i.{c} AS {c}_i" for c in CLUSTER_JOIN_COLS if f"{c}_i" not in existing_cols]
    + [f"c_j.{c} AS {c}_j" for c in CLUSTER_JOIN_COLS if f"{c}_j" not in existing_cols]
)
extra_sql = (", " + ", ".join(extra_parts)) if extra_parts else ""

enriched = spark.sql(f"""
    SELECT hp.*{extra_sql}
    FROM hp_base hp
    LEFT JOIN hotel_data_v   h_i ON hp.uid_i = h_i.uid
    LEFT JOIN hotel_data_v   h_j ON hp.uid_j = h_j.uid
    LEFT JOIN cluster_data_v c_i ON hp.uid_i = c_i.uid
    LEFT JOIN cluster_data_v c_j ON hp.uid_j = c_j.uid
""")

extra_ordered = []
for base in [*HOTEL_JOIN_COLS, *CLUSTER_JOIN_COLS]:
    for suffix in ("i", "j"):
        c = f"{base}_{suffix}"
        if c in enriched.columns:
            extra_ordered.append(c)

orig_kept = [c for c in original_cols if c not in extra_ordered]
final_order = [c for c in orig_kept + extra_ordered if c in enriched.columns]
hotel_pairs = enriched.select(*final_order)

print(
    f"✓ hotel_pairs enriched: {hotel_pairs.count():,} rows, {len(hotel_pairs.columns)} columns"
)
print()
print("All columns after joins:")
for c in hotel_pairs.columns:
    print(f"  {c}")

hotel_pairs.select(*extra_ordered).show(10, truncate=50)
