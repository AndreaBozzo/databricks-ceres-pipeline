# Databricks notebook source
# COMMAND ----------
# Install runtime dependencies (see requirements.txt for version ranges)
%pip install "huggingface_hub>=0.24,<2.0"

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import shutil
import logging

from huggingface_hub import HfApi, hf_hub_download
from pyspark.sql.functions import current_timestamp, lit, rand
from config import DATASET_NAME, BRONZE_TABLE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ceres.bronze")

HF_FILENAME = "all.parquet"   # canonical complete index in the published snapshot
STAGING_VOLUME = "staging"

# COMMAND ----------
# Resolve target catalog/schema (passed by the Asset Bundle job; safe defaults
# for manual runs) and an optional row cap for cheap end-to-end test runs.
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "ceres")
dbutils.widgets.text("sample_rows", "0")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
SAMPLE_ROWS = int(dbutils.widgets.get("sample_rows") or "0")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
logger.info("Target: %s.%s (sample_rows=%d)", CATALOG, SCHEMA, SAMPLE_ROWS)

# COMMAND ----------
# 1. Check dataset fingerprint to skip ingestion if unchanged
api = HfApi()
dataset_info = api.dataset_info(DATASET_NAME)
current_sha = dataset_info.sha

last_sha = None
if spark.catalog.tableExists(BRONZE_TABLE):
    try:
        last_sha = spark.read.table(BRONZE_TABLE).select("source_sha").first()[0]
    except Exception:
        pass  # column missing from older runs, proceed with ingestion

if last_sha == current_sha and SAMPLE_ROWS == 0:
    logger.info("Skipping ingestion: dataset unchanged (sha=%s)", current_sha[:8])
    dbutils.notebook.exit(f"SKIPPED: sha={current_sha[:8]}")

logger.info("Dataset updated (sha=%s), proceeding with ingestion.", current_sha[:8])

# COMMAND ----------
# 2. Download the canonical all.parquet snapshot and read it with Spark.
# We avoid the `datasets` library: its fsspec-based globbing is incompatible with
# the Databricks runtime's fsspec. Serverless Spark also can't read the driver-local
# HuggingFace cache, so we stage the file into a UC Volume first, then read it back.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{STAGING_VOLUME}")
staged_path = f"/Volumes/{CATALOG}/{SCHEMA}/{STAGING_VOLUME}/{HF_FILENAME}"
local_path = hf_hub_download(repo_id=DATASET_NAME, filename=HF_FILENAME, repo_type="dataset")
shutil.copyfile(local_path, staged_path)

df_raw = spark.read.parquet(staged_path)
if SAMPLE_ROWS > 0:
    # Random sample, not a head LIMIT: all.parquet is ordered by portal, so a head
    # slice would only cover the largest portal. Shuffle first to span all portals.
    df_raw = df_raw.orderBy(rand(seed=42)).limit(SAMPLE_ROWS)
    logger.info("Sampling %d rows (random, seed=42).", SAMPLE_ROWS)
logger.info("Loaded snapshot from %s.", staged_path)

# COMMAND ----------
# 3. Add Audit Columns (including source SHA for fingerprint tracking)
df_bronze = df_raw.withColumn("ingestion_ts", current_timestamp()) \
                  .withColumn("source_system", lit("HuggingFace")) \
                  .withColumn("source_sha", lit(current_sha))

# 4. Write to Delta Lake (Managed Table)
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(BRONZE_TABLE)

# 5. Data quality check
row_count = spark.read.table(BRONZE_TABLE).count()
assert row_count > 0, f"Bronze table {BRONZE_TABLE} is empty after ingestion"
logger.info("Bronze ingestion complete: %d records written (sha=%s).", row_count, current_sha[:8])
