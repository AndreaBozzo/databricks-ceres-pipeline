# Databricks notebook source
# COMMAND ----------
# Install runtime dependencies (see requirements.txt for version ranges)
%pip install "huggingface_hub>=0.24,<2.0" "datasets>=2.20,<5.0"

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import logging

from huggingface_hub import HfApi
from datasets import load_dataset
from pyspark.sql.functions import current_timestamp, lit
from config import DATASET_NAME, BRONZE_TABLE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ceres.bronze")

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

if last_sha == current_sha:
    logger.info("Skipping ingestion: dataset unchanged (sha=%s)", current_sha[:8])
    dbutils.notebook.exit(f"SKIPPED: sha={current_sha[:8]}")

logger.info("Dataset updated (sha=%s), proceeding with ingestion.", current_sha[:8])

# COMMAND ----------
# 2. Load dataset via HuggingFace datasets library and convert to Spark DataFrame
# Uses Arrow under the hood — single driver load but more memory-efficient than raw Pandas
ds = load_dataset(DATASET_NAME, split="train")
pdf = ds.to_pandas()

# Cast object columns to string for consistent Bronze schema
for c in pdf.columns:
    if pdf[c].dtype == "object":
        pdf[c] = pdf[c].astype(str)

df_raw = spark.createDataFrame(pdf)
logger.info("Loaded %d records from HuggingFace.", len(pdf))

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
