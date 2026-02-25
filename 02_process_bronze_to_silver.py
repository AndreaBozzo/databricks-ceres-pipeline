# Databricks notebook source
# COMMAND ----------
import logging

from pyspark.sql.functions import col, to_timestamp, split, trim, when, lower
from config import BRONZE_TABLE, SILVER_TABLE

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ceres.silver")

df_bronze = spark.read.table(BRONZE_TABLE)
bronze_count = df_bronze.count()
logger.info("Read %d records from %s.", bronze_count, BRONZE_TABLE)

# Transformation Logic
# 1. Filter duplicates
# 2. Handle empty strings in date columns
# 3. Convert tags string to array
# 4. Standardize text fields
df_silver = (
    df_bronze
    .filter(col("is_duplicate") == False)  # noqa: E712
    .withColumn("created_ts",
                when(col("metadata_created") == "", None)
                .otherwise(to_timestamp(col("metadata_created"))))
    .withColumn("modified_ts",
                when(col("metadata_modified") == "", None)
                .otherwise(to_timestamp(col("metadata_modified"))))
    .withColumn("tags_array", split(col("tags"), r",\s*"))
    .withColumn("title", trim(col("title")))
    .withColumn("portal", lower(trim(col("portal_name"))))
    .withColumn("first_seen_ts",
                when(col("first_seen_at") == "", None)
                .otherwise(to_timestamp(col("first_seen_at"))))
    .select(
        col("original_id"),
        col("portal"),
        col("source_portal"),
        col("organization"),
        col("title"),
        col("description"),
        col("tags_array").alias("tags"),
        col("license"),
        col("language"),
        col("created_ts"),
        col("modified_ts"),
        col("first_seen_ts"),
        col("url")
    )
)

# Write to Silver Layer
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_TABLE)

# Data quality checks
silver_count = spark.read.table(SILVER_TABLE).count()
assert silver_count > 0, f"Silver table {SILVER_TABLE} is empty"
null_titles = spark.read.table(SILVER_TABLE).filter(col("title").isNull()).count()
assert null_titles == 0, f"Silver table has {null_titles} null titles"
logger.info("Silver processing complete: %d → %d records (%.1f%% dedup rate).",
            bronze_count, silver_count, (1 - silver_count / bronze_count) * 100)
