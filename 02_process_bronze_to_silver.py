# Databricks notebook source
# COMMAND ----------
from pyspark.sql.functions import col, to_timestamp, split, trim, when, lower

# Configuration
source_table = "bronze_ceres_metadata"
target_table = "silver_ceres_metadata"

df_bronze = spark.read.table(source_table)

# Transformation Logic
# 1. Filter duplicates
# 2. Handle empty strings in date columns
# 3. Convert tags string to array
# 4. Standardize text fields
df_silver = df_bronze \
    .filter(col("is_duplicate") == "false") \
    .withColumn("created_ts", 
                when(col("metadata_created") == "", None)
                .otherwise(to_timestamp(col("metadata_created")))) \
    .withColumn("modified_ts", 
                when(col("metadata_modified") == "", None)
                .otherwise(to_timestamp(col("metadata_modified")))) \
    .withColumn("tags_array", split(col("tags"), ",\s*")) \
    .withColumn("title", trim(col("title"))) \
    .withColumn("portal", lower(trim(col("portal_name")))) \
    .select(
        col("original_id"),
        col("portal"),
        col("organization"),
        col("title"),
        col("description"),
        col("tags_array").alias("tags"),
        col("license"),
        col("language"),
        col("created_ts"),
        col("modified_ts"),
        col("url")
    )

# Write to Silver Layer
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(target_table)

print(f"Success: Table {target_table} updated.")