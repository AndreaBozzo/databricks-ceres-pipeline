# Databricks notebook source
# COMMAND ----------
from pyspark.sql.functions import col, explode, size, date_format, year

# Setup
spark.read.table("silver_ceres_metadata").createOrReplaceTempView("v_silver_metadata")

# COMMAND ----------
# 1. Monthly Ingestion Trend
# Filters out invalid dates (< 2000)
df_trend = spark.sql("""
    SELECT 
        date_format(created_ts, 'yyyy-MM') as month_year,
        portal,
        COUNT(DISTINCT title) as new_datasets
    FROM v_silver_metadata
    WHERE created_ts IS NOT NULL 
      AND year(created_ts) > 2000 
    GROUP BY 1, 2
    ORDER BY 1
""")

df_trend.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_monthly_trend")

# COMMAND ----------
# 2. Topic Analysis (Exploded Tags)
df_topics = spark.sql("""
    SELECT 
        portal,
        explode(tags) as topic,
        COUNT(*) as frequency
    FROM v_silver_metadata
    WHERE size(tags) > 0 
    GROUP BY 1, 2
    ORDER BY frequency DESC
    LIMIT 200
""")

df_topics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_topic_analysis")

# COMMAND ----------
# 3. Portal General Statistics
df_stats = spark.sql("""
    SELECT 
        initcap(portal) as portal_name,
        COUNT(DISTINCT title) as distinct_datasets,
        COUNT(DISTINCT organization) as active_orgs,
        MIN(CASE WHEN year(created_ts) > 2000 THEN created_ts END) as first_valid_dataset,
        MAX(CASE WHEN year(created_ts) <= year(current_date()) THEN created_ts END) as last_valid_dataset
    FROM v_silver_metadata
    WHERE portal IS NOT NULL AND length(trim(portal)) > 0
    GROUP BY 1
    ORDER BY 2 DESC
""")

df_stats.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_portal_stats")

print("Success: All Gold tables created.")