# Databricks notebook source
# COMMAND ----------
import logging

from config import (
    GOLD_MONTHLY_TREND, GOLD_PORTAL_STATS, GOLD_TOPIC_ANALYSIS,
    MIN_VALID_YEAR, SILVER_TABLE, TOP_TOPICS_LIMIT,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ceres.gold")

# Setup: register Silver table as temp view for all 3 Gold queries
df_silver = spark.read.table(SILVER_TABLE)
df_silver.createOrReplaceTempView("v_silver_metadata")
silver_count = df_silver.count()
logger.info("Silver table loaded: %d records.", silver_count)

# COMMAND ----------
# 1. Monthly Ingestion Trend
# Filters out invalid dates (< MIN_VALID_YEAR)
df_trend = spark.sql(f"""
    SELECT
        date_format(created_ts, 'yyyy-MM') as month_year,
        portal,
        COUNT(DISTINCT title) as new_datasets
    FROM v_silver_metadata
    WHERE created_ts IS NOT NULL
      AND year(created_ts) > {MIN_VALID_YEAR}
    GROUP BY 1, 2
    ORDER BY 1
""")

df_trend.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_MONTHLY_TREND)
trend_count = spark.read.table(GOLD_MONTHLY_TREND).count()
logger.info("Monthly trend: %d rows written to %s.", trend_count, GOLD_MONTHLY_TREND)

# COMMAND ----------
# 2. Topic Analysis — top N topics per portal using window function
# Avoids global ORDER BY on millions of exploded rows
df_topics = spark.sql(f"""
    WITH tag_counts AS (
        SELECT
            portal,
            explode(tags) as topic,
            COUNT(*) as frequency
        FROM v_silver_metadata
        WHERE size(tags) > 0
        GROUP BY 1, 2
    ),
    ranked AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY portal ORDER BY frequency DESC) as rn
        FROM tag_counts
    )
    SELECT portal, topic, frequency
    FROM ranked
    WHERE rn <= {TOP_TOPICS_LIMIT}
    ORDER BY portal, frequency DESC
""")

df_topics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_TOPIC_ANALYSIS)
topics_count = spark.read.table(GOLD_TOPIC_ANALYSIS).count()
logger.info("Topic analysis: %d rows written to %s.", topics_count, GOLD_TOPIC_ANALYSIS)

# COMMAND ----------
# 3. Portal General Statistics
df_stats = spark.sql(f"""
    SELECT
        initcap(portal) as portal_name,
        COUNT(DISTINCT title) as distinct_datasets,
        COUNT(DISTINCT organization) as active_orgs,
        MIN(CASE WHEN year(created_ts) > {MIN_VALID_YEAR} THEN created_ts END) as first_valid_dataset,
        MAX(CASE WHEN year(created_ts) <= year(current_date()) THEN created_ts END) as last_valid_dataset
    FROM v_silver_metadata
    WHERE portal IS NOT NULL AND length(trim(portal)) > 0
    GROUP BY 1
    ORDER BY 2 DESC
""")

df_stats.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_PORTAL_STATS)
stats_count = spark.read.table(GOLD_PORTAL_STATS).count()
assert stats_count > 0, "Portal stats table is empty — no valid portals found"
logger.info("Portal stats: %d portals written to %s.", stats_count, GOLD_PORTAL_STATS)

logger.info("All Gold tables created successfully.")
