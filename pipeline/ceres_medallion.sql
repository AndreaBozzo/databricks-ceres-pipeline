-- Ceres medallion transforms as a Lakeflow (Spark) Declarative Pipeline.
--
-- This replaces the imperative Silver (02) and Gold (03) notebooks with
-- declarative materialized views. The Bronze layer stays a Python task
-- (01_ingest_huggingface_to_bronze.py) because it pulls from the Hugging Face
-- Hub and keeps a SHA fingerprint to skip unchanged loads — logic that is
-- inherently imperative and source-specific.
--
-- Data-quality checks that were manual `assert`s in the notebooks are now
-- `EXPECT` expectations, tracked in the pipeline event log. Config values
-- (${bronze_table}, ${min_valid_year}, ${top_topics_limit}) come from the
-- pipeline `configuration` block in databricks.yml.

-- Silver: dedupe, parse timestamps, split tags, standardize text.
-- Mirrors 02_process_bronze_to_silver.py. Reads the external Bronze table
-- produced by the ingest task.
CREATE OR REFRESH MATERIALIZED VIEW silver_ceres_metadata (
  CONSTRAINT valid_title  EXPECT (title IS NOT NULL AND length(trim(title)) > 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_portal EXPECT (portal IS NOT NULL)                            ON VIOLATION DROP ROW
)
COMMENT "Cleaned, deduplicated Ceres metadata: parsed timestamps, tag arrays, standardized text."
AS
SELECT
  original_id,
  lower(trim(portal_name))                                                           AS portal,
  source_portal,
  organization,
  trim(title)                                                                        AS title,
  description,
  split(tags, ',\\s*')                                                               AS tags,
  license,
  language,
  CASE WHEN metadata_created  = '' THEN NULL ELSE to_timestamp(metadata_created)  END AS created_ts,
  CASE WHEN metadata_modified = '' THEN NULL ELSE to_timestamp(metadata_modified) END AS modified_ts,
  CASE WHEN first_seen_at     = '' THEN NULL ELSE to_timestamp(first_seen_at)     END AS first_seen_ts,
  url
FROM ${bronze_table}
WHERE is_duplicate = false;

-- Gold 1: monthly ingestion trend by portal.
-- Mirrors 03_create_gold_analytics.py query 1.
CREATE OR REFRESH MATERIALIZED VIEW gold_monthly_trend
COMMENT "Monthly dataset ingestion counts by portal."
AS
SELECT
  date_format(created_ts, 'yyyy-MM') AS month_year,
  portal,
  COUNT(DISTINCT title)              AS new_datasets
FROM silver_ceres_metadata
WHERE created_ts IS NOT NULL
  AND year(created_ts) > ${min_valid_year}
GROUP BY 1, 2;

-- Gold 2: top-N topics per portal by frequency.
-- Mirrors 03_create_gold_analytics.py query 2 (window function avoids a global
-- ORDER BY over millions of exploded tag rows).
CREATE OR REFRESH MATERIALIZED VIEW gold_topic_analysis
COMMENT "Top-N topics per portal by frequency across the index."
AS
WITH tag_counts AS (
  SELECT
    portal,
    explode(tags) AS topic,
    COUNT(*)      AS frequency
  FROM silver_ceres_metadata
  WHERE size(tags) > 0
  GROUP BY 1, 2
),
ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY portal ORDER BY frequency DESC) AS rn
  FROM tag_counts
)
SELECT portal, topic, frequency
FROM ranked
WHERE rn <= ${top_topics_limit};

-- Gold 3: per-portal statistics.
-- Mirrors 03_create_gold_analytics.py query 3.
CREATE OR REFRESH MATERIALIZED VIEW gold_portal_stats (
  CONSTRAINT has_portal EXPECT (portal_name IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Per-portal statistics: distinct datasets, active orgs, valid date range."
AS
SELECT
  initcap(portal)                                                                    AS portal_name,
  COUNT(DISTINCT title)                                                              AS distinct_datasets,
  COUNT(DISTINCT organization)                                                       AS active_orgs,
  MIN(CASE WHEN year(created_ts) > ${min_valid_year} THEN created_ts END)            AS first_valid_dataset,
  MAX(CASE WHEN year(created_ts) <= year(current_date()) THEN created_ts END)        AS last_valid_dataset
FROM silver_ceres_metadata
WHERE portal IS NOT NULL AND length(trim(portal)) > 0
GROUP BY 1;
