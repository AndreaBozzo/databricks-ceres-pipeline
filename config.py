# Shared configuration for the Ceres Pipeline.
# All table names, dataset references, and tunable parameters are defined here.

# Source
DATASET_NAME = "AndreaBozzo/ceres-open-data-index"

# Table names (Medallion Architecture)
BRONZE_TABLE = "bronze_ceres_metadata"
SILVER_TABLE = "silver_ceres_metadata"
GOLD_MONTHLY_TREND = "gold_monthly_trend"
GOLD_TOPIC_ANALYSIS = "gold_topic_analysis"
GOLD_PORTAL_STATS = "gold_portal_stats"
GOLD_ML_FEATURES = "gold_ml_features"

# Feature Engineering
NUM_FEATURES = 256  # kept small to fit serverless ML model size limit (256 MB)
TITLE_WEIGHT = 2  # number of times title is repeated in text_soup for boosting

# Analytics
TOP_TOPICS_LIMIT = 200
MIN_VALID_YEAR = 2000
