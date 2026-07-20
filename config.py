# Shared configuration for the Ceres Pipeline.
# All table names, dataset references, and tunable parameters are defined here.

import re

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

# --- Identifier safety ------------------------------------------------------

_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")


def quote_ident(name: str) -> str:
    """Validate a catalog/schema/table identifier and return it backtick-quoted.

    Catalog and schema names reach the notebooks as job parameters/widgets and
    are interpolated into ``USE``/``CREATE`` statements. Restricting them to a
    safe character set and backtick-quoting the result keeps those statements
    robust and free of injection, whatever a caller passes in.
    """
    if not _IDENT_RE.match(name or ""):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f"`{name}`"
