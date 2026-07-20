"""
Unit tests for pipeline configuration values.

Validates that config.py contains sensible, non-empty values
that won't cause silent failures at runtime.
"""

import pytest

from config import (
    BRONZE_TABLE,
    DATASET_NAME,
    GOLD_ML_FEATURES,
    GOLD_MONTHLY_TREND,
    GOLD_PORTAL_STATS,
    GOLD_TOPIC_ANALYSIS,
    MIN_VALID_YEAR,
    NUM_FEATURES,
    SILVER_TABLE,
    TITLE_WEIGHT,
    TOP_TOPICS_LIMIT,
    quote_ident,
)


class TestConfigValues:
    def test_dataset_name_is_valid_hf_id(self):
        assert "/" in DATASET_NAME, "Dataset name must be in author/name format"
        author, name = DATASET_NAME.split("/", 1)
        assert len(author) > 0
        assert len(name) > 0

    def test_table_names_non_empty(self):
        all_tables = [
            BRONZE_TABLE,
            SILVER_TABLE,
            GOLD_MONTHLY_TREND,
            GOLD_TOPIC_ANALYSIS,
            GOLD_PORTAL_STATS,
            GOLD_ML_FEATURES,
        ]
        for table in all_tables:
            assert isinstance(table, str) and len(table) > 0

    def test_table_names_are_unique(self):
        tables = [
            BRONZE_TABLE,
            SILVER_TABLE,
            GOLD_MONTHLY_TREND,
            GOLD_TOPIC_ANALYSIS,
            GOLD_PORTAL_STATS,
            GOLD_ML_FEATURES,
        ]
        assert len(tables) == len(set(tables)), "Duplicate table names detected"

    def test_num_features_is_power_of_two(self):
        assert NUM_FEATURES > 0
        assert (NUM_FEATURES & (NUM_FEATURES - 1)) == 0, "NUM_FEATURES should be a power of 2 for HashingTF"

    def test_title_weight_positive(self):
        assert isinstance(TITLE_WEIGHT, int)
        assert TITLE_WEIGHT >= 1

    def test_top_topics_limit_positive(self):
        assert isinstance(TOP_TOPICS_LIMIT, int)
        assert TOP_TOPICS_LIMIT > 0

    def test_min_valid_year_reasonable(self):
        assert isinstance(MIN_VALID_YEAR, int)
        assert 1900 <= MIN_VALID_YEAR <= 2025


class TestQuoteIdent:
    @pytest.mark.parametrize("name", ["main", "workspace", "ceres", "ceres_dev", "cat123"])
    def test_valid_identifiers_are_backtick_quoted(self, name: str):
        assert quote_ident(name) == f"`{name}`"

    @pytest.mark.parametrize(
        "name",
        ["", "has space", "has-dash", "a.b", "drop`;", "tbl;DROP", "naïve"],
    )
    def test_invalid_identifiers_raise(self, name: str):
        with pytest.raises(ValueError):
            quote_ident(name)
