"""
Unit tests for Silver-layer transformation logic.

These tests validate the core transformation rules without requiring
a Databricks cluster. They use plain Python/Pandas equivalents of the
Spark transformations defined in 02_process_bronze_to_silver.py.
"""

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Helpers — mirror the Silver transformation logic in pure Pandas
# ---------------------------------------------------------------------------


def apply_silver_transforms(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the same cleaning rules as 02_process_bronze_to_silver.py."""
    # 1. Filter duplicates
    df = df[df["is_duplicate"] == "false"].copy()

    # 2. Parse timestamps (empty → None)
    for col_name in ("metadata_created", "metadata_modified"):
        df[col_name] = df[col_name].replace("", pd.NaT)
        df[col_name] = pd.to_datetime(df[col_name], errors="coerce")

    # 3. Split tags
    df["tags"] = df["tags"].apply(lambda x: [t.strip() for t in x.split(",")] if isinstance(x, str) and x else [])

    # 4. Standardize text
    df["title"] = df["title"].str.strip()
    df["portal"] = df["portal_name"].str.strip().str.lower()

    return df


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_bronze_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "original_id": ["id1", "id2", "id3"],
            "portal_name": [" Milano ", "Roma", "Milano"],
            "organization": ["org_a", "org_b", "org_a"],
            "title": [" Dataset A ", "Dataset B", "Dataset C"],
            "description": ["Desc A", "Desc B", "Desc C"],
            "tags": ["tag1, tag2", "tag3", ""],
            "license": ["CC0", "ODbL", "CC-BY"],
            "language": ["it", "it", "en"],
            "metadata_created": ["2023-01-15T10:00:00", "", "2024-06-01T08:30:00"],
            "metadata_modified": ["2023-02-01T12:00:00", "2024-01-01T00:00:00", ""],
            "url": ["http://a", "http://b", "http://c"],
            "is_duplicate": ["false", "false", "true"],
        }
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSilverTransformations:
    def test_duplicates_removed(self, sample_bronze_df: pd.DataFrame):
        result = apply_silver_transforms(sample_bronze_df)
        assert len(result) == 2
        assert "id3" not in result["original_id"].values

    def test_empty_dates_become_nat(self, sample_bronze_df: pd.DataFrame):
        result = apply_silver_transforms(sample_bronze_df)
        row_b = result[result["original_id"] == "id2"].iloc[0]
        assert pd.isna(row_b["metadata_created"])

    def test_valid_dates_parsed(self, sample_bronze_df: pd.DataFrame):
        result = apply_silver_transforms(sample_bronze_df)
        row_a = result[result["original_id"] == "id1"].iloc[0]
        assert row_a["metadata_created"].year == 2023

    def test_tags_split_into_list(self, sample_bronze_df: pd.DataFrame):
        result = apply_silver_transforms(sample_bronze_df)
        row_a = result[result["original_id"] == "id1"].iloc[0]
        assert row_a["tags"] == ["tag1", "tag2"]

    def test_empty_tags_become_empty_list(self, sample_bronze_df: pd.DataFrame):
        # id3 is duplicate so it's filtered. Manually test with a non-dup empty tag
        df = sample_bronze_df.copy()
        df.loc[2, "is_duplicate"] = "false"
        result = apply_silver_transforms(df)
        row_c = result[result["original_id"] == "id3"].iloc[0]
        assert row_c["tags"] == []

    def test_title_stripped(self, sample_bronze_df: pd.DataFrame):
        result = apply_silver_transforms(sample_bronze_df)
        row_a = result[result["original_id"] == "id1"].iloc[0]
        assert row_a["title"] == "Dataset A"

    def test_portal_lowered_and_stripped(self, sample_bronze_df: pd.DataFrame):
        result = apply_silver_transforms(sample_bronze_df)
        row_a = result[result["original_id"] == "id1"].iloc[0]
        assert row_a["portal"] == "milano"
