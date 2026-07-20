"""
Structural tests for the Lakeflow Declarative Pipeline SQL.

These run offline: they don't execute the SQL, they just guard the contract
(expected materialized views, expectations, and config placeholders) so an
accidental edit to pipeline/ceres_medallion.sql is caught in CI.
"""

import re
from pathlib import Path

import pytest

SQL_PATH = Path(__file__).resolve().parent.parent / "pipeline" / "ceres_medallion.sql"


@pytest.fixture(scope="module")
def sql_text() -> str:
    # Collapse runs of whitespace so alignment padding doesn't break matches.
    return re.sub(r"\s+", " ", SQL_PATH.read_text(encoding="utf-8"))


def test_sql_file_exists():
    assert SQL_PATH.is_file(), f"Missing pipeline SQL at {SQL_PATH}"


@pytest.mark.parametrize(
    "view",
    [
        "silver_ceres_metadata",
        "gold_monthly_trend",
        "gold_topic_analysis",
        "gold_portal_stats",
    ],
)
def test_defines_materialized_view(sql_text: str, view: str):
    assert f"MATERIALIZED VIEW {view}" in sql_text


def test_has_data_quality_expectations(sql_text: str):
    # Manual asserts in the old notebooks are now declarative expectations.
    assert "CONSTRAINT valid_title EXPECT" in sql_text
    assert "ON VIOLATION DROP ROW" in sql_text


@pytest.mark.parametrize(
    "placeholder",
    ["${bronze_table}", "${min_valid_year}", "${top_topics_limit}"],
)
def test_references_pipeline_configuration(sql_text: str, placeholder: str):
    assert placeholder in sql_text
