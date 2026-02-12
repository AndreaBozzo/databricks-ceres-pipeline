<div align="center">
  <h1>Databricks Pipeline for Ceres</h1>
  <p><strong>Medallion Architecture pipeline for open data analytics on Databricks</strong></p>
  <p>
    <a href="https://github.com/AndreaBozzo/Ceres"><img src="https://img.shields.io/badge/main_project-Ceres-blue" alt="Ceres"></a>
    <a href="https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index"><img src="https://img.shields.io/badge/%F0%9F%A4%97%20Dataset-ceres--open--data--index-yellow" alt="HuggingFace Dataset"></a>
    <a href="https://github.com/AndreaBozzo/databricks-ceres-pipeline/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License"></a>
  </p>
</div>

---

This repository contains the **Databricks** analytics pipeline for the [Ceres](https://github.com/AndreaBozzo/Ceres) project. It implements a **Medallion Architecture** (Bronze → Silver → Gold) that ingests the Ceres open data index from Hugging Face and produces analytics-ready tables plus a lightweight semantic search engine — all running on Databricks.

## Architecture

![Ceres Databricks Pipeline Architecture](docs/assets/architecture.png)

## Notebooks

| # | Notebook | Layer | Description |
|---|----------|-------|-------------|
| 01 | `01_ingest_huggingface_to_bronze.py` | Bronze | Loads the dataset from Hugging Face, coerces types, and writes to a managed Delta table with audit columns |
| 02 | `02_process_bronze_to_silver.py` | Silver | Deduplicates, parses timestamps, splits tags into arrays, and standardizes text fields |
| 03 | `03_create_gold_analytics.py` | Gold | Produces three analytics tables: monthly ingestion trends, topic frequency analysis, and portal-level statistics |
| 04 | `04_semantic_search_engine.py` | Gold / ML | Builds a TF-IDF feature store using Spark ML and exposes a simple hashing-based search engine with an interactive widget |

## Quick Start

### Prerequisites

- A Databricks workspace (Community Edition works for testing)
- Databricks CLI configured (`databricks configure`)

### Option A — Databricks Asset Bundles (recommended)

```bash
# Clone and deploy
git clone https://github.com/AndreaBozzo/databricks-ceres-pipeline.git
cd databricks-ceres-pipeline

# Validate the bundle
databricks bundle validate

# Deploy to your workspace
databricks bundle deploy -t dev

# Run the pipeline
databricks bundle run ceres_pipeline -t dev
```

### Option B — Manual import

1. Import the four `.py` notebooks into your Databricks workspace
2. Run them in order: `01` → `02` → `03` → `04`
3. Notebook `01` installs HuggingFace dependencies automatically via `%pip`

## Configuration

### Databricks Asset Bundle

The pipeline is configured as a [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/index.html) in [`databricks.yml`](databricks.yml). Targets:

| Target | Description |
|--------|-------------|
| `dev` | Development — runs on your personal workspace folder |
| `prod` | Production — designed for a shared workspace with job scheduling |

### Environment variables

No secrets are required. The pipeline reads from a public Hugging Face dataset.

| Variable | Default | Description |
|----------|---------|-------------|
| `dataset_name` | `AndreaBozzo/ceres-open-data-index` | HuggingFace dataset identifier |
| `table_name` | `bronze_ceres_metadata` | Bronze target table |

## Delta Tables Produced

| Table | Layer | Description |
|-------|-------|-------------|
| `bronze_ceres_metadata` | Bronze | Raw dataset metadata + `ingestion_ts`, `source_system` |
| `silver_ceres_metadata` | Silver | Cleaned, deduplicated, with parsed timestamps and tag arrays |
| `gold_monthly_trend` | Gold | Monthly dataset ingestion counts by portal |
| `gold_topic_analysis` | Gold | Top 200 topics by frequency across portals |
| `gold_portal_stats` | Gold | Per-portal statistics (dataset count, orgs, date range) |
| `gold_ml_features` | Gold | TF-IDF feature vectors (1024-dim) for semantic search |

## Relationship to Ceres

[Ceres](https://github.com/AndreaBozzo/Ceres) is a Rust-based semantic search engine that harvests metadata from CKAN open data portals and indexes them with vector embeddings. This pipeline provides a **complementary analytics layer** on the same data:

- **Ceres** (main repo) → Real-time harvesting, Gemini embeddings, PostgreSQL + pgvector, REST API
- **This pipeline** → Batch analytics, Spark ML features, Delta Lake, Databricks dashboards

Both consume the same [Hugging Face dataset](https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index) as their source of truth.

## Development

```bash
# Lint notebooks
pip install -r requirements-dev.txt
ruff check .

# Run tests (requires Databricks Connect or a cluster)
pytest tests/
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).

## Acknowledgments

- [Ceres](https://github.com/AndreaBozzo/Ceres) — the main semantic search engine project
- [Databricks](https://databricks.com/) — unified analytics platform
- [Hugging Face](https://huggingface.co/) — dataset hosting
- [Delta Lake](https://delta.io/) — open-source storage layer
