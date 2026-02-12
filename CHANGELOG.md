# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] — 2025-02-12

### Added

- `01_ingest_huggingface_to_bronze.py` — Ingest Ceres open data index from Hugging Face into a Bronze Delta table with audit columns (`ingestion_ts`, `source_system`)
- `02_process_bronze_to_silver.py` — Deduplicate, parse timestamps, split tags, and standardize text fields into a Silver Delta table
- `03_create_gold_analytics.py` — Generate Gold analytics tables: monthly trends, topic analysis, and portal statistics
- `04_semantic_search_engine.py` — Build TF-IDF feature vectors with Spark ML and expose a hashing-based semantic search engine with an interactive Databricks widget
- Databricks Asset Bundle configuration (`databricks.yml`) with `dev` and `prod` targets
- CI workflow for linting with Ruff
- Project documentation and README
