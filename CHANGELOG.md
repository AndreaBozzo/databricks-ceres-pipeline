# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] — 2026-02-19

### Added

- `01_ingest_huggingface_to_bronze.py` — Ingest Ceres open data index from Hugging Face into a Bronze Delta table with audit columns (`ingestion_ts`, `source_system`, `source_sha`)
- `02_process_bronze_to_silver.py` — Deduplicate, parse timestamps, split tags, and standardize text fields into a Silver Delta table
- `03_create_gold_analytics.py` — Generate Gold analytics tables: monthly trends, topic analysis (top-N per portal via window functions), and portal statistics
- `04_semantic_search_engine.py` — Pure SQL TF-IDF pipeline with multilingual stopwords (EN/IT/DE/FR/ES), sparse map storage, and dot-product search engine with interactive Databricks widget
- `config.py` — Centralized configuration for table names, dataset references, and tunable parameters
- `requirements.txt` — Runtime dependencies (`huggingface_hub`, `datasets`)
- Databricks Asset Bundle configuration (`databricks.yml`) with `dev` and `prod` targets
- CI workflow for linting with Ruff and testing with pytest
- Unit tests for config validation, search logic (dot product, text_soup), and Silver transformations
- SHA-based fingerprint check to skip Bronze ingestion when dataset is unchanged
- Structured logging and data quality assertions across all notebooks
- Unicode-aware text processing (`\w` regex) for future multi-script portal support
- Project documentation and README
