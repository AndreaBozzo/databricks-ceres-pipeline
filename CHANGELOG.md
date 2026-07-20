# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] — 2026-07-20

### Added

- `pipeline/ceres_medallion.sql` — the Silver and Gold layers as a **Lakeflow (Spark) Declarative Pipeline**: declarative materialized views with `EXPECT` data-quality expectations (replacing the notebooks' manual `assert`s) and built-in lineage.
- `sample_rows` job parameter on the Bronze ingest for cheap end-to-end test runs (`0` = full load).
- `catalog` / `schema` bundle variables threaded through the Bronze/search notebooks (as job params) and the pipeline, so `--var catalog=workspace` works end-to-end on Free Edition.

### Changed

- Migrated Silver/Gold from imperative notebooks to the Lakeflow pipeline; the job DAG is now `ingest_bronze` → `transform_medallion` (pipeline) → `semantic_search`.
- Reworked Bronze ingestion to download `all.parquet` directly via `huggingface_hub` and read it with Spark (staged through a UC Volume for serverless), replacing the `datasets` library — whose fsspec globbing is incompatible with the Databricks runtime's fsspec (`HfFileSystem.find() got multiple values for keyword argument 'maxdepth'`). Bumped `huggingface_hub` to `>=0.24,<2.0` (was `<1.0`).
- Sampling (`sample_rows`) is now a representative **random** sample; `all.parquet` is ordered by portal, so the previous head-slice would only have covered the largest portal.
- README: updated for Databricks **Free Edition** (Community Edition retired 2026-01-01) and cross-linked the sibling `ceres-discovery-agent` (search/agent layer) to clarify this repo's batch-analytics scope.

### Removed

- `02_process_bronze_to_silver.py` and `03_create_gold_analytics.py` — superseded by `pipeline/ceres_medallion.sql`.
- The `datasets` runtime dependency (Bronze now reads the parquet directly).

### Fixed

- README: `gold_ml_features` is 256-dim (matches `config.NUM_FEATURES`), not 1024-dim.

### Security

- `config.quote_ident()` validates and backtick-quotes `catalog`/`schema`/volume identifiers before they are interpolated into `USE`/`CREATE` statements in the notebooks, keeping those statements robust and injection-safe.

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
