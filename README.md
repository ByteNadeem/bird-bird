# bird-bird

[![Bird Bird CI](https://github.com/ByteNadeem/bird-bird/actions/workflows/ci.yml/badge.svg)](https://github.com/ByteNadeem/bird-bird/actions/workflows/ci.yml)

## Introduction

## Business Value / Purpose
This project explores how emerging technologies (edge devices, IoT sensors, microcontrollers, and high-definition imaging) can power a game-like wildlife observation experience. The aim is to increase biodiversity data collection while improving engagement and tourism value in rural areas.

## Project Status Snapshot (April 2026)
- Story 1 (clean + standardize data): current and operational.
- Story 2 (coverage and completeness): current and operational.
- Story 3 (migration SQLite schema + validation): current and operational.
- Story 4 (API endpoints and UI integration): current and operational.
- Story 5 (migration timing shift analysis): baseline model script now added at backend/analysis/stats_models.py.
- Automated smoke tests are present under tests/ and pass locally.
- CI runs smoke tests and uses a deterministic raw fixture in the ci job.

## Purpose
This repository structure is a development roadmap that tracks what exists now and what is still planned, so project direction stays explicit over time.

## Structure Roadmap
Legend: [current], [planned], [candidate-removal]

```text
project-root/
|
|-- backend/ [current]
|   |-- analysis/ [current]
|   |   |-- stats_models.py [current]
|   |   `-- plots.py [planned]
|   |
|   |-- data/ [current]
|   |   |-- clean/ [current]
|   |   |-- plots/ [current]
|   |   |-- raw/ [current]
|   |   `-- schema/ [current]
|   |       |-- migration_schema.sql [current]
|   |       |-- movebank_core.sql [current]
|   |       |-- movebank_normalized.sql [current]
|   |       `-- movebank_study_filename_map.json [current]
|   |
|   |-- database/ [current]
|   |   |-- bird_bird.db [current]
|   |   `-- migration.db [current]
|   |
|   |-- services/ [current]
|   |   |-- ebird_api.py [current]
|   |   `-- backend/ [current, candidate-removal]
|   |       `-- data/raw/ [current, candidate-removal]
|   |
|   |-- app.py [current]
|   |-- data_cleaning.py [current, wrapper]
|   |-- data_coverage.py [current, wrapper]
|   |-- init_migration_db.py [current, wrapper]
|   |-- load_movebank_sqlite.py [current, wrapper]
|   `-- verify_movebank_sqlite.py [current, wrapper]
|
|-- scripts/ [current]
|   |-- data_cleaning.py [current]
|   |-- data_coverage.py [current]
|   |-- init_migration_db.py [current]
|   |-- load_ebird_to_migration.py [current]
|   |-- load_movebank_sqlite.py [current]
|   |-- verify_movebank_sqlite.py [current]
|   |-- visualize_ebird.py [current]
|   `-- visualize_movebank_gps.py [current]
|
|-- tests/ [current]
|   |-- test_module_1.py [current]
|   |-- test_module_2.py [current]
|   `-- test_module_3.py [current]
|
|-- docs/ [current]
|   |-- coverage_metrics.json [current]
|   `-- coverage_report.md [current]
|
|-- frontend/ [current]
|   |-- index.html [current]
|   |-- scripts/ [current]
|   `-- styles/ [current]
|
|-- .github/ [current]
|-- .gitignore [current]
|-- README.md [current]
|-- makefile [current]
|-- pytest.ini [current]
|-- requirements.txt [current]
`-- run.sh [current]
```

## Candidate Removals (Do Not Remove Yet)
- backend/services/backend/data/raw appears duplicated relative to backend/data/raw.
- Keep until we confirm no script or job depends on the nested path.

## SQLite Schema
- Core schema: backend/data/schema/movebank_core.sql
- Normalized schema migration: backend/data/schema/movebank_normalized.sql
- Migration schema: backend/data/schema/migration_schema.sql
- Study-id filename map: backend/data/schema/movebank_study_filename_map.json

## SQLite Load and Normalize
Load raw Movebank files into SQLite core tables and refresh normalized join tables.

Preferred (from project root):
```bash
python scripts/load_movebank_sqlite.py
```

With Make:
```bash
make load-sqlite
```

Useful options:
```bash
python scripts/load_movebank_sqlite.py --default-study-id 1841091905
python scripts/load_movebank_sqlite.py --require-mapped-study-id
python scripts/load_movebank_sqlite.py --study-id-map-path backend/data/schema/movebank_study_filename_map.json
python scripts/load_movebank_sqlite.py --truncate-core
python scripts/load_movebank_sqlite.py --skip-normalized
```

## SQLite Verification
Run verification checks for required tables, counts, linkage, normalized counts, and nickname backfill dry-run:
```bash
python scripts/verify_movebank_sqlite.py
```

With Make:
```bash
make verify-sqlite
```

Apply nickname backfill (optional):
```bash
python scripts/verify_movebank_sqlite.py --apply-backfill
```

## Data Cleaning (Story 1)
Clean and standardize raw Movebank event data.

```bash
python scripts/data_cleaning.py --replace-table
```

With Make:
```bash
make clean-data
```

Default cleaned outputs:
- CSV: backend/data/clean/movebank_events_cleaned.csv
- SQLite table: cleaned_observations in backend/database/bird_bird.db

## Data Coverage (Story 2)
Generate completeness metrics and summary report from cleaned CSV.

```bash
python scripts/data_coverage.py
```

With Make:
```bash
make coverage-report
```

Default outputs:
- docs/coverage_report.md
- docs/coverage_metrics.json

## Migration DB (Story 3)
Initialize migration SQLite DB with species + observations, foreign-key validation, and insert/query checks.

```bash
python scripts/init_migration_db.py --replace --seed-from-cleaned
```

Skip eBird integration if needed:
```bash
python scripts/init_migration_db.py --replace --seed-from-cleaned --skip-ebird
```

With Make:
```bash
make init-migration-db
```

Output:
- backend/database/migration.db

## API Endpoints (Story 4)
Run API server:
```bash
python backend/app.py
```

Primary endpoints:
- GET /api/species
- GET /api/migration/
- GET /api/routes/
- GET /api/visualization/
- GET /api/internal/collectibles/individuals

Quick smoke check:
```bash
make test-api
```

## Migration Timing Shift Analysis (Story 5)
Run baseline regression and mixed-effects models for migration timing shifts.

### Command Contract
```bash
python backend/analysis/stats_models.py \
	--db-path backend/database/migration.db \
	--output-dir docs \
	--plot-dir backend/data/plots \
	--min-rows-per-species 50 \
	--model both \
	--alpha 0.05
```

Key options:
- --species-code (repeatable) filter species
- --from-year / --to-year limit year window
- --model ols|mixed|both
- --climate-csv optional climate dataset with required date column
- --climate-vars comma list (for example: tmean,precip)
- --min-climate-coverage default 0.9

Exit codes:
- 0 success
- 2 validation/data-shape error
- 3 model execution failure (no model completed)

Outputs:
- docs/migration_timing_effects.csv
- docs/migration_timing_effects.json
- docs/migration_timing_summary.md
- backend/data/plots/migration_timing_trends.png
- backend/data/plots/migration_timing_residuals.png

Notes:
- This is observational modeling, not causal proof of climate impact.
- Mixed-effects uses deployment_id random intercept to reduce repeated-measure bias.

## Climate Data Options (for Story 5)
Recommended sources for adding climate covariates:
- ERA5-Land (reanalysis, global gridded, strong default for consistency)
- Meteostat (fast station access for daily aggregates)
- NOAA GHCN Daily (station-based, broad historical depth)
- NASA POWER (easy point time series)

Practical integration approach:
1. Start with no-climate baseline model.
2. Add one covariate first (temperature anomaly).
3. Validate coverage and spatial representativeness before interpretation.

## Testing
Run all tests:
```bash
python -m pytest
```

Run smoke subset used in CI:
```bash
python -m pytest -k "migration or cleaned_observations or coverage"
```

## CI Notes
CI workflow:
- installs dependencies and pytest
- creates deterministic raw fixture in ci job for data_cleaning input
- builds cleaned and migration layers
- runs non-destructive smoke tests
- runs API smoke checks

Weekly refresh pipeline:
- refreshes Movebank core tables
- rebuilds cleaned/migration/coverage outputs
- reruns smoke tests and API checks
- uploads database and coverage artifacts

## Troubleshooting
If sensor mismatch exists, use generic mode:
```bash
python movebank_api.py --mode events --study-id <id> --individual-id <id> --sensor-type-id 673
```

Find studies for target taxa:
```bash
python movebank_api.py --mode studies-query --sensor-name GPS --require-download-access --taxon-query "Scolopax rusticola" --taxon-query "Gallinago gallinago" --taxon-query "Numenius arquata"
```


