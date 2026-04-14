# bird-bird

## Purpose
This repository structure is a development roadmap.
It tracks what already exists and what is planned, so we can keep direction and project integrity over time.

## Structure Roadmap
Legend: [current], [planned], [candidate-removal]

```text
project-root/
|
|-- backend/ [current]
|   |-- data/ [current]
|   |   |-- raw/ [current]
|   |   |-- plots/ [current]
|   |   `-- schema/ [current]
|   |       `-- movebank_core.sql [current]
|   |
|   |-- services/ [current]
|   |   |-- ebird_api.py [current]
|   |   `-- backend/ [current, candidate-removal]
|   |       `-- data/raw/ [current, candidate-removal]
|   |
|   |-- app.py [planned]
|   |-- config.py [planned]
|   |-- load_movebank_sqlite.py [current, wrapper]
|   |-- verify_movebank_sqlite.py [current, wrapper]
|   |
|   |-- analysis/ [planned]
|   |   |-- stats_models.py [planned]
|   |   `-- plots.py [planned]
|   |
|   |-- utils/ [planned]
|   |   `-- helpers.py [planned]
|   |
|   `-- database/ [planned]
|       `-- bird_bird.db [planned, SQLite]
|
|-- scripts/ [current]
|   |-- verify_movebank_sqlite.py [current]
|   |-- visualize_ebird.py [current]
|   `-- visualize_movebank_gps.py [current]
|
|-- frontend/ [planned]
|   |-- index.html [planned]
|   |-- styles/main.css [planned]
|   |-- scripts/ [planned]
|   `-- assets/ [planned]
|
|-- notebooks/ [planned]
|   |-- exploratory_analysis.ipynb [planned]
|   `-- data_validation.ipynb [planned]
|
|-- docs/ [planned]
|   |-- dissertation/ [planned]
|   |-- diagrams/ [planned]
|   `-- outreach/ [planned]
|
|-- tests/ [planned]
|   |-- test_api_endpoints.py [planned]
|   |-- test_data_cleaning.py [planned]
|   `-- test_stats.py [planned]
|
|-- .docs/writeup.txt [current]
|-- .github/ [current]
|-- .env [current]
|-- .gitignore [current]
|-- ebird_api.py [current]
|-- movebank_api.py [current]
|-- requirements.txt [current]
|-- makefile [current]
|-- run.sh [current]
`-- README.md [current]
```

## Candidate Removals (Do Not Remove Yet)
- backend/services/backend/data/raw appears duplicated relative to backend/data/raw.
- Keep until we confirm no scripts depend on that nested path.

## SQLite Schema
- Schema file location: backend/data/schema/movebank_core.sql
- Normalization migration: backend/data/schema/movebank_normalized.sql
- Filename study mapping: backend/data/schema/movebank_study_filename_map.json
- Planned SQLite database location: backend/database/bird_bird.db

Optional database initialization command:
```bash
sqlite3 backend/database/bird_bird.db ".read backend/data/schema/movebank_core.sql"
```

## SQLite Load and Normalize
Load raw Movebank files into SQLite core tables and refresh normalized join tables.

Preferred (canonical, from project root):
```bash
python scripts/load_movebank_sqlite.py
```

Compatibility command (if your shell is in backend/):
```bash
python load_movebank_sqlite.py
```

With Make (from project root):
```bash
make load-sqlite
```

Optional: set a fallback study ID for individual files that do not include study_id:
```bash
python scripts/load_movebank_sqlite.py --default-study-id 1841091905
```

Optional: require every individual file to resolve a study_id (mapping or fallback):
```bash
python scripts/load_movebank_sqlite.py --require-mapped-study-id
```

Optional: use a custom filename map file:
```bash
python scripts/load_movebank_sqlite.py --study-id-map-path backend/data/schema/movebank_study_filename_map.json
```

Optional: reload from scratch (truncate core tables first):
```bash
python scripts/load_movebank_sqlite.py --truncate-core
```

Optional: skip normalized migration step:
```bash
python scripts/load_movebank_sqlite.py --skip-normalized
```

## SQLite Verification
Run verification checks (required tables, row counts, study linkage, normalized counts, and nickname backfill dry-run):
```bash
python scripts/verify_movebank_sqlite.py
```

Compatibility command (if your shell is in backend/):
```bash
python verify_movebank_sqlite.py
```

With Make (from project root):
```bash
make verify-sqlite
```

Optional: apply nickname backfill update after dry-run:
```bash
python scripts/verify_movebank_sqlite.py --apply-backfill
```

## Troubleshooting
If sensor mismatch exists, use generic mode:
```bash
python movebank_api.py --mode events --study-id <id> --individual-id <id> --sensor-type-id 673
```

Find studies for target taxa:
```bash
python movebank_api.py --mode studies-query --sensor-name GPS --require-download-access --taxon-query "Scolopax rusticola" --taxon-query "Gallinago gallinago" --taxon-query "Numenius arquata"
```
