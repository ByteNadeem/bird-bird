.PHONY: help run spplist recent recent-species load-sqlite verify-sqlite clean-data coverage-report init-migration-db run-api test-api

PY ?= python
SCRIPT := backend/services/ebird_api.py
REGION ?= GB-ENG-CON
SPECIES ?=
MAX_RESULTS ?= 50
TIMEOUT ?= 20
OUTPUT_DIR ?= backend/data/raw

help:
	@echo "bird-bird Make targets"
	@echo "  make spplist REGION=GB-ENG-CON"
	@echo "  make recent REGION=GB-ENG-CON MAX_RESULTS=50"
	@echo "  make recent-species REGION=GB-ENG-CON SPECIES=barswa"
	@echo "  make run REGION=GB-ENG-CON"
	@echo "  make load-sqlite"
	@echo "  make verify-sqlite"
	@echo "  make clean-data"
	@echo "  make coverage-report"
	@echo "  make init-migration-db"
	@echo "  make run-api"
	@echo "  make test-api"
	@echo ""
	@echo "Variables: PY, REGION, SPECIES, MAX_RESULTS, TIMEOUT, OUTPUT_DIR"

# Alias for the most common new mode.
run: spplist

spplist:
	$(PY) $(SCRIPT) --region-spplist $(REGION) --timeout $(TIMEOUT) --output-dir $(OUTPUT_DIR)

recent:
	$(PY) $(SCRIPT) --region $(REGION) --max-results $(MAX_RESULTS) --timeout $(TIMEOUT) --output-dir $(OUTPUT_DIR)

recent-species:
	$(PY) $(SCRIPT) --region $(REGION) --species $(SPECIES) --max-results $(MAX_RESULTS) --timeout $(TIMEOUT) --output-dir $(OUTPUT_DIR)

load-sqlite:
	$(PY) scripts/load_movebank_sqlite.py

verify-sqlite:
	$(PY) scripts/verify_movebank_sqlite.py

clean-data:
	$(PY) scripts/data_cleaning.py --replace-table

coverage-report:
	$(PY) scripts/data_coverage.py

init-migration-db:
	$(PY) scripts/init_migration_db.py --replace --seed-from-cleaned

run-api:
	$(PY) backend/app.py

test-api:
	$(PY) -c "from backend.app import app; c=app.test_client(); r1=c.get('/api/species'); r2=c.get('/api/migration/?limit=10'); print('species status:', r1.status_code, 'count:', r1.get_json().get('meta',{}).get('count')); print('migration status:', r2.status_code, 'count:', r2.get_json().get('meta',{}).get('count'))"
