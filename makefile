.PHONY: help run spplist recent recent-species load-sqlite

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
