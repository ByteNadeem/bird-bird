#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-help}"

PY="${PY:-python}"
SCRIPT="backend/services/ebird_api.py"
REGION="${REGION:-GB-ENG-CON}"
SPECIES="${SPECIES:-}"
MAX_RESULTS="${MAX_RESULTS:-50}"
TIMEOUT="${TIMEOUT:-20}"
OUTPUT_DIR="${OUTPUT_DIR:-backend/data/raw}"

print_help() {
  echo "bird-bird runner"
  echo "  ./run.sh spplist"
  echo "  ./run.sh recent"
  echo "  ./run.sh recent-species"
  echo ""
  echo "Overrides (env vars): REGION, SPECIES, MAX_RESULTS, TIMEOUT, OUTPUT_DIR, PY"
  echo "Example: REGION=GB-ENG-CON ./run.sh spplist"
}

case "$MODE" in
  help|-h|--help)
    print_help
    ;;
  run|spplist)
    "$PY" "$SCRIPT" --region-spplist "$REGION" --timeout "$TIMEOUT" --output-dir "$OUTPUT_DIR"
    ;;
  recent)
    "$PY" "$SCRIPT" --region "$REGION" --max-results "$MAX_RESULTS" --timeout "$TIMEOUT" --output-dir "$OUTPUT_DIR"
    ;;
  recent-species)
    if [[ -z "$SPECIES" ]]; then
      echo "SPECIES is required for recent-species mode (example: SPECIES=barswa ./run.sh recent-species)" >&2
      exit 1
    fi
    "$PY" "$SCRIPT" --region "$REGION" --species "$SPECIES" --max-results "$MAX_RESULTS" --timeout "$TIMEOUT" --output-dir "$OUTPUT_DIR"
    ;;
  *)
    echo "Unknown mode: $MODE" >&2
    print_help
    exit 1
    ;;
esac
