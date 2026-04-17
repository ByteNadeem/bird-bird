from pathlib import Path
import json
import subprocess
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_CSV = PROJECT_ROOT / "backend" / "data" / "clean" / "movebank_events_cleaned.csv"
COVERAGE_SCRIPT = PROJECT_ROOT / "scripts" / "data_coverage.py"


def test_coverage_regeneration_outputs_exist(tmp_path):
    assert INPUT_CSV.exists(), f"Missing input CSV: {INPUT_CSV}"
    assert COVERAGE_SCRIPT.exists(), f"Missing script: {COVERAGE_SCRIPT}"

    cmd = [
        sys.executable,
        str(COVERAGE_SCRIPT),
        "--input-csv",
        str(INPUT_CSV),
        "--output-dir",
        str(tmp_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    assert result.returncode == 0, (
        "Coverage script failed.\n"
        f"STDOUT:\n{result.stdout}\n\nSTDERR:\n{result.stderr}"
    )

    md_path = tmp_path / "coverage_report.md"
    json_path = tmp_path / "coverage_metrics.json"

    assert md_path.exists(), "coverage_report.md was not generated"
    assert json_path.exists(), "coverage_metrics.json was not generated"

    payload = json.loads(json_path.read_text(encoding="utf-8"))

    for key in [
        "generated_at",
        "input_csv",
        "threshold_pct",
        "status",
        "overall_missing_pct",
        "overall_coverage_pct",
    ]:
        assert key in payload, f"Missing key in coverage_metrics.json: {key}"

    assert payload["status"] in {"PASS", "FLAG"}
    assert 0.0 <= float(payload["overall_missing_pct"]) <= 100.0
    assert 0.0 <= float(payload["overall_coverage_pct"]) <= 100.0