import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_CSV = PROJECT_ROOT / "backend" / "data" / "clean" / "movebank_events_cleaned.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "docs"
DEFAULT_THRESHOLD = 90.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate data completeness coverage report from cleaned CSV.",
    )
    parser.add_argument(
        "--input-csv",
        default=str(DEFAULT_INPUT_CSV),
        help="Path to cleaned CSV file",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for report outputs",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=DEFAULT_THRESHOLD,
        help="Coverage pass threshold percentage (default: 90.0)",
    )
    return parser.parse_args()


def resolve_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def is_missing(value: object) -> bool:
    if value is None:
        return True
    return str(value).strip() == ""


def load_rows(input_csv: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")

    with input_csv.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    if not fieldnames:
        raise ValueError("Input CSV has no header columns.")

    return fieldnames, rows


def compute_metrics(columns: list[str], rows: list[dict[str, str]]) -> dict[str, object]:
    row_count = len(rows)
    if row_count == 0:
        raise ValueError("Input CSV has no data rows.")

    per_column: list[dict[str, object]] = []
    total_cells = row_count * len(columns)
    total_missing = 0

    for col in columns:
        missing = sum(1 for row in rows if is_missing(row.get(col)))
        non_missing = row_count - missing
        missing_pct = (missing / row_count) * 100
        coverage_pct = 100 - missing_pct

        total_missing += missing
        per_column.append(
            {
                "column": col,
                "rows": row_count,
                "missing_count": missing,
                "missing_pct": round(missing_pct, 4),
                "coverage_pct": round(coverage_pct, 4),
                "non_missing_count": non_missing,
            }
        )

    overall_missing_pct = (total_missing / total_cells) * 100
    overall_coverage_pct = 100 - overall_missing_pct

    return {
        "row_count": row_count,
        "column_count": len(columns),
        "total_cells": total_cells,
        "total_missing": total_missing,
        "overall_missing_pct": round(overall_missing_pct, 4),
        "overall_coverage_pct": round(overall_coverage_pct, 4),
        "per_column": per_column,
    }


def save_outputs(metrics: dict[str, object], threshold: float, output_dir: Path, input_csv: Path) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    status = "PASS" if metrics["overall_coverage_pct"] >= threshold else "FLAG"
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    json_path = output_dir / "coverage_metrics.json"
    md_path = output_dir / "coverage_report.md"

    payload = {
        "generated_at": generated_at,
        "input_csv": str(input_csv),
        "threshold_pct": threshold,
        "status": status,
        **metrics,
    }

    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# Data Coverage Report",
        "",
        f"- Generated at: {generated_at}",
        f"- Input CSV: {input_csv}",
        f"- Rows: {metrics['row_count']}",
        f"- Columns: {metrics['column_count']}",
        f"- Overall missing %: {metrics['overall_missing_pct']}",
        f"- Overall coverage %: {metrics['overall_coverage_pct']}",
        f"- Threshold %: {threshold}",
        f"- Status: {status}",
        "",
        "## Column Coverage",
        "",
        "| Column | Missing % | Coverage % | Missing Count |",
        "|---|---:|---:|---:|",
    ]

    for item in metrics["per_column"]:
        lines.append(
            f"| {item['column']} | {item['missing_pct']} | {item['coverage_pct']} | {item['missing_count']} |"
        )

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return md_path, json_path


def main() -> int:
    args = parse_args()

    input_csv = resolve_path(args.input_csv)
    output_dir = resolve_path(args.output_dir)

    columns, rows = load_rows(input_csv)
    metrics = compute_metrics(columns, rows)
    md_path, json_path = save_outputs(metrics, args.threshold, output_dir, input_csv)

    status = "PASS" if metrics["overall_coverage_pct"] >= args.threshold else "FLAG"
    print("=== COVERAGE SUMMARY ===")
    print(f"Rows: {metrics['row_count']}")
    print(f"Columns: {metrics['column_count']}")
    print(f"Overall missing %: {metrics['overall_missing_pct']}")
    print(f"Overall coverage %: {metrics['overall_coverage_pct']}")
    print(f"Threshold %: {args.threshold}")
    print(f"Status: {status}")
    print(f"Markdown report: {md_path}")
    print(f"Metrics JSON: {json_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
