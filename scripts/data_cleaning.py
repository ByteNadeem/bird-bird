import argparse
import csv
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_GLOB = "backend/data/raw/movebank_events*.json"
DEFAULT_OUTPUT_CSV = PROJECT_ROOT / "backend" / "data" / "clean" / "movebank_events_cleaned.csv"
DEFAULT_DB_PATH = PROJECT_ROOT / "backend" / "database" / "bird_bird.db"
DEFAULT_TABLE_NAME = "cleaned_observations"
TIMESTAMP_OUT_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean and standardize raw Movebank event data for analysis.",
    )
    parser.add_argument(
        "--input-glob",
        action="append",
        default=[DEFAULT_INPUT_GLOB],
        help="Glob pattern for raw event JSON files (repeatable)",
    )
    parser.add_argument(
        "--output-csv",
        default=str(DEFAULT_OUTPUT_CSV),
        help="Path for cleaned CSV output",
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="SQLite database path for cleaned output",
    )
    parser.add_argument(
        "--table-name",
        default=DEFAULT_TABLE_NAME,
        help="SQLite table name for cleaned observations",
    )
    parser.add_argument(
        "--skip-csv",
        action="store_true",
        help="Skip saving CSV output",
    )
    parser.add_argument(
        "--skip-sqlite",
        action="store_true",
        help="Skip saving SQLite output",
    )
    parser.add_argument(
        "--replace-table",
        action="store_true",
        help="Drop and recreate target SQLite table before insert",
    )
    return parser.parse_args()


def resolve_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def resolve_files(patterns: Iterable[str]) -> list[Path]:
    files: set[Path] = set()
    for pattern in patterns:
        files.update(PROJECT_ROOT.glob(pattern))
    return sorted(path for path in files if path.is_file())


def parse_timestamp(value: object) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    formats = (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
    )
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.strftime(TIMESTAMP_OUT_FORMAT)[:-3]
        except ValueError:
            continue

    return None


def parse_float(value: object) -> float | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    try:
        return float(text)
    except ValueError:
        return None


def normalize_deployment_id(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def extract_row(item: object) -> tuple[object, object, object, object] | None:
    if isinstance(item, (list, tuple)) and len(item) >= 4:
        return item[0], item[1], item[2], item[3]

    if isinstance(item, dict):
        timestamp = item.get("timestamp")
        deployment_id = item.get("deployment_id", item.get("tag_local_identifier"))
        lat = item.get("location_lat", item.get("lat"))
        lon = item.get("location_long", item.get("lng", item.get("lon")))
        return timestamp, deployment_id, lat, lon

    return None


def clean_records(file_paths: list[Path]) -> tuple[list[dict[str, object]], dict[str, int]]:
    stats = {
        "source_rows": 0,
        "valid_rows": 0,
        "invalid_shape": 0,
        "invalid_timestamp": 0,
        "invalid_coordinates": 0,
        "duplicates_removed": 0,
    }

    seen: set[tuple[str, str, float, float]] = set()
    cleaned: list[dict[str, object]] = []

    for file_path in file_paths:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            continue

        for item in payload:
            stats["source_rows"] += 1

            extracted = extract_row(item)
            if extracted is None:
                stats["invalid_shape"] += 1
                continue

            raw_ts, raw_dep, raw_lat, raw_lon = extracted

            ts = parse_timestamp(raw_ts)
            if ts is None:
                stats["invalid_timestamp"] += 1
                continue

            lat = parse_float(raw_lat)
            lon = parse_float(raw_lon)
            if lat is None or lon is None:
                stats["invalid_coordinates"] += 1
                continue

            if not (-90 <= lat <= 90 and -180 <= lon <= 180):
                stats["invalid_coordinates"] += 1
                continue

            dep = normalize_deployment_id(raw_dep)
            key = (ts, dep, round(lat, 6), round(lon, 6))
            if key in seen:
                stats["duplicates_removed"] += 1
                continue

            seen.add(key)
            cleaned.append(
                {
                    "event_timestamp": ts,
                    "deployment_id": dep,
                    "latitude": round(lat, 6),
                    "longitude": round(lon, 6),
                    "source_file": file_path.name,
                }
            )
            stats["valid_rows"] += 1

    return cleaned, stats


def save_csv(rows: list[dict[str, object]], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["event_timestamp", "deployment_id", "latitude", "longitude", "source_file"]

    with output_csv.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def save_sqlite(rows: list[dict[str, object]], db_path: Path, table_name: str, replace_table: bool) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    table_sql = quote_identifier(table_name)

    with sqlite3.connect(db_path) as con:
        cur = con.cursor()

        if replace_table:
            cur.execute(f"DROP TABLE IF EXISTS {table_sql}")

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_sql} (
                id INTEGER PRIMARY KEY,
                event_timestamp TEXT NOT NULL,
                deployment_id TEXT,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                source_file TEXT
            )
            """
        )

        if replace_table:
            cur.execute(f"DELETE FROM {table_sql}")

        cur.executemany(
            f"""
            INSERT INTO {table_sql} (
                event_timestamp,
                deployment_id,
                latitude,
                longitude,
                source_file
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    row["event_timestamp"],
                    row["deployment_id"],
                    row["latitude"],
                    row["longitude"],
                    row["source_file"],
                )
                for row in rows
            ],
        )

        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp ON {table_sql}(event_timestamp)"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_deployment ON {table_sql}(deployment_id)"
        )
        con.commit()


def main() -> int:
    args = parse_args()

    input_files = resolve_files(args.input_glob)
    if not input_files:
        raise FileNotFoundError("No raw files matched input glob(s).")

    output_csv = resolve_path(args.output_csv)
    db_path = resolve_path(args.db_path)

    cleaned_rows, stats = clean_records(input_files)

    if not args.skip_csv:
        save_csv(cleaned_rows, output_csv)

    if not args.skip_sqlite:
        save_sqlite(cleaned_rows, db_path, args.table_name, args.replace_table)

    print("=== DATA CLEANING SUMMARY ===")
    print(f"Input files: {len(input_files)}")
    print(f"Source rows: {stats['source_rows']}")
    print(f"Valid rows: {stats['valid_rows']}")
    print(f"Invalid shape rows: {stats['invalid_shape']}")
    print(f"Invalid timestamps: {stats['invalid_timestamp']}")
    print(f"Invalid coordinates: {stats['invalid_coordinates']}")
    print(f"Duplicates removed: {stats['duplicates_removed']}")

    if not args.skip_csv:
        print(f"Cleaned CSV: {output_csv}")
    if not args.skip_sqlite:
        print(f"Cleaned SQLite table: {args.table_name} in {db_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
