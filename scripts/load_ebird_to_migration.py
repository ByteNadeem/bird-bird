import argparse
import json
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "backend" / "database" / "migration.db"
DEFAULT_INPUT_GLOB = "backend/data/raw/ebird_*.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load eBird JSON observations into migration.db species and observations tables.",
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Path to migration SQLite DB",
    )
    parser.add_argument(
        "--input-glob",
        action="append",
        default=[DEFAULT_INPUT_GLOB],
        help="Glob pattern for eBird JSON files (repeatable)",
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


def normalize_timestamp(value: str) -> str | None:
    text = value.strip()
    if not text:
        return None

    formats = (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    )
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        except ValueError:
            continue

    return None


def week_start_from_timestamp(ts_text: str) -> str:
    parsed = datetime.strptime(ts_text, "%Y-%m-%d %H:%M:%S.%f")
    week_start = parsed - timedelta(days=parsed.weekday())
    return week_start.strftime("%Y-%m-%d")


def ensure_migration_tables(cur: sqlite3.Cursor) -> None:
    table_names = {
        row[0]
        for row in cur.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    missing = [name for name in ("species", "observations") if name not in table_names]
    if missing:
        raise RuntimeError(
            "Missing migration tables: "
            + ", ".join(missing)
            + ". Run scripts/init_migration_db.py first."
        )


def upsert_species(
    cur: sqlite3.Cursor,
    species_cache: dict[str, int],
    species_code: str,
    scientific_name: str,
    common_name: str,
) -> int:
    cached = species_cache.get(species_code)
    if cached is not None:
        return cached

    cur.execute(
        """
        INSERT INTO species (species_code, scientific_name, common_name)
        VALUES (?, ?, ?)
        ON CONFLICT(species_code) DO UPDATE SET
            scientific_name = excluded.scientific_name,
            common_name = excluded.common_name
        """,
        (species_code, scientific_name, common_name),
    )
    row = cur.execute(
        "SELECT id FROM species WHERE species_code = ?",
        (species_code,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"Failed to upsert species_code={species_code}")

    species_id = int(row[0])
    species_cache[species_code] = species_id
    return species_id


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


def ingest_file(
    cur: sqlite3.Cursor,
    file_path: Path,
    species_cache: dict[str, int],
) -> dict[str, int]:
    payload = json.loads(file_path.read_text(encoding="utf-8"))
    summary = {
        "source_rows": 0,
        "valid_rows": 0,
        "inserted_rows": 0,
        "invalid_rows": 0,
    }

    if not isinstance(payload, list):
        return summary

    # Some eBird files contain only species-code reference lists.
    if payload and all(isinstance(item, str) for item in payload):
        return summary

    for item in payload:
        summary["source_rows"] += 1

        if not isinstance(item, dict):
            summary["invalid_rows"] += 1
            continue

        species_code = str(item.get("speciesCode", "")).strip().lower()
        scientific_name = str(item.get("sciName", "")).strip()
        common_name = str(item.get("comName", "")).strip()
        obs_dt = str(item.get("obsDt", "")).strip()

        timestamp = normalize_timestamp(obs_dt)
        lat = parse_float(item.get("lat"))
        lon = parse_float(item.get("lng"))

        if (
            not species_code
            or not scientific_name
            or timestamp is None
            or lat is None
            or lon is None
        ):
            summary["invalid_rows"] += 1
            continue

        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            summary["invalid_rows"] += 1
            continue

        species_id = upsert_species(
            cur,
            species_cache,
            species_code,
            scientific_name,
            common_name,
        )

        deployment_id = (
            str(item.get("subId", "")).strip()
            or str(item.get("locId", "")).strip()
            or None
        )

        cur.execute(
            """
            INSERT OR IGNORE INTO observations (
                species_id,
                event_timestamp,
                week_start,
                deployment_id,
                latitude,
                longitude,
                source_file
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                species_id,
                timestamp,
                week_start_from_timestamp(timestamp),
                deployment_id,
                lat,
                lon,
                file_path.name,
            ),
        )
        summary["inserted_rows"] += cur.rowcount
        summary["valid_rows"] += 1

    return summary


def main() -> int:
    args = parse_args()
    db_path = resolve_path(args.db_path)
    input_files = resolve_files(args.input_glob)

    if not input_files:
        raise FileNotFoundError("No eBird JSON files matched input glob(s).")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    totals = {
        "files": 0,
        "source_rows": 0,
        "valid_rows": 0,
        "inserted_rows": 0,
        "invalid_rows": 0,
    }

    species_cache: dict[str, int] = {}

    with sqlite3.connect(db_path) as con:
        con.execute("PRAGMA foreign_keys = ON")
        cur = con.cursor()
        ensure_migration_tables(cur)

        for file_path in input_files:
            file_summary = ingest_file(cur, file_path, species_cache)

            # Skip non-observation files (for example, species code reference lists).
            if file_summary["source_rows"] == 0:
                continue

            totals["files"] += 1
            for key in ("source_rows", "valid_rows", "inserted_rows", "invalid_rows"):
                totals[key] += file_summary[key]

            print(
                f"{file_path.name}: source={file_summary['source_rows']} "
                f"valid={file_summary['valid_rows']} inserted={file_summary['inserted_rows']} "
                f"invalid={file_summary['invalid_rows']}"
            )

        con.commit()

        species_count = cur.execute("SELECT COUNT(*) FROM species").fetchone()[0]
        observations_count = cur.execute("SELECT COUNT(*) FROM observations").fetchone()[0]

    print("=== EBIRD LOAD SUMMARY ===")
    print(f"Database: {db_path}")
    print(f"Files processed: {totals['files']}")
    print(f"Source rows: {totals['source_rows']}")
    print(f"Valid rows: {totals['valid_rows']}")
    print(f"Inserted rows: {totals['inserted_rows']}")
    print(f"Invalid rows: {totals['invalid_rows']}")
    print(f"Distinct eBird species touched: {len(species_cache)}")
    print(f"species table count: {species_count}")
    print(f"observations table count: {observations_count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
