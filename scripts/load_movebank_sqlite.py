import argparse
import fnmatch
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "backend" / "database" / "bird_bird.db"
DEFAULT_CORE_SCHEMA_PATH = PROJECT_ROOT / "backend" / "data" / "schema" / "movebank_core.sql"
DEFAULT_NORMALIZED_SCHEMA_PATH = PROJECT_ROOT / "backend" / "data" / "schema" / "movebank_normalized.sql"
DEFAULT_STUDY_ID_MAP_PATH = PROJECT_ROOT / "backend" / "data" / "schema" / "movebank_study_filename_map.json"
DEFAULT_STUDY_GLOB = "backend/data/raw/movebank_studies*.json"
DEFAULT_INDIVIDUAL_GLOB = "backend/data/raw/movebank_individuals*.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Movebank raw JSON into SQLite core and normalized schema.",
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="SQLite database path (default: backend/database/bird_bird.db)",
    )
    parser.add_argument(
        "--core-schema-path",
        default=str(DEFAULT_CORE_SCHEMA_PATH),
        help="SQL file for core tables",
    )
    parser.add_argument(
        "--normalized-schema-path",
        default=str(DEFAULT_NORMALIZED_SCHEMA_PATH),
        help="SQL file for normalized join tables",
    )
    parser.add_argument(
        "--study-glob",
        action="append",
        default=[DEFAULT_STUDY_GLOB],
        help="Glob for study JSON files; repeatable",
    )
    parser.add_argument(
        "--individual-glob",
        action="append",
        default=[DEFAULT_INDIVIDUAL_GLOB],
        help="Glob for individual JSON files; repeatable",
    )
    parser.add_argument(
        "--default-study-id",
        type=int,
        default=None,
        help="Optional study_id to apply to individuals that do not have study_id",
    )
    parser.add_argument(
        "--study-id-map-path",
        default=str(DEFAULT_STUDY_ID_MAP_PATH),
        help="JSON file mapping individual filename patterns to study IDs",
    )
    parser.add_argument(
        "--require-mapped-study-id",
        action="store_true",
        help="Fail if a study_id cannot be resolved for an individual file",
    )
    parser.add_argument(
        "--skip-normalized",
        action="store_true",
        help="Skip normalized migration after core data load",
    )
    parser.add_argument(
        "--truncate-core",
        action="store_true",
        help="Delete all rows from core tables before loading",
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


def load_json_records(file_path: Path) -> list[dict[str, object]]:
    payload = json.loads(file_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Expected JSON array in {file_path}")

    records: list[dict[str, object]] = []
    for item in payload:
        if isinstance(item, dict):
            records.append(item)
    return records


def load_study_id_map(map_path: Path) -> list[tuple[str, int | None]]:
    if not map_path.exists():
        return []

    payload = json.loads(map_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object in mapping file: {map_path}")

    mapping_rules: list[tuple[str, int | None]] = []
    for pattern, value in payload.items():
        if not isinstance(pattern, str) or not pattern.strip():
            raise ValueError(f"Invalid filename pattern in mapping file: {pattern!r}")

        if value is None:
            study_id = None
        elif isinstance(value, int):
            study_id = value
        elif isinstance(value, str) and value.strip():
            try:
                study_id = int(value.strip())
            except ValueError as exc:
                raise ValueError(
                    f"Invalid study_id value for pattern {pattern!r}: {value!r}"
                ) from exc
        else:
            raise ValueError(f"Unsupported study_id type for pattern {pattern!r}: {type(value).__name__}")

        mapping_rules.append((pattern, study_id))

    return mapping_rules


def resolve_file_study_id(
    file_path: Path,
    mapping_rules: list[tuple[str, int | None]],
    default_study_id: int | None,
) -> tuple[int | None, str]:
    file_name = file_path.name
    for pattern, study_id in mapping_rules:
        if fnmatch.fnmatch(file_name, pattern):
            return study_id, f"map:{pattern}"

    if default_study_id is not None:
        return default_study_id, "default-study-id"

    return None, "unmapped"


def to_none_if_empty(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def to_int(value: object, default: int | None = None) -> int | None:
    text = to_none_if_empty(value)
    if text is None:
        return default
    try:
        return int(text)
    except ValueError:
        return default


def to_float(value: object) -> float | None:
    text = to_none_if_empty(value)
    if text is None:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def normalize_csv(value: object, *, lowercase: bool) -> str:
    text = to_none_if_empty(value)
    if text is None:
        return ""

    tokens: list[str] = []
    seen: set[str] = set()
    for raw_token in text.split(","):
        token = raw_token.strip()
        if not token:
            continue
        if lowercase:
            token = token.lower()
        if token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return ",".join(tokens)


def normalize_timestamp(value: object) -> str | None:
    text = to_none_if_empty(value)
    if text is None:
        return None

    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        except ValueError:
            continue

    return text


def normalize_sex(value: object) -> str | None:
    text = to_none_if_empty(value)
    if text is None:
        return None
    code = text.lower()
    if code in {"m", "f", "u"}:
        return code
    return None


def apply_sql_file(connection: sqlite3.Connection, sql_file: Path) -> None:
    if not sql_file.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")
    connection.executescript(sql_file.read_text(encoding="utf-8"))


def study_exists(connection: sqlite3.Connection, study_id: int) -> bool:
    row = connection.execute("SELECT 1 FROM studies WHERE id = ?", (study_id,)).fetchone()
    return row is not None


def upsert_studies(connection: sqlite3.Connection, records: list[dict[str, object]]) -> int:
    rows: list[tuple[object, ...]] = []
    for record in records:
        study_id = to_int(record.get("id"))
        if study_id is None:
            continue

        rows.append(
            (
                study_id,
                to_float(record.get("main_location_lat")),
                to_float(record.get("main_location_long")),
                normalize_csv(record.get("taxon_ids"), lowercase=False),
                normalize_csv(record.get("sensor_type_ids"), lowercase=True),
                to_none_if_empty(record.get("contact_person_name")),
            )
        )

    if not rows:
        return 0

    connection.executemany(
        """
        INSERT INTO studies (
            id,
            main_location_lat,
            main_location_long,
            taxon_ids,
            sensor_type_ids,
            contact_person_name
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            main_location_lat = excluded.main_location_lat,
            main_location_long = excluded.main_location_long,
            taxon_ids = excluded.taxon_ids,
            sensor_type_ids = excluded.sensor_type_ids,
            contact_person_name = excluded.contact_person_name
        """,
        rows,
    )
    return len(rows)


def upsert_individuals(
    connection: sqlite3.Connection,
    records: list[dict[str, object]],
    default_study_id: int | None,
) -> int:
    rows: list[tuple[object, ...]] = []
    for record in records:
        individual_id = to_int(record.get("id"))
        if individual_id is None:
            continue

        study_id = to_int(record.get("study_id"), default=default_study_id)

        rows.append(
            (
                individual_id,
                study_id,
                to_none_if_empty(record.get("local_identifier")),
                to_none_if_empty(record.get("nick_name")),
                to_none_if_empty(record.get("ring_id")),
                normalize_sex(record.get("sex")),
                to_none_if_empty(record.get("taxon_canonical_name")),
                normalize_timestamp(record.get("timestamp_start")),
                normalize_timestamp(record.get("timestamp_end")),
                to_int(record.get("number_of_events"), default=0) or 0,
                to_int(record.get("number_of_deployments"), default=0) or 0,
                normalize_csv(record.get("sensor_type_ids"), lowercase=True),
            )
        )

    if not rows:
        return 0

    connection.executemany(
        """
        INSERT INTO individuals (
            id,
            study_id,
            local_identifier,
            nick_name,
            ring_id,
            sex,
            taxon_canonical_name,
            timestamp_start,
            timestamp_end,
            number_of_events,
            number_of_deployments,
            sensor_type_ids
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            study_id = COALESCE(excluded.study_id, individuals.study_id),
            local_identifier = excluded.local_identifier,
            nick_name = excluded.nick_name,
            ring_id = excluded.ring_id,
            sex = excluded.sex,
            taxon_canonical_name = excluded.taxon_canonical_name,
            timestamp_start = excluded.timestamp_start,
            timestamp_end = excluded.timestamp_end,
            number_of_events = excluded.number_of_events,
            number_of_deployments = excluded.number_of_deployments,
            sensor_type_ids = excluded.sensor_type_ids
        """,
        rows,
    )
    return len(rows)


def load_studies(connection: sqlite3.Connection, study_files: list[Path]) -> int:
    loaded = 0
    for file_path in study_files:
        records = load_json_records(file_path)
        loaded += upsert_studies(connection, records)
    return loaded


def load_individuals(
    connection: sqlite3.Connection,
    individual_files: list[Path],
    default_study_id: int | None,
    mapping_rules: list[tuple[str, int | None]],
    require_mapped_study_id: bool,
) -> int:
    loaded = 0
    for file_path in individual_files:
        file_study_id, source = resolve_file_study_id(
            file_path,
            mapping_rules,
            default_study_id,
        )

        if file_study_id is None and require_mapped_study_id:
            raise ValueError(
                f"No study_id resolved for {file_path.name}. "
                "Add a mapping entry or pass --default-study-id."
            )

        if file_study_id is not None and not study_exists(connection, file_study_id):
            raise ValueError(
                f"Resolved study_id={file_study_id} for {file_path.name}, "
                "but that study does not exist in loaded studies table."
            )

        print(f"Resolved {file_path.name} -> study_id={file_study_id} ({source})")

        records = load_json_records(file_path)
        loaded += upsert_individuals(connection, records, file_study_id)
    return loaded


def truncate_core_tables(connection: sqlite3.Connection) -> None:
    connection.execute("DELETE FROM individuals")
    connection.execute("DELETE FROM studies")


def main() -> int:
    args = parse_args()

    db_path = resolve_path(args.db_path)
    core_schema_path = resolve_path(args.core_schema_path)
    normalized_schema_path = resolve_path(args.normalized_schema_path)
    study_id_map_path = resolve_path(args.study_id_map_path)

    study_files = resolve_files(args.study_glob)
    individual_files = resolve_files(args.individual_glob)
    mapping_rules = load_study_id_map(study_id_map_path)

    if not study_files and not individual_files:
        raise FileNotFoundError("No study or individual JSON files matched the provided globs.")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        apply_sql_file(connection, core_schema_path)

        if args.truncate_core:
            truncate_core_tables(connection)

        studies_loaded = load_studies(connection, study_files)
        individuals_loaded = load_individuals(
            connection,
            individual_files,
            args.default_study_id,
            mapping_rules,
            args.require_mapped_study_id,
        )

        if not args.skip_normalized:
            apply_sql_file(connection, normalized_schema_path)

        connection.commit()

    print(f"Database: {db_path}")
    print(f"Study files processed: {len(study_files)}")
    print(f"Individual files processed: {len(individual_files)}")
    print(f"Studies upserted: {studies_loaded}")
    print(f"Individuals upserted: {individuals_loaded}")
    print(f"Study-id map rules loaded: {len(mapping_rules)} ({study_id_map_path})")
    if args.skip_normalized:
        print("Normalized migration: skipped")
    else:
        print(f"Normalized migration: applied ({normalized_schema_path})")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
