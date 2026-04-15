import argparse
import csv
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "backend" / "database" / "migration.db"
DEFAULT_SCHEMA_PATH = PROJECT_ROOT / "backend" / "data" / "schema" / "migration_schema.sql"
DEFAULT_CLEANED_CSV = PROJECT_ROOT / "backend" / "data" / "clean" / "movebank_events_cleaned.csv"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialize migration SQLite DB with species and observations tables.",
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="Path to migration SQLite DB",
    )
    parser.add_argument(
        "--schema-path",
        default=str(DEFAULT_SCHEMA_PATH),
        help="Path to migration schema SQL file",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Drop species and observations tables before applying schema",
    )
    parser.add_argument(
        "--cleaned-csv",
        default=str(DEFAULT_CLEANED_CSV),
        help="Cleaned CSV path used when --seed-from-cleaned is enabled",
    )
    parser.add_argument(
        "--seed-from-cleaned",
        action="store_true",
        help="Load cleaned CSV rows into observations with temporary 'unknown' species",
    )
    return parser.parse_args()


def resolve_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def apply_schema(con: sqlite3.Connection, schema_path: Path, replace: bool) -> None:
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys = ON")

    if replace:
        cur.execute("DROP TABLE IF EXISTS observations")
        cur.execute("DROP TABLE IF EXISTS species")

    sql_text = schema_path.read_text(encoding="utf-8")
    con.executescript(sql_text)
    con.commit()


def get_week_start(ts_text: str) -> str:
    parsed = datetime.strptime(ts_text, "%Y-%m-%d %H:%M:%S.%f")
    week_start = parsed - timedelta(days=parsed.weekday())
    return week_start.strftime("%Y-%m-%d")


def ensure_unknown_species(cur: sqlite3.Cursor) -> int:
    cur.execute(
        """
        INSERT INTO species (species_code, scientific_name, common_name)
        VALUES ('unknown', 'Unknown species', 'Unknown')
        ON CONFLICT(species_code) DO NOTHING
        """
    )
    row = cur.execute("SELECT id FROM species WHERE species_code = 'unknown'").fetchone()
    if row is None:
        raise RuntimeError("Failed to ensure unknown species row")
    return int(row[0])


def seed_from_cleaned_csv(cur: sqlite3.Cursor, cleaned_csv_path: Path) -> int:
    if not cleaned_csv_path.exists():
        raise FileNotFoundError(f"Cleaned CSV not found: {cleaned_csv_path}")

    species_id = ensure_unknown_species(cur)

    inserted = 0
    with cleaned_csv_path.open("r", encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            event_timestamp = (row.get("event_timestamp") or "").strip()
            lat_text = (row.get("latitude") or "").strip()
            lon_text = (row.get("longitude") or "").strip()

            if not event_timestamp or not lat_text or not lon_text:
                continue

            try:
                latitude = float(lat_text)
                longitude = float(lon_text)
                week_start = get_week_start(event_timestamp)
            except ValueError:
                continue

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
                    event_timestamp,
                    week_start,
                    (row.get("deployment_id") or "").strip() or None,
                    latitude,
                    longitude,
                    (row.get("source_file") or "").strip() or None,
                ),
            )
            inserted += cur.rowcount

    return inserted


def validate_foreign_key(cur: sqlite3.Cursor) -> bool:
    try:
        cur.execute(
            """
            INSERT INTO observations (
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
                -999999,
                "2026-01-01 00:00:00.000",
                "2025-12-29",
                "fk-test",
                0.0,
                0.0,
                "fk-test",
            ),
        )
    except sqlite3.IntegrityError:
        return True
    return False


def test_insert_query(cur: sqlite3.Cursor) -> tuple[bool, tuple[int, str, str, str] | None]:
    test_code = "__test_species__"

    cur.execute(
        """
        INSERT OR REPLACE INTO species (species_code, scientific_name, common_name)
        VALUES (?, ?, ?)
        """,
        (test_code, "Test species", "Test species"),
    )
    species_id = cur.execute("SELECT id FROM species WHERE species_code = ?", (test_code,)).fetchone()[0]

    cur.execute(
        """
        INSERT OR REPLACE INTO observations (
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
            "2026-01-01 00:00:00.000",
            "2025-12-29",
            "test-deployment",
            10.0,
            20.0,
            "test-source",
        ),
    )

    result = cur.execute(
        """
        SELECT o.id, s.species_code, o.event_timestamp, o.deployment_id
        FROM observations o
        JOIN species s ON s.id = o.species_id
        WHERE s.species_code = ?
        ORDER BY o.id DESC
        LIMIT 1
        """,
        (test_code,),
    ).fetchone()

    cur.execute("DELETE FROM observations WHERE species_id = ?", (species_id,))
    cur.execute("DELETE FROM species WHERE id = ?", (species_id,))

    if result is None:
        return False, None

    return True, (int(result[0]), str(result[1]), str(result[2]), str(result[3]))


def main() -> int:
    args = parse_args()

    db_path = resolve_path(args.db_path)
    schema_path = resolve_path(args.schema_path)
    cleaned_csv_path = resolve_path(args.cleaned_csv)

    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as con:
        con.execute("PRAGMA foreign_keys = ON")
        apply_schema(con, schema_path, args.replace)
        cur = con.cursor()

        seeded_count = 0
        if args.seed_from_cleaned:
            seeded_count = seed_from_cleaned_csv(cur, cleaned_csv_path)

        fk_valid = validate_foreign_key(cur)
        insert_ok, sample_row = test_insert_query(cur)
        con.commit()

        species_count = cur.execute("SELECT COUNT(*) FROM species").fetchone()[0]
        observation_count = cur.execute("SELECT COUNT(*) FROM observations").fetchone()[0]

    print("=== MIGRATION DB INIT SUMMARY ===")
    print(f"Database: {db_path}")
    print(f"Schema: {schema_path}")
    print(f"species count: {species_count}")
    print(f"observations count: {observation_count}")
    print(f"foreign key validation: {'PASS' if fk_valid else 'FAIL'}")
    print(f"test insert/query: {'PASS' if insert_ok else 'FAIL'}")
    if sample_row is not None:
        print(f"test query sample row: {sample_row}")
    if args.seed_from_cleaned:
        print(f"seeded observations from cleaned CSV: {seeded_count}")
        print(f"cleaned CSV source: {cleaned_csv_path}")

    if not fk_valid or not insert_ok:
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
