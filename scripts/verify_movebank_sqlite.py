import argparse
import sqlite3
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "backend" / "database" / "bird_bird.db"
REQUIRED_TABLES = {"studies", "individuals", "study_taxa", "study_sensors", "individual_sensors"}
SAMPLE_QUERY = """
SELECT id, local_identifier, nick_name, ring_id, sex, taxon_canonical_name,
       timestamp_start, timestamp_end, number_of_events, number_of_deployments,
       sensor_type_ids, study_id
FROM individuals
LIMIT ?
"""
NICKNAME_BACKFILL_WHERE = (
    "(nick_name IS NULL OR nick_name = '') "
    "AND local_identifier IS NOT NULL "
    "AND local_identifier <> ''"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify Movebank SQLite tables, linkage, and nickname backfill impact.",
    )
    parser.add_argument(
        "--db-path",
        default=str(DEFAULT_DB_PATH),
        help="SQLite database path (default: backend/database/bird_bird.db)",
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=5,
        help="Number of individual sample rows to print",
    )
    parser.add_argument(
        "--apply-backfill",
        action="store_true",
        help="Apply nickname backfill update (default is dry-run only)",
    )
    return parser.parse_args()


def resolve_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def verify_required_tables(cur: sqlite3.Cursor) -> tuple[list[str], list[str]]:
    existing = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    return sorted(REQUIRED_TABLES & existing), sorted(REQUIRED_TABLES - existing)


def verify_individuals(cur: sqlite3.Cursor, sample_limit: int) -> int:
    total = cur.execute("SELECT COUNT(*) FROM individuals").fetchone()[0]
    print("=== INDIVIDUALS ===")
    print(f"row count: {total}")
    print(f"sample rows (limit {sample_limit}):")
    for row in cur.execute(SAMPLE_QUERY, (sample_limit,)):
        print(row)
    return total


def verify_study_linkage(cur: sqlite3.Cursor, total_individuals: int) -> None:
    null_count = cur.execute("SELECT COUNT(*) FROM individuals WHERE study_id IS NULL").fetchone()[0]
    print("=== STUDY LINKAGE ===")
    print(f"study_id NULL count: {null_count}")
    print(f"all NULL: {total_individuals == null_count and total_individuals > 0}")


def verify_normalized_counts(cur: sqlite3.Cursor) -> None:
    print("=== NORMALIZED COUNTS ===")
    for table in ("study_taxa", "study_sensors", "individual_sensors"):
        count = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"{table}: {count}")


def dry_run_backfill(cur: sqlite3.Cursor, con: sqlite3.Connection) -> None:
    before = cur.execute(
        f"SELECT COUNT(*) FROM individuals WHERE {NICKNAME_BACKFILL_WHERE}"
    ).fetchone()[0]

    cur.execute("BEGIN")
    cur.execute(
        f"UPDATE individuals SET nick_name = local_identifier WHERE {NICKNAME_BACKFILL_WHERE}"
    )
    changed = cur.rowcount
    after = cur.execute(
        f"SELECT COUNT(*) FROM individuals WHERE {NICKNAME_BACKFILL_WHERE}"
    ).fetchone()[0]
    con.rollback()

    print("=== NICKNAME BACKFILL DRY RUN ===")
    print(f"candidates before: {before}")
    print(f"rows changed: {changed}")
    print(f"candidates after (dry-run): {after}")
    print(f"check ok: {changed == before and after == 0}")


def apply_backfill(cur: sqlite3.Cursor, con: sqlite3.Connection) -> None:
    before = cur.execute(
        f"SELECT COUNT(*) FROM individuals WHERE {NICKNAME_BACKFILL_WHERE}"
    ).fetchone()[0]

    cur.execute(
        f"UPDATE individuals SET nick_name = local_identifier WHERE {NICKNAME_BACKFILL_WHERE}"
    )
    changed = cur.rowcount
    con.commit()

    after = cur.execute(
        f"SELECT COUNT(*) FROM individuals WHERE {NICKNAME_BACKFILL_WHERE}"
    ).fetchone()[0]

    print("=== NICKNAME BACKFILL APPLIED ===")
    print(f"before: {before}")
    print(f"rows updated: {changed}")
    print(f"after: {after}")
    print(f"check ok: {changed == before and after == 0}")


def main() -> int:
    args = parse_args()
    db_path = resolve_path(args.db_path)

    with sqlite3.connect(db_path) as con:
        cur = con.cursor()

        print(f"Database: {db_path}")
        print("=== REQUIRED TABLES ===")
        existing, missing = verify_required_tables(cur)
        print(f"existing required tables: {existing}")
        print(f"missing required tables: {missing}")

        if missing:
            print("Cannot continue verification because required tables are missing.")
            return 1

        total_individuals = verify_individuals(cur, args.sample_limit)
        verify_study_linkage(cur, total_individuals)
        verify_normalized_counts(cur)
        dry_run_backfill(cur, con)

        if args.apply_backfill:
            apply_backfill(cur, con)

    print("Verification complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
