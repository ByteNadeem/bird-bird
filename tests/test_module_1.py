from pathlib import Path
import sqlite3
import uuid

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_DB = PROJECT_ROOT / "backend" / "database" / "migration.db"


def _get_counts(con: sqlite3.Connection) -> tuple[int, int]:
    cur = con.cursor()
    species_count = cur.execute("SELECT COUNT(*) FROM species").fetchone()[0]
    obs_count = cur.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    return species_count, obs_count


def test_migration_fk_insert_query_rollback():
    assert MIGRATION_DB.exists(), f"Missing DB: {MIGRATION_DB}"

    test_tag = uuid.uuid4().hex[:12]
    species_code = f"__test_species_{test_tag}__"
    deployment_id = f"__test_dep_{test_tag}__"

    con = sqlite3.connect(MIGRATION_DB)
    try:
        con.execute("PRAGMA foreign_keys = ON")
        before_species, before_obs = _get_counts(con)

        con.execute("BEGIN")
        cur = con.cursor()

        # FK enforcement should reject non-existent species_id.
        with pytest.raises(sqlite3.IntegrityError):
            cur.execute(
                """
                INSERT INTO observations (
                    species_id, event_timestamp, week_start, deployment_id, latitude, longitude, source_file
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    -99999999,
                    "2026-01-01 00:00:00.000",
                    "2025-12-29",
                    f"fk_fail_{test_tag}",
                    0.0,
                    0.0,
                    "pytest-fk-check",
                ),
            )

        # Valid species insert.
        cur.execute(
            """
            INSERT INTO species (species_code, scientific_name, common_name)
            VALUES (?, ?, ?)
            """,
            (species_code, "Pytest species", "Pytest species"),
        )
        species_id = cur.execute(
            "SELECT id FROM species WHERE species_code = ?",
            (species_code,),
        ).fetchone()[0]

        # Valid observation insert linked to that species.
        cur.execute(
            """
            INSERT INTO observations (
                species_id, event_timestamp, week_start, deployment_id, latitude, longitude, source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                species_id,
                "2026-01-01 01:02:03.456",
                "2025-12-29",
                deployment_id,
                10.0,
                20.0,
                "pytest-insert-query",
            ),
        )

        row = cur.execute(
            """
            SELECT s.species_code, o.deployment_id
            FROM observations o
            JOIN species s ON s.id = o.species_id
            WHERE s.species_code = ? AND o.deployment_id = ?
            """,
            (species_code, deployment_id),
        ).fetchone()

        assert row is not None
        assert row[0] == species_code
        assert row[1] == deployment_id

        # Non-destructive guarantee.
        con.rollback()

        after_species, after_obs = _get_counts(con)
        assert (after_species, after_obs) == (before_species, before_obs)

    finally:
        con.close()

    # Verify persisted state unchanged from a fresh connection.
    con2 = sqlite3.connect(MIGRATION_DB)
    try:
        con2.execute("PRAGMA foreign_keys = ON")
        species_left = con2.execute(
            "SELECT COUNT(*) FROM species WHERE species_code = ?",
            (species_code,),
        ).fetchone()[0]
        obs_left = con2.execute(
            "SELECT COUNT(*) FROM observations WHERE deployment_id = ?",
            (deployment_id,),
        ).fetchone()[0]
        assert species_left == 0
        assert obs_left == 0
    finally:
        con2.close()