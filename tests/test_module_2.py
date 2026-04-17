from pathlib import Path
import sqlite3


PROJECT_ROOT = Path(__file__).resolve().parents[1]
BIRD_DB = PROJECT_ROOT / "backend" / "database" / "bird_bird.db"


def test_cleaned_observations_quality():
    assert BIRD_DB.exists(), f"Missing DB: {BIRD_DB}"

    con = sqlite3.connect(BIRD_DB)
    try:
        cur = con.cursor()

        exists = cur.execute(
            """
            SELECT COUNT(*)
            FROM sqlite_master
            WHERE type = 'table' AND name = 'cleaned_observations'
            """
        ).fetchone()[0]
        assert exists == 1, "cleaned_observations table missing"

        row_count = cur.execute(
            "SELECT COUNT(*) FROM cleaned_observations"
        ).fetchone()[0]
        assert row_count > 0, "cleaned_observations is empty"

        dup_count = cur.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT 1
                FROM cleaned_observations
                GROUP BY event_timestamp, deployment_id, latitude, longitude
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
        assert dup_count == 0, f"Duplicate composite keys found: {dup_count}"

        coord_bad = cur.execute(
            """
            SELECT COUNT(*)
            FROM cleaned_observations
            WHERE latitude IS NULL
               OR longitude IS NULL
               OR latitude NOT BETWEEN -90 AND 90
               OR longitude NOT BETWEEN -180 AND 180
            """
        ).fetchone()[0]
        assert coord_bad == 0, f"Out-of-range or null coordinates found: {coord_bad}"

        ts_bad = cur.execute(
            """
            SELECT COUNT(*)
            FROM cleaned_observations
            WHERE event_timestamp IS NULL
               OR LENGTH(event_timestamp) != 23
               OR event_timestamp NOT GLOB '????-??-?? ??:??:??.???'
            """
        ).fetchone()[0]
        assert ts_bad == 0, f"Timestamp format anomalies found: {ts_bad}"

    finally:
        con.close()