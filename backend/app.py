import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from werkzeug.exceptions import HTTPException
from flask import Flask, g, jsonify, request


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "backend" / "database" / "migration.db"


class ApiError(Exception):
    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def resolve_db_path() -> Path:
    env_path = os.getenv("MIGRATION_DB_PATH", "").strip()
    if env_path:
        candidate = Path(env_path)
        if not candidate.is_absolute():
            candidate = PROJECT_ROOT / candidate
        return candidate
    return DEFAULT_DB_PATH


def get_db() -> sqlite3.Connection:
    if "db_conn" not in g:
        db_path = resolve_db_path()
        if not db_path.exists():
            raise ApiError(f"Migration DB not found: {db_path}", status_code=500)

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        g.db_conn = conn

    return g.db_conn


def parse_int_arg(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = request.args.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ApiError(f"Query parameter '{name}' must be an integer", status_code=400) from exc
    if value < minimum or value > maximum:
        raise ApiError(
            f"Query parameter '{name}' must be between {minimum} and {maximum}",
            status_code=400,
        )
    return value


def parse_date_arg(name: str) -> str | None:
    raw = request.args.get(name)
    if raw is None or raw.strip() == "":
        return None
    value = raw.strip()
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise ApiError(
            f"Query parameter '{name}' must be in YYYY-MM-DD format",
            status_code=400,
        ) from exc
    return value


def create_app() -> Flask:
    app = Flask(__name__)

    @app.teardown_appcontext
    def close_db_connection(_: object) -> None:
        conn = g.pop("db_conn", None)
        if conn is not None:
            conn.close()

    @app.errorhandler(ApiError)
    def handle_api_error(error: ApiError):
        return jsonify({"error": error.message, "status": error.status_code}), error.status_code

    @app.errorhandler(HTTPException)
    def handle_http_error(error: HTTPException):
        message = error.description if error.description else error.name
        status = error.code if error.code is not None else 500
        return jsonify({"error": message, "status": status}), status

    @app.errorhandler(sqlite3.Error)
    def handle_db_error(_: sqlite3.Error):
        return jsonify({"error": "database error", "status": 500}), 500

    @app.errorhandler(Exception)
    def handle_unexpected_error(_: Exception):
        return jsonify({"error": "internal server error", "status": 500}), 500

    @app.get("/")
    def api_index():
        return jsonify(
            {
                "name": "bird-bird migration api",
                "endpoints": [
                    "/health",
                    "/api/species",
                    "/api/migration/",
                ],
            }
        )

    @app.get("/api/species")
    def get_species():
        start = time.perf_counter()
        limit = parse_int_arg("limit", default=200, minimum=1, maximum=2000)

        rows = get_db().execute(
            """
            SELECT
                s.id,
                s.species_code,
                s.scientific_name,
                s.common_name,
                COUNT(o.id) AS observation_count
            FROM species s
            LEFT JOIN observations o ON o.species_id = s.id
            GROUP BY s.id, s.species_code, s.scientific_name, s.common_name
            ORDER BY s.species_code
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        elapsed_ms = round((time.perf_counter() - start) * 1000, 3)
        payload = [
            {
                "id": int(row["id"]),
                "species_code": row["species_code"],
                "scientific_name": row["scientific_name"],
                "common_name": row["common_name"],
                "observation_count": int(row["observation_count"]),
            }
            for row in rows
        ]

        return jsonify(
            {
                "data": payload,
                "meta": {
                    "count": len(payload),
                    "limit": limit,
                    "query_ms": elapsed_ms,
                },
            }
        )

    @app.get("/api/migration/")
    def get_migration_weekly():
        start = time.perf_counter()

        limit = parse_int_arg("limit", default=260, minimum=1, maximum=5000)
        species_code = request.args.get("species_code", "").strip() or None
        from_date = parse_date_arg("from")
        to_date = parse_date_arg("to")

        filters: list[str] = []
        params: list[object] = []

        if species_code is not None:
            filters.append("s.species_code = ?")
            params.append(species_code)

        if from_date is not None:
            filters.append("o.week_start >= ?")
            params.append(from_date)

        if to_date is not None:
            filters.append("o.week_start <= ?")
            params.append(to_date)

        where_clause = ""
        if filters:
            where_clause = "WHERE " + " AND ".join(filters)

        params.append(limit)

        rows = get_db().execute(
            f"""
            SELECT
                o.week_start,
                s.species_code,
                COUNT(o.id) AS observation_count
            FROM observations o
            JOIN species s ON s.id = o.species_id
            {where_clause}
            GROUP BY o.week_start, s.species_code
            ORDER BY o.week_start, s.species_code
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()

        elapsed_ms = round((time.perf_counter() - start) * 1000, 3)

        payload = [
            {
                "week_start": row["week_start"],
                "species_code": row["species_code"],
                "observation_count": int(row["observation_count"]),
            }
            for row in rows
        ]

        return jsonify(
            {
                "data": payload,
                "meta": {
                    "count": len(payload),
                    "limit": limit,
                    "species_code": species_code,
                    "from": from_date,
                    "to": to_date,
                    "query_ms": elapsed_ms,
                },
            }
        )

    @app.get("/health")
    def health_check():
        return jsonify({"status": "ok"})

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
