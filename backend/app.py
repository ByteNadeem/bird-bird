import os
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from werkzeug.exceptions import HTTPException
from flask import Flask, g, jsonify, request, send_from_directory


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "backend" / "database" / "migration.db"
FRONTEND_DIR = PROJECT_ROOT / "frontend"
API_CACHE_TTL_SECONDS = int(os.getenv("API_CACHE_TTL_SECONDS", "30"))

_api_cache: dict[str, tuple[float, dict[str, object]]] = {}


class ApiError(Exception):
    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def _build_cache_key(name: str, parts: tuple[object, ...]) -> str:
    joined = "|".join(str(part) for part in parts)
    return f"{name}|{joined}"


def _get_cached_payload(cache_key: str) -> dict[str, object] | None:
    now = time.time()
    cached = _api_cache.get(cache_key)
    if cached is None:
        return None

    expires_at, payload = cached
    if expires_at <= now:
        _api_cache.pop(cache_key, None)
        return None

    return payload


def _set_cached_payload(cache_key: str, payload: dict[str, object]) -> None:
    _api_cache[cache_key] = (time.time() + API_CACHE_TTL_SECONDS, payload)


def _with_cache_meta(payload: dict[str, object], cache_hit: bool) -> dict[str, object]:
    meta = payload.get("meta")
    if not isinstance(meta, dict):
        return payload

    output = dict(payload)
    output["meta"] = {**meta, "cache_hit": cache_hit, "cache_ttl_s": API_CACHE_TTL_SECONDS}
    return output


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
                    "/ui",
                    "/health",
                    "/api/species",
                    "/api/migration/",
                    "/api/routes/",
                    "/api/visualization/",
                ],
            }
        )

    @app.get("/ui")
    def ui_index():
        index_path = FRONTEND_DIR / "index.html"
        if not index_path.exists():
            raise ApiError("Frontend index not found", status_code=404)
        return send_from_directory(FRONTEND_DIR, "index.html")

    @app.get("/ui/<path:asset_path>")
    def ui_assets(asset_path: str):
        return send_from_directory(FRONTEND_DIR, asset_path)

    @app.get("/api/species")
    def get_species():
        start = time.perf_counter()
        limit = parse_int_arg("limit", default=200, minimum=1, maximum=2000)

        cache_key = _build_cache_key("species", (limit,))
        cached = _get_cached_payload(cache_key)
        if cached is not None:
            return jsonify(_with_cache_meta(cached, cache_hit=True))

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

        response_payload = {
            "data": payload,
            "meta": {
                "count": len(payload),
                "limit": limit,
                "query_ms": elapsed_ms,
            },
        }
        _set_cached_payload(cache_key, response_payload)
        return jsonify(_with_cache_meta(response_payload, cache_hit=False))

    @app.get("/api/migration/")
    def get_migration_weekly():
        start = time.perf_counter()

        limit = parse_int_arg("limit", default=260, minimum=1, maximum=5000)
        species_code = request.args.get("species_code", "").strip() or None
        from_date = parse_date_arg("from")
        to_date = parse_date_arg("to")

        cache_key = _build_cache_key("migration", (limit, species_code, from_date, to_date))
        cached = _get_cached_payload(cache_key)
        if cached is not None:
            return jsonify(_with_cache_meta(cached, cache_hit=True))

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

        response_payload = {
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
        _set_cached_payload(cache_key, response_payload)
        return jsonify(_with_cache_meta(response_payload, cache_hit=False))

    @app.get("/api/routes/")
    def get_migration_routes():
        start = time.perf_counter()

        limit = parse_int_arg("limit", default=2500, minimum=1, maximum=10000)
        species_code = request.args.get("species_code", "").strip() or None
        from_date = parse_date_arg("from")
        to_date = parse_date_arg("to")

        cache_key = _build_cache_key("routes", (limit, species_code, from_date, to_date))
        cached = _get_cached_payload(cache_key)
        if cached is not None:
            return jsonify(_with_cache_meta(cached, cache_hit=True))

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
                o.event_timestamp,
                o.week_start,
                o.latitude,
                o.longitude,
                s.species_code
            FROM observations o
            JOIN species s ON s.id = o.species_id
            {where_clause}
            ORDER BY o.event_timestamp
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()

        elapsed_ms = round((time.perf_counter() - start) * 1000, 3)

        payload = [
            {
                "event_timestamp": row["event_timestamp"],
                "week_start": row["week_start"],
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "species_code": row["species_code"],
            }
            for row in rows
        ]

        response_payload = {
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
        _set_cached_payload(cache_key, response_payload)
        return jsonify(_with_cache_meta(response_payload, cache_hit=False))

    @app.get("/api/visualization/")
    def get_visualization_bundle():
        start = time.perf_counter()

        species_code = request.args.get("species_code", "").strip()
        if not species_code:
            raise ApiError("Query parameter 'species_code' is required", status_code=400)

        limit_weekly = parse_int_arg("limit_weekly", default=5000, minimum=1, maximum=5000)
        limit_points = parse_int_arg("limit_points", default=3000, minimum=1, maximum=10000)
        max_route_points = parse_int_arg("max_route_points", default=1200, minimum=50, maximum=3000)
        recent_pct = parse_int_arg("recent_pct", default=33, minimum=5, maximum=95)
        from_date = parse_date_arg("from")
        to_date = parse_date_arg("to")

        cache_key = _build_cache_key(
            "visualization",
            (species_code, limit_weekly, limit_points, max_route_points, recent_pct, from_date, to_date),
        )
        cached = _get_cached_payload(cache_key)
        if cached is not None:
            return jsonify(_with_cache_meta(cached, cache_hit=True))

        filters: list[str] = ["s.species_code = ?"]
        params: list[object] = [species_code]

        if from_date is not None:
            filters.append("o.week_start >= ?")
            params.append(from_date)

        if to_date is not None:
            filters.append("o.week_start <= ?")
            params.append(to_date)

        where_clause = "WHERE " + " AND ".join(filters)

        weekly_rows = get_db().execute(
            f"""
            SELECT
                o.week_start,
                COUNT(o.id) AS observation_count
            FROM observations o
            JOIN species s ON s.id = o.species_id
            {where_clause}
            GROUP BY o.week_start
            ORDER BY o.week_start
            LIMIT ?
            """,
            tuple([*params, limit_weekly]),
        ).fetchall()

        # Blend historical context with recent observations so new local sightings
        # remain visible even when older tracking data is much denser.
        recent_limit = max(1, int(round((limit_points * recent_pct) / 100)))
        if limit_points > 1:
            recent_limit = min(limit_points - 1, recent_limit)
        historical_limit = max(0, limit_points - recent_limit)

        historical_rows = get_db().execute(
            f"""
            SELECT
                o.event_timestamp,
                o.week_start,
                o.latitude,
                o.longitude
            FROM observations o
            JOIN species s ON s.id = o.species_id
            {where_clause}
            ORDER BY o.event_timestamp
            LIMIT ?
            """,
            tuple([*params, historical_limit]),
        ).fetchall()

        recent_rows = get_db().execute(
            f"""
            SELECT
                o.event_timestamp,
                o.week_start,
                o.latitude,
                o.longitude
            FROM observations o
            JOIN species s ON s.id = o.species_id
            {where_clause}
            ORDER BY o.event_timestamp DESC
            LIMIT ?
            """,
            tuple([*params, recent_limit]),
        ).fetchall()

        source_rows = get_db().execute(
            f"""
            SELECT
                CASE
                    WHEN o.source_file LIKE 'ebird_%' THEN 'ebird'
                    WHEN o.source_file LIKE 'movebank_%' THEN 'movebank'
                    ELSE 'other'
                END AS source_name,
                COUNT(o.id) AS observation_count
            FROM observations o
            JOIN species s ON s.id = o.species_id
            {where_clause}
            GROUP BY source_name
            """,
            tuple(params),
        ).fetchall()

        ordered_rows: list[sqlite3.Row] = []
        seen_keys: set[tuple[str, str, str, str]] = set()
        for row in [*historical_rows, *reversed(recent_rows)]:
            key = (
                str(row["event_timestamp"]),
                str(row["week_start"]),
                str(row["latitude"]),
                str(row["longitude"]),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            ordered_rows.append(row)

        route_rows = ordered_rows[:limit_points]

        weekly_payload = [
            {
                "week_start": row["week_start"],
                "species_code": species_code,
                "observation_count": int(row["observation_count"]),
            }
            for row in weekly_rows
        ]

        route_payload_raw = [
            {
                "week_start": row["week_start"],
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "species_code": species_code,
            }
            for row in route_rows
        ]

        source_breakdown = {
            row["source_name"]: int(row["observation_count"])
            for row in source_rows
        }

        step = 1
        if len(route_payload_raw) > max_route_points:
            step = max(1, (len(route_payload_raw) + max_route_points - 1) // max_route_points)

        route_payload = [
            row for index, row in enumerate(route_payload_raw)
            if index % step == 0
        ]

        timeline_weeks = sorted(
            {
                row["week_start"]
                for row in weekly_payload
                if isinstance(row.get("week_start"), str) and row["week_start"]
            }
        )

        elapsed_ms = round((time.perf_counter() - start) * 1000, 3)

        response_payload = {
            "data": {
                "weekly": weekly_payload,
                "route_points": route_payload,
                "timeline_weeks": timeline_weeks,
            },
            "meta": {
                "species_code": species_code,
                "count_weekly": len(weekly_payload),
                "count_route_points": len(route_payload),
                "count_route_points_raw": len(route_payload_raw),
                "count_route_points_historical_raw": len(historical_rows),
                "count_route_points_recent_raw": len(recent_rows),
                "recent_pct": recent_pct,
                "historical_pct": 100 - recent_pct,
                "source_breakdown": source_breakdown,
                "from": from_date,
                "to": to_date,
                "query_ms": elapsed_ms,
            },
        }
        _set_cached_payload(cache_key, response_payload)
        return jsonify(_with_cache_meta(response_payload, cache_hit=False))

    @app.get("/health")
    def health_check():
        return jsonify({"status": "ok"})

    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
