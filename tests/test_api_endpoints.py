from pathlib import Path

import pytest

from backend.app import create_app


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_DB = PROJECT_ROOT / "backend" / "database" / "migration.db"


@pytest.fixture()
def client():
    app = create_app()
    app.testing = True
    with app.test_client() as client:
        yield client


def _skip_if_db_missing() -> None:
    if not MIGRATION_DB.exists():
        pytest.skip(f"Missing migration DB: {MIGRATION_DB}")


def test_ui_homepage_served(client):
    response = client.get("/ui")

    assert response.status_code == 200
    assert b"Bird Migration Explorer" in response.data


def test_health_check(client):
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.get_json()
    assert isinstance(payload, dict)
    assert payload.get("status") == "ok"


def test_api_species_limit(client):
    _skip_if_db_missing()

    response = client.get("/api/species?limit=5")

    assert response.status_code == 200
    payload = response.get_json()
    assert isinstance(payload, dict)
    assert "data" in payload
    assert "meta" in payload
    assert payload["meta"].get("limit") == 5


def test_api_migration_limit(client):
    _skip_if_db_missing()

    response = client.get("/api/migration/?limit=10")

    assert response.status_code == 200
    payload = response.get_json()
    assert isinstance(payload, dict)
    assert "data" in payload
    assert "meta" in payload
    assert payload["meta"].get("limit") == 10


def test_api_visualization_requires_species_code(client):
    _skip_if_db_missing()

    response = client.get("/api/visualization/")

    assert response.status_code == 400
    payload = response.get_json()
    assert isinstance(payload, dict)
    assert payload.get("status") == 400


def test_api_visualization_bundle(client):
    _skip_if_db_missing()

    species_response = client.get("/api/species?limit=1")
    assert species_response.status_code == 200
    species_payload = species_response.get_json()
    assert isinstance(species_payload, dict)

    species_list = species_payload.get("data", [])
    if not species_list:
        pytest.skip("No species available in migration DB")

    species_code = species_list[0].get("species_code")
    if not species_code:
        pytest.skip("Species response missing species_code")

    response = client.get(f"/api/visualization/?species_code={species_code}")

    assert response.status_code == 200
    payload = response.get_json()
    assert isinstance(payload, dict)
    assert "data" in payload
    assert "meta" in payload
    assert "weekly" in payload["data"]
    assert "route_points" in payload["data"]
