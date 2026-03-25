import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class EbirdApiError(Exception):
    pass


def _build_session() -> requests.Session:
    retry_strategy = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "bird-bird/1.0"})
    return session


def fetch_recent_observations(
    region_code: str,
    api_key: str | None = None,
    max_results: int = 50,
    timeout_seconds: int = 20,
) -> list[dict]:
    key = api_key or os.getenv("EBIRD_API_KEY")
    if not key:
        raise EbirdApiError("Missing EBIRD_API_KEY")

    url = f"https://api.ebird.org/v2/data/obs/{region_code}/recent"
    params = {"maxResults": max_results}
    headers = {"X-eBirdApiToken": key}

    session = _build_session()
    logger.info("Requesting eBird data for region=%s", region_code)

    response = session.get(url, headers=headers, params=params, timeout=timeout_seconds)
    if response.status_code == 429:
        logger.warning("eBird rate limit hit for region=%s", region_code)
        raise EbirdApiError("Rate limited by eBird API")
    if not response.ok:
        logger.error(
            "eBird request failed: status=%s body=%s",
            response.status_code,
            response.text[:500],
        )
        raise EbirdApiError(f"eBird request failed with status {response.status_code}")

    try:
        payload = response.json()
    except ValueError as exc:
        logger.exception("Invalid JSON from eBird")
        raise EbirdApiError("Invalid JSON from eBird") from exc

    if not isinstance(payload, list):
        logger.error("Unexpected payload type: %s", type(payload).__name__)
        raise EbirdApiError("Unexpected eBird payload shape")

    logger.info("Fetched %s records from eBird", len(payload))
    return payload


def save_raw_json(data: list[dict], output_dir: str = "backend/data/raw") -> Path:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = Path(output_dir) / f"ebird_raw_{ts}.json"

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info("Saved raw eBird JSON to %s", out_path)
    return out_path


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    region = os.getenv("EBIRD_REGION", "GB")
    records = fetch_recent_observations(region_code=region)
    save_raw_json(records)