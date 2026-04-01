import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_REGION_CODE = "GB-ENG-CON"
DEFAULT_MAX_RESULTS = 50
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_OUTPUT_DIR = "backend/data/raw"
EBIRD_URL_TEMPLATE = "https://api.ebird.org/v2/data/obs/{region_code}/recent"
EBIRD_SPECIES_URL_TEMPLATE = "https://api.ebird.org/v2/data/obs/{region_code}/recent/{species_code}"
EBIRD_REGION_SPPLIST_URL_TEMPLATE = "https://api.ebird.org/v2/product/spplist/{region_code}"


class EbirdApiError(Exception):
    pass


def load_env_file(path: str = ".env", override: bool = False) -> None:
    env_path = Path(path)
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[len("export "):].strip()

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if not key:
            continue

        if override or key not in os.environ:
            os.environ[key] = value


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


def _normalize_region_code(region_code: str) -> str:
    cleaned = region_code.strip().upper()
    if not cleaned:
        raise ValueError("region_code must not be empty")
    return cleaned


def _normalize_species_code(species_code: str | None) -> str | None:
    if species_code is None:
        return None
    cleaned = species_code.strip().lower()
    if not cleaned:
        raise ValueError("species_code must not be empty when provided")
    return cleaned


def _resolve_api_key(api_key: str | None) -> str:
    key = (api_key or os.getenv("EBIRD_API_KEY", "")).strip()
    if not key:
        raise EbirdApiError("Missing EBIRD_API_KEY. Add it to .env or pass --api-key.")
    return key


def _resolve_output_dir(output_dir: str) -> Path:
    out_dir = Path(output_dir)
    if out_dir.is_absolute():
        return out_dir
    return PROJECT_ROOT / out_dir


def _request_json(
    *,
    url: str,
    api_key: str,
    timeout_seconds: int,
    session: requests.Session | None = None,
    params: dict | None = None,
    error_prefix: str = "eBird request failed",
) -> object:
    if timeout_seconds < 1:
        raise ValueError("timeout_seconds must be at least 1")

    headers = {"X-eBirdApiToken": api_key}
    client = session or _build_session()

    try:
        response = client.get(url, headers=headers, params=params, timeout=timeout_seconds)
    except requests.RequestException as exc:
        raise EbirdApiError(f"Network error while calling eBird: {exc}") from exc

    if response.status_code == 429:
        raise EbirdApiError("Rate limited by eBird API")

    if not response.ok:
        body_preview = (response.text or "")[:500]
        raise EbirdApiError(
            f"{error_prefix} with status {response.status_code}: {body_preview}"
        )

    try:
        return response.json()
    except ValueError as exc:
        raise EbirdApiError("Invalid JSON from eBird") from exc


def fetch_recent_observations(
    region_code: str,
    api_key: str | None = None,
    max_results: int = DEFAULT_MAX_RESULTS,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
    species_code: str | None = None,
) -> list[dict]:
    if max_results < 1:
        raise ValueError("max_results must be at least 1")

    cleaned_region = _normalize_region_code(region_code)
    cleaned_species = _normalize_species_code(species_code)
    key = _resolve_api_key(api_key)

    if cleaned_species:
        url = EBIRD_SPECIES_URL_TEMPLATE.format(
            region_code=cleaned_region,
            species_code=cleaned_species,
        )
    else:
        url = EBIRD_URL_TEMPLATE.format(region_code=cleaned_region)

    logger.info(
        "Requesting eBird data for region=%s species=%s",
        cleaned_region,
        cleaned_species or "ALL",
    )

    payload = _request_json(
        url=url,
        api_key=key,
        timeout_seconds=timeout_seconds,
        session=session,
        params={"maxResults": max_results},
        error_prefix="eBird data request failed",
    )

    if not isinstance(payload, list):
        raise EbirdApiError("Unexpected eBird payload shape (expected a list)")

    logger.info("Fetched %s records from eBird", len(payload))
    return payload


def fetch_region_species_list(
    region_code: str,
    api_key: str | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[str]:
    cleaned_region = _normalize_region_code(region_code)
    key = _resolve_api_key(api_key)
    url = EBIRD_REGION_SPPLIST_URL_TEMPLATE.format(region_code=cleaned_region)

    logger.info("Requesting eBird species list for region=%s", cleaned_region)

    payload = _request_json(
        url=url,
        api_key=key,
        timeout_seconds=timeout_seconds,
        session=session,
        error_prefix="eBird species list request failed",
    )

    if not isinstance(payload, list):
        raise EbirdApiError("Unexpected species list payload shape (expected a list)")
    if not all(isinstance(code, str) for code in payload):
        raise EbirdApiError("Unexpected species list payload contents (expected list[str])")

    logger.info("Fetched %s species codes for region %s", len(payload), cleaned_region)
    return payload


def save_raw_json(data: object, output_dir: str = DEFAULT_OUTPUT_DIR) -> Path:
    out_dir = _resolve_output_dir(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"ebird_raw_{ts}.json"

    with out_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)

    logger.info("Saved raw eBird JSON to %s", out_path)
    return out_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch recent eBird observations or region species list and save as raw JSON.",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("EBIRD_REGION", DEFAULT_REGION_CODE),
        help="eBird region code (default: %(default)s)",
    )
    parser.add_argument(
        "--species",
        default=None,
        help="Optional species code (example: barswa, oystca1, norwhe).",
    )
    parser.add_argument(
        "--region-spplist",
        default=None,
        help="Optional region code for species list mode (example: GB-ENG-CON).",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=DEFAULT_MAX_RESULTS,
        help="Maximum number of records to request (default: %(default)s)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="eBird API key. If omitted, EBIRD_API_KEY from environment is used.",
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where JSON output is saved (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    # Prefer repository-root .env so execution works from any current directory.
    project_root_env = PROJECT_ROOT / ".env"
    load_env_file(str(project_root_env))
    args = _parse_args()

    try:
        if args.region_spplist:
            records = fetch_region_species_list(
                region_code=args.region_spplist,
                api_key=args.api_key,
                timeout_seconds=args.timeout,
            )
            output_path = save_raw_json(records, output_dir=args.output_dir)
            print(
                f"Fetched {len(records)} species codes for region {args.region_spplist} -> {output_path}"
            )
        else:
            records = fetch_recent_observations(
                region_code=args.region,
                api_key=args.api_key,
                max_results=args.max_results,
                timeout_seconds=args.timeout,
                species_code=args.species,
            )
            output_path = save_raw_json(records, output_dir=args.output_dir)
            species_label = args.species if args.species else "ALL_SPECIES"
            print(
                f"Fetched {len(records)} records for region {args.region}, species {species_label} -> {output_path}"
            )
    except (EbirdApiError, ValueError) as exc:
        logger.error(str(exc))
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())