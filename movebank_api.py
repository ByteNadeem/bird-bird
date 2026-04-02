import argparse
import csv
import hashlib
import io
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

MOVEBANK_URL = "https://www.movebank.org/movebank/service/direct-read"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_OUTPUT_DIR = "backend/data/raw"
PROJECT_ROOT = Path(__file__).resolve().parent

GPS_SENSOR_TYPE_ID = 653
ACC_SENSOR_TYPE_ID = 2365683
RADIO_SENSOR_TYPE_ID = 673


class MovebankApiError(Exception):
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
            line = line[len("export ") :].strip()

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


def _resolve_credentials(username: str | None = None, password: str | None = None) -> tuple[str, str]:
    user = (username or os.getenv("MOVEBANK_USERNAME") or os.getenv("mbus") or "").strip()
    pw = (password or os.getenv("MOVEBANK_PASSWORD") or os.getenv("mbpw") or "").strip()

    if not user or not pw:
        raise MovebankApiError(
            "Missing Movebank credentials. Set MOVEBANK_USERNAME/MOVEBANK_PASSWORD "
            "(or legacy mbus/mbpw) in your environment or .env."
        )

    return user, pw


def _request_movebank_text(
    params: tuple[tuple[str, str | int], ...],
    *,
    username: str,
    password: str,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> str:
    if timeout_seconds < 1:
        raise ValueError("timeout_seconds must be at least 1")

    client = session or _build_session()

    try:
        response = client.get(
            MOVEBANK_URL,
            params=params,
            auth=(username, password),
            timeout=timeout_seconds,
        )
    except requests.RequestException as exc:
        raise MovebankApiError(f"Network error while calling Movebank: {exc}") from exc

    logger.info("Movebank request: %s", response.url)

    if response.status_code == 200 and b"License Terms:" in response.content:
        license_hash = hashlib.md5(response.content).hexdigest()
        params_with_license = params + (("license-md5", license_hash),)
        response = client.get(
            MOVEBANK_URL,
            params=params_with_license,
            cookies=response.cookies,
            auth=(username, password),
            timeout=timeout_seconds,
        )

    if not response.ok:
        body_preview = (response.text or "")[:500]
        raise MovebankApiError(
            f"Movebank request failed with status {response.status_code}: {body_preview}"
        )

    return response.content.decode("utf-8")


def _parse_csv_text(text: str) -> list[dict[str, str]]:
    if not text.strip():
        return []
    return list(csv.DictReader(io.StringIO(text), delimiter=","))


def get_studies(
    username: str,
    password: str,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    params = (
        ("entity_type", "study"),
        ("i_can_see_data", "true"),
        ("there_are_data_which_i_cannot_see", "false"),
    )
    text = _request_movebank_text(
        params,
        username=username,
        password=password,
        timeout_seconds=timeout_seconds,
        session=session,
    )
    studies = _parse_csv_text(text)
    return [
        s
        for s in studies
        if s.get("i_can_see_data") == "true"
        and s.get("there_are_data_which_i_cannot_see") == "false"
    ]


def get_studies_by_sensor(studies: list[dict[str, str]], sensor_name: str = "GPS") -> list[dict[str, str]]:
    return [s for s in studies if sensor_name in s.get("sensor_type_ids", "")]


def filter_studies(
    studies: list[dict[str, str]],
    *,
    taxon_queries: list[str] | None = None,
    sensor_name: str | None = None,
    require_download_access: bool = False,
) -> list[dict[str, str]]:
    filtered = studies

    if require_download_access:
        filtered = [s for s in filtered if s.get("i_have_download_access") == "true"]

    if sensor_name:
        wanted_sensor = sensor_name.strip().lower()
        filtered = [s for s in filtered if wanted_sensor in s.get("sensor_type_ids", "").lower()]

    if taxon_queries:
        wanted_taxa = [query.strip().lower() for query in taxon_queries if query.strip()]
        if wanted_taxa:
            filtered = [
                s
                for s in filtered
                if any(query in s.get("taxon_ids", "").lower() for query in wanted_taxa)
            ]

    return filtered


def get_individuals_by_study(
    study_id: int,
    username: str,
    password: str,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    params = (("entity_type", "individual"), ("study_id", study_id))
    text = _request_movebank_text(
        params,
        username=username,
        password=password,
        timeout_seconds=timeout_seconds,
        session=session,
    )
    return _parse_csv_text(text)


def get_individual_events(
    study_id: int,
    individual_id: int,
    sensor_type_id: int,
    username: str,
    password: str,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    params = (
        ("entity_type", "event"),
        ("study_id", study_id),
        ("individual_id", individual_id),
        ("sensor_type_id", sensor_type_id),
        ("attributes", "all"),
    )
    text = _request_movebank_text(
        params,
        username=username,
        password=password,
        timeout_seconds=timeout_seconds,
        session=session,
    )
    return _parse_csv_text(text)


def _get_individual_sensor_text(
    *,
    study_id: int,
    individual_id: int,
    username: str,
    password: str,
    timeout_seconds: int,
) -> str | None:
    individuals = get_individuals_by_study(
        study_id=study_id,
        username=username,
        password=password,
        timeout_seconds=timeout_seconds,
    )
    for individual in individuals:
        try:
            if int(individual.get("id", "0")) == individual_id:
                return individual.get("sensor_type_ids", "")
        except ValueError:
            continue
    return None


def transform_raw_gps(gps_events: list[dict[str, str]]) -> list[tuple[str, str, float | str, float | str]]:
    out: list[tuple[str, str, float | str, float | str]] = []

    for event in gps_events:
        lat: float | str = event.get("location_lat", "")
        lng: float | str = event.get("location_long", "")

        try:
            if lat != "":
                lat = float(lat)
            if lng != "":
                lng = float(lng)
        except (TypeError, ValueError):
            logger.warning("Could not parse lat/long for event with timestamp=%s", event.get("timestamp"))

        out.append((event.get("timestamp", ""), event.get("deployment_id", ""), lat, lng))

    return out


def transform_raw_acc(
    acc_events: list[dict[str, str]],
    unit: str = "m/s2",
    sensitivity: str = "high",
) -> list[list[tuple[str, str, float, float, float]]]:
    ts_format = "%Y-%m-%d %H:%M:%S.%f"
    out: list[list[tuple[str, str, float, float, float]]] = []

    if not acc_events:
        return out

    unit_factor = 1.0 if unit == "g" else 9.81

    try:
        tag_local_identifier = int(acc_events[0].get("tag_local_identifier", "0"))
    except ValueError:
        tag_local_identifier = 0

    slope = 0.001
    if tag_local_identifier <= 2241:
        if sensitivity == "low":
            slope = 0.0027
    elif 2242 <= tag_local_identifier <= 4117:
        slope = 0.0022
    else:
        slope = 1 / 512

    for event in acc_events:
        deploym = event.get("deployment_id", "")
        freq = event.get("eobs_acceleration_sampling_frequency_per_axis", "")
        raw_text = event.get("eobs_accelerations_raw", "")
        ts_text = event.get("timestamp", "")

        if not freq or not raw_text or not ts_text:
            continue

        try:
            seconds = 1 / float(freq)
            parsedts = datetime.strptime(ts_text, ts_format)
            raw = list(map(int, raw_text.split()))
        except (TypeError, ValueError):
            logger.warning("Skipping malformed ACC event with timestamp=%s", ts_text)
            continue

        ts = [parsedts + timedelta(seconds=seconds * x) for x in range(0, int(len(raw) / 3))]
        it = iter(raw)
        transformed = [
            (
                tstamp.strftime(ts_format),
                deploym,
                (xyz[0] - 2048) * slope * unit_factor,
                (xyz[1] - 2048) * slope * unit_factor,
                (xyz[2] - 2048) * slope * unit_factor,
            )
            for (tstamp, xyz) in list(zip(ts, list(zip(it, it, it))))
        ]
        out.append(transformed)

    return out


def _resolve_output_dir(output_dir: str) -> Path:
    out_dir = Path(output_dir)
    if out_dir.is_absolute():
        return out_dir
    return PROJECT_ROOT / out_dir


def save_raw_json(data: object, mode: str, output_dir: str = DEFAULT_OUTPUT_DIR) -> Path:
    out_dir = _resolve_output_dir(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = out_dir / f"movebank_{mode}_{ts}.json"

    with out_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)

    logger.info("Saved Movebank JSON to %s", out_path)
    return out_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Movebank studies, individuals, or events.")
    parser.add_argument(
        "--mode",
        required=True,
        choices=["studies", "studies-query", "individuals", "events-gps", "events-acc", "events"],
        help="Operation mode",
    )
    parser.add_argument("--study-id", type=int, default=None, help="Movebank study ID")
    parser.add_argument("--individual-id", type=int, default=None, help="Movebank individual ID")
    parser.add_argument("--sensor-name", default="GPS", help="Filter studies by sensor name")
    parser.add_argument(
        "--taxon-query",
        action="append",
        default=None,
        help="Optional taxon filter for studies-query mode. Repeat for multiple taxa.",
    )
    parser.add_argument(
        "--require-download-access",
        action="store_true",
        help="In studies-query mode, keep only studies with i_have_download_access=true.",
    )
    parser.add_argument("--acc-unit", choices=["m/s2", "g"], default="m/s2", help="ACC output unit")
    parser.add_argument(
        "--acc-sensitivity",
        choices=["high", "low"],
        default="high",
        help="ACC conversion sensitivity",
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Output directory for JSON")
    parser.add_argument("--username", default=None, help="Movebank username override")
    parser.add_argument("--password", default=None, help="Movebank password override")
    parser.add_argument(
        "--sensor-type-id",
        type=int,
        default=None,
        help="For mode=events, explicit Movebank sensor type ID (example: 653 GPS, 673 radio-transmitter, 2365683 ACC).",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    load_env_file(str(PROJECT_ROOT / ".env"))
    args = _parse_args()

    try:
        username, password = _resolve_credentials(args.username, args.password)

        if args.mode == "studies":
            studies = get_studies(username=username, password=password, timeout_seconds=args.timeout)
            result = get_studies_by_sensor(studies, args.sensor_name)

        elif args.mode == "studies-query":
            studies = get_studies(username=username, password=password, timeout_seconds=args.timeout)
            result = filter_studies(
                studies,
                taxon_queries=args.taxon_query,
                sensor_name=args.sensor_name,
                require_download_access=args.require_download_access,
            )

        elif args.mode == "individuals":
            if args.study_id is None:
                raise ValueError("--study-id is required for mode=individuals")
            result = get_individuals_by_study(
                study_id=args.study_id,
                username=username,
                password=password,
                timeout_seconds=args.timeout,
            )

        elif args.mode == "events-gps":
            if args.study_id is None or args.individual_id is None:
                raise ValueError("--study-id and --individual-id are required for mode=events-gps")
            events = get_individual_events(
                study_id=args.study_id,
                individual_id=args.individual_id,
                sensor_type_id=GPS_SENSOR_TYPE_ID,
                username=username,
                password=password,
                timeout_seconds=args.timeout,
            )
            result = transform_raw_gps(events)

            if not result:
                sensor_text = _get_individual_sensor_text(
                    study_id=args.study_id,
                    individual_id=args.individual_id,
                    username=username,
                    password=password,
                    timeout_seconds=args.timeout,
                )
                logger.warning(
                    "No GPS events returned for study=%s individual=%s. Individual sensor_type_ids=%s",
                    args.study_id,
                    args.individual_id,
                    sensor_text or "UNKNOWN",
                )

        elif args.mode == "events":
            if args.study_id is None or args.individual_id is None:
                raise ValueError("--study-id and --individual-id are required for mode=events")
            if args.sensor_type_id is None:
                raise ValueError("--sensor-type-id is required for mode=events")

            result = get_individual_events(
                study_id=args.study_id,
                individual_id=args.individual_id,
                sensor_type_id=args.sensor_type_id,
                username=username,
                password=password,
                timeout_seconds=args.timeout,
            )

            if not result:
                sensor_text = _get_individual_sensor_text(
                    study_id=args.study_id,
                    individual_id=args.individual_id,
                    username=username,
                    password=password,
                    timeout_seconds=args.timeout,
                )
                logger.warning(
                    "No events returned for sensor_type_id=%s, study=%s, individual=%s. Individual sensor_type_ids=%s",
                    args.sensor_type_id,
                    args.study_id,
                    args.individual_id,
                    sensor_text or "UNKNOWN",
                )

        else:
            if args.study_id is None or args.individual_id is None:
                raise ValueError("--study-id and --individual-id are required for mode=events-acc")
            events = get_individual_events(
                study_id=args.study_id,
                individual_id=args.individual_id,
                sensor_type_id=ACC_SENSOR_TYPE_ID,
                username=username,
                password=password,
                timeout_seconds=args.timeout,
            )
            result = transform_raw_acc(
                events,
                unit=args.acc_unit,
                sensitivity=args.acc_sensitivity,
            )

            if not result:
                sensor_text = _get_individual_sensor_text(
                    study_id=args.study_id,
                    individual_id=args.individual_id,
                    username=username,
                    password=password,
                    timeout_seconds=args.timeout,
                )
                logger.warning(
                    "No ACC events returned for study=%s individual=%s. Individual sensor_type_ids=%s",
                    args.study_id,
                    args.individual_id,
                    sensor_text or "UNKNOWN",
                )

        output_path = save_raw_json(result, mode=args.mode, output_dir=args.output_dir)
    except (MovebankApiError, ValueError) as exc:
        logger.error(str(exc))
        return 1

    print(f"Fetched {len(result)} records for mode {args.mode} -> {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())



# """"
# SENSORS
# ===============================================================================
# description,external_id,id,is_location_sensor,name
# "","bird-ring",397,true,"Bird Ring"
# "","gps",653,true,"GPS"
# "","radio-transmitter",673,true,"Radio Transmitter"
# "","argos-doppler-shift",82798,true,"Argos Doppler Shift"
# "","natural-mark",2365682,true,"Natural Mark"
# "","acceleration",2365683,false,"Acceleration"
# "","solar-geolocator",3886361,true,"Solar Geolocator"
# "","accessory-measurements",7842954,false,"Accessory Measurements"
# "","solar-geolocator-raw",9301403,false,"Solar Geolocator Raw"
# "","barometer",77740391,false,"Barometer"
# "","magnetometer",77740402,false,"Magnetometer"
# "","orientation",819073350,false,"Orientation"
# "","solar-geolocator-twilight",914097241,false,"Solar Geolocator Twilight"
# """
