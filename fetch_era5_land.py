import argparse
import calendar
import sqlite3
from datetime import datetime
from pathlib import Path

import cdsapi
import pandas as pd
import xarray as xr

PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DB_PATH = PROJECT_ROOT / "backend" / "database" / "migration.db"
DEFAULT_OUTPUT_CSV = PROJECT_ROOT / "docs" / "climate_daily_story5_1_era5_land.csv"
DEFAULT_RAW_DIR = PROJECT_ROOT / "docs" / "era5_land_raw"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch ERA5-Land hourly data and aggregate to daily point series."
    )
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="Path to migration SQLite DB")
    parser.add_argument(
        "--output-csv",
        default=str(DEFAULT_OUTPUT_CSV),
        help="Path to write daily climate CSV",
    )
    parser.add_argument(
        "--raw-dir",
        default=str(DEFAULT_RAW_DIR),
        help="Directory for raw ERA5-Land NetCDF files",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Redownload NetCDF files even if they already exist",
    )
    return parser.parse_args()


def ensure_cds_config() -> None:
    config_path = Path.home() / ".cdsapirc"
    if not config_path.exists():
        raise FileNotFoundError(
            "Missing CDS API config at ~/.cdsapirc. Set your Copernicus API key before running."
        )


def load_bounds(db_path: Path) -> tuple[str, str, datetime, datetime, float, float]:
    if not db_path.exists():
        raise FileNotFoundError(f"Migration DB not found: {db_path}")

    with sqlite3.connect(db_path) as con:
        query = (
            "SELECT MIN(date(event_timestamp)), MAX(date(event_timestamp)), "
            "AVG(latitude), AVG(longitude) FROM observations"
        )
        date_min_str, date_max_str, avg_lat, avg_lon = con.execute(query).fetchone()

    if not date_min_str or not date_max_str:
        raise ValueError("No observation dates found in the migration DB.")
    if avg_lat is None or avg_lon is None:
        raise ValueError("No latitude/longitude values found in the migration DB.")

    date_min = datetime.strptime(date_min_str, "%Y-%m-%d")
    date_max = datetime.strptime(date_max_str, "%Y-%m-%d")
    return date_min_str, date_max_str, date_min, date_max, float(avg_lat), float(avg_lon)


def month_iterator(start: datetime, end: datetime):
    year = start.year
    month = start.month
    while (year, month) <= (end.year, end.month):
        last_day = calendar.monthrange(year, month)[1]
        day_start = 1
        day_end = last_day
        if year == start.year and month == start.month:
            day_start = start.day
        if year == end.year and month == end.month:
            day_end = end.day
        yield year, month, day_start, day_end

        if month == 12:
            year += 1
            month = 1
        else:
            month += 1


def download_month(
    client: cdsapi.Client,
    year: int,
    month: int,
    day_start: int,
    day_end: int,
    area: list[float],
    out_path: Path,
    overwrite: bool,
) -> None:
    if out_path.exists() and not overwrite:
        return

    days = [f"{day:02d}" for day in range(day_start, day_end + 1)]
    hours = [f"{hour:02d}:00" for hour in range(24)]

    client.retrieve(
        "reanalysis-era5-land",
        {
            "variable": ["2m_temperature", "total_precipitation"],
            "year": [str(year)],
            "month": [f"{month:02d}"],
            "day": days,
            "time": hours,
            "area": area,
            "format": "netcdf",
        },
        str(out_path),
    )


def build_daily_from_nc(
    nc_path: Path,
    start: datetime,
    end: datetime,
    lat: float,
    lon: float,
) -> pd.DataFrame:
    ds = xr.open_dataset(nc_path)
    try:
        ds = ds.sel(time=slice(start, end))
        ds = ds.sel(latitude=lat, longitude=lon, method="nearest")

        if "t2m" not in ds or "tp" not in ds:
            raise ValueError(f"Expected variables t2m and tp in {nc_path.name}.")

        tmean_c = ds["t2m"].resample(time="1D").mean() - 273.15
        precip_mm = ds["tp"].resample(time="1D").sum() * 1000.0

        return pd.DataFrame(
            {
                "date": tmean_c["time"].dt.strftime("%Y-%m-%d").values,
                "tmean_c": tmean_c.values,
                "precip_mm": precip_mm.values,
            }
        )
    finally:
        ds.close()


def main() -> int:
    args = parse_args()
    ensure_cds_config()

    db_path = Path(args.db_path)
    output_csv = Path(args.output_csv)
    raw_dir = Path(args.raw_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    date_min_str, date_max_str, date_min, date_max, lat, lon = load_bounds(db_path)

    area = [lat + 0.1, lon - 0.1, lat - 0.1, lon + 0.1]
    client = cdsapi.Client()

    daily_frames = []
    for year, month, day_start, day_end in month_iterator(date_min, date_max):
        nc_path = raw_dir / f"era5_land_{year}_{month:02d}.nc"
        download_month(client, year, month, day_start, day_end, area, nc_path, args.overwrite)
        daily_frames.append(build_daily_from_nc(nc_path, date_min, date_max, lat, lon))

    climate = pd.concat(daily_frames, ignore_index=True)
    climate = climate.drop_duplicates(subset=["date"]).sort_values("date")
    climate["date"] = pd.to_datetime(climate["date"], errors="coerce")
    climate = climate.dropna(subset=["date"]).sort_values("date")

    climate["tmean_c_roll7"] = climate["tmean_c"].rolling(window=7, min_periods=7).mean()
    climate["tmean_c_roll14"] = climate["tmean_c"].rolling(window=14, min_periods=14).mean()
    climate["precip_mm_roll7"] = climate["precip_mm"].rolling(window=7, min_periods=7).sum()
    climate["precip_mm_roll14"] = climate["precip_mm"].rolling(window=14, min_periods=14).sum()

    climate["date"] = climate["date"].dt.strftime("%Y-%m-%d")
    climate.to_csv(output_csv, index=False)

    rows = len(climate)
    tmean_cov = climate["tmean_c"].notnull().mean()
    precip_cov = climate["precip_mm"].notnull().mean()
    tmean_roll7_cov = climate["tmean_c_roll7"].notnull().mean()
    tmean_roll14_cov = climate["tmean_c_roll14"].notnull().mean()
    precip_roll7_cov = climate["precip_mm_roll7"].notnull().mean()
    precip_roll14_cov = climate["precip_mm_roll14"].notnull().mean()

    print("Source: ERA5-Land reanalysis")
    print(f"Point: ({lat}, {lon})")
    print(f"Area: {area}")
    print(f"Rows: {rows}")
    print(f"Date Range: {date_min_str} to {date_max_str}")
    print(f"tmean_c coverage: {tmean_cov:.2%}")
    print(f"precip_mm coverage: {precip_cov:.2%}")
    print(f"tmean_c_roll7 coverage: {tmean_roll7_cov:.2%}")
    print(f"tmean_c_roll14 coverage: {tmean_roll14_cov:.2%}")
    print(f"precip_mm_roll7 coverage: {precip_roll7_cov:.2%}")
    print(f"precip_mm_roll14 coverage: {precip_roll14_cov:.2%}")
    print(f"Output CSV: {output_csv}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
