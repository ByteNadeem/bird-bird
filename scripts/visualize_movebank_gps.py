import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


DEFAULT_OUTPUT_DIR = Path("backend/data/plots")


def _load_records(path: Path) -> list:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("Expected top-level JSON array")
    if not data:
        raise ValueError("No records found in file")
    return data


def _to_dataframe(records: list) -> pd.DataFrame:
    first = records[0]

    # Supports transformed movebank GPS tuples/lists:
    # [timestamp, deployment_id, lat, lng]
    if isinstance(first, (list, tuple)) and len(first) >= 4:
        df = pd.DataFrame(records, columns=["timestamp", "deployment_id", "lat", "lng"])
        return df

    # Supports raw dict events from mode=events
    if isinstance(first, dict):
        df = pd.DataFrame(records)
        rename_map = {
            "location_lat": "lat",
            "location_long": "lng",
            "deployment_id": "deployment_id",
            "timestamp": "timestamp",
        }
        df = df.rename(columns=rename_map)
        return df

    raise ValueError("Unsupported Movebank GPS JSON shape")


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    required = ["timestamp", "lat", "lng"]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    if "deployment_id" not in df.columns:
        df["deployment_id"] = "unknown"

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lng"] = pd.to_numeric(df["lng"], errors="coerce")

    df = df.dropna(subset=["timestamp", "lat", "lng"]).copy()
    if df.empty:
        raise ValueError("No valid GPS rows after cleaning")

    df = df.sort_values("timestamp")
    return df


def visualize(input_file: Path, output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    records = _load_records(input_file)
    df = _clean_dataframe(_to_dataframe(records))

    stem = input_file.stem
    outputs: list[Path] = []

    # 1) GPS track path
    plt.figure(figsize=(8, 8))
    plt.plot(df["lng"], df["lat"], linewidth=1, alpha=0.7)
    plt.scatter(df["lng"], df["lat"], s=10, alpha=0.8)
    plt.title("Movebank GPS Track")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.tight_layout()
    out1 = output_dir / f"{stem}_track.png"
    plt.savefig(out1, dpi=150)
    plt.close()
    outputs.append(out1)

    # 2) Fixes per day
    per_day = df.assign(day=df["timestamp"].dt.date).groupby("day").size()
    if not per_day.empty:
        plt.figure(figsize=(11, 4))
        per_day.plot(marker="o")
        plt.title("GPS Fixes Per Day")
        plt.xlabel("Day")
        plt.ylabel("Fix Count")
        plt.tight_layout()
        out2 = output_dir / f"{stem}_fixes_per_day.png"
        plt.savefig(out2, dpi=150)
        plt.close()
        outputs.append(out2)

    # 3) Hour-of-day distribution
    per_hour = df["timestamp"].dt.hour.value_counts().sort_index()
    plt.figure(figsize=(10, 4))
    per_hour.plot(kind="bar")
    plt.title("GPS Fixes by Hour")
    plt.xlabel("Hour")
    plt.ylabel("Fix Count")
    plt.tight_layout()
    out3 = output_dir / f"{stem}_fixes_by_hour.png"
    plt.savefig(out3, dpi=150)
    plt.close()
    outputs.append(out3)

    return outputs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Visualize Movebank GPS event data")
    parser.add_argument("input_file", help="Path to Movebank GPS JSON file")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for chart outputs (default: backend/data/plots)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_file = Path(args.input_file)
    output_dir = Path(args.output_dir)

    try:
        outputs = visualize(input_file, output_dir)
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}")
        return 1

    print("Generated charts:")
    for item in outputs:
        print(item)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
