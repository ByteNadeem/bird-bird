import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


DEFAULT_OUTPUT_DIR = Path("backend/data/plots")


def _load_json(path: Path) -> list:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("Expected top-level JSON array")
    if not data:
        raise ValueError("JSON array is empty")
    return data


def _plot_species_code_list(data: list[str], output_dir: Path, stem: str) -> list[Path]:
    outputs: list[Path] = []
    series = pd.Series(data, name="speciesCode")

    prefix_counts = series.str[:2].value_counts().head(20).sort_values()
    plt.figure(figsize=(10, 6))
    prefix_counts.plot(kind="barh")
    plt.title("Top Species-Code Prefixes")
    plt.xlabel("Count")
    plt.ylabel("Prefix")
    plt.tight_layout()
    out1 = output_dir / f"{stem}_prefixes.png"
    plt.savefig(out1, dpi=150)
    plt.close()
    outputs.append(out1)

    first_char_counts = series.str[:1].value_counts().sort_values()
    plt.figure(figsize=(10, 6))
    first_char_counts.plot(kind="barh")
    plt.title("Species-Code First Character Distribution")
    plt.xlabel("Count")
    plt.ylabel("First Character")
    plt.tight_layout()
    out2 = output_dir / f"{stem}_first_char.png"
    plt.savefig(out2, dpi=150)
    plt.close()
    outputs.append(out2)

    return outputs


def _plot_observation_records(data: list[dict], output_dir: Path, stem: str) -> list[Path]:
    outputs: list[Path] = []
    df = pd.DataFrame(data)

    if "obsDt" in df.columns:
        df["obsDt"] = pd.to_datetime(df["obsDt"], errors="coerce")
    if "howMany" in df.columns:
        df["howMany"] = pd.to_numeric(df["howMany"], errors="coerce").fillna(1)
    else:
        df["howMany"] = 1

    if "obsDt" in df.columns:
        day_counts = (
            df.dropna(subset=["obsDt"])
            .assign(day=lambda x: x["obsDt"].dt.date)
            .groupby("day")
            .size()
        )
        if not day_counts.empty:
            plt.figure(figsize=(11, 4))
            day_counts.plot(marker="o")
            plt.title("Observations Per Day")
            plt.xlabel("Day")
            plt.ylabel("Count")
            plt.tight_layout()
            out = output_dir / f"{stem}_per_day.png"
            plt.savefig(out, dpi=150)
            plt.close()
            outputs.append(out)

    if "locName" in df.columns:
        top_locs = df["locName"].value_counts().head(15).sort_values()
        if not top_locs.empty:
            plt.figure(figsize=(11, 6))
            top_locs.plot(kind="barh")
            plt.title("Top Locations")
            plt.xlabel("Records")
            plt.ylabel("Location")
            plt.tight_layout()
            out = output_dir / f"{stem}_top_locations.png"
            plt.savefig(out, dpi=150)
            plt.close()
            outputs.append(out)

    if {"lat", "lng"}.issubset(df.columns):
        geo = df.dropna(subset=["lat", "lng"]).copy()
        if not geo.empty:
            plt.figure(figsize=(8, 8))
            plt.scatter(
                geo["lng"],
                geo["lat"],
                s=(geo["howMany"].clip(lower=1) * 25),
                alpha=0.6,
            )
            plt.title("Observation Locations")
            plt.xlabel("Longitude")
            plt.ylabel("Latitude")
            plt.tight_layout()
            out = output_dir / f"{stem}_locations.png"
            plt.savefig(out, dpi=150)
            plt.close()
            outputs.append(out)

    return outputs


def visualize(input_file: Path, output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    data = _load_json(input_file)
    stem = input_file.stem

    first = data[0]
    if isinstance(first, str):
        return _plot_species_code_list(data, output_dir, stem)

    if isinstance(first, dict):
        return _plot_observation_records(data, output_dir, stem)

    raise ValueError("Unsupported JSON item type. Expected list[str] or list[dict].")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Visualize eBird JSON outputs from spplist or recent observation modes.",
    )
    parser.add_argument("input_file", help="Path to eBird JSON file")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory for generated plots (default: backend/data/plots)",
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

    if not outputs:
        print("No charts generated (data may be missing required columns).")
        return 0

    print("Generated charts:")
    for path in outputs:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
