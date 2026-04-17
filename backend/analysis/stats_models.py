import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import statsmodels.formula.api as smf


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB_PATH = PROJECT_ROOT / "backend" / "database" / "migration.db"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "docs"
DEFAULT_PLOT_DIR = PROJECT_ROOT / "backend" / "data" / "plots"

EXIT_VALIDATION_ERROR = 2
EXIT_MODEL_ERROR = 3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze migration timing shifts with baseline regression and optional mixed-effects models."
        )
    )
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH), help="Path to migration SQLite DB")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Directory for model outputs")
    parser.add_argument("--plot-dir", default=str(DEFAULT_PLOT_DIR), help="Directory for plot outputs")
    parser.add_argument(
        "--species-code",
        action="append",
        default=[],
        help="Optional species code filter (repeatable)",
    )
    parser.add_argument("--from-year", type=int, default=None, help="Optional lower year bound")
    parser.add_argument("--to-year", type=int, default=None, help="Optional upper year bound")
    parser.add_argument(
        "--min-rows-per-species",
        type=int,
        default=50,
        help="Minimum rows per species required after filtering",
    )
    parser.add_argument(
        "--model",
        choices=["ols", "mixed", "both"],
        default="both",
        help="Model(s) to run",
    )
    parser.add_argument("--alpha", type=float, default=0.05, help="Significance threshold for reporting")
    parser.add_argument(
        "--climate-csv",
        default=None,
        help="Optional climate CSV joined by date (column 'date' required)",
    )
    parser.add_argument(
        "--climate-vars",
        default="",
        help="Comma-separated climate variables to include, e.g. tmean,precip",
    )
    parser.add_argument(
        "--min-climate-coverage",
        type=float,
        default=0.9,
        help="Minimum non-missing coverage required per climate variable",
    )
    return parser.parse_args()


def resolve_path(path_text: str) -> Path:
    path = Path(path_text)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def load_base_data(db_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        raise FileNotFoundError(f"Migration DB not found: {db_path}")

    with sqlite3.connect(db_path) as con:
        query = """
        SELECT
            s.species_code,
            o.event_timestamp,
            o.week_start,
            o.deployment_id,
            o.latitude,
            o.longitude
        FROM observations o
        JOIN species s ON s.id = o.species_id
        """
        return pd.read_sql_query(query, con)


def build_analysis_frame(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    if df.empty:
        raise ValueError("No observations found in migration DB.")

    required = ["species_code", "event_timestamp", "week_start", "deployment_id"]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns in source data: {missing_cols}")

    source_rows = len(df)

    work = df.copy()
    work["event_timestamp"] = pd.to_datetime(work["event_timestamp"], errors="coerce")
    work["week_start"] = pd.to_datetime(work["week_start"], errors="coerce")
    work["deployment_id"] = work["deployment_id"].astype(str).str.strip()
    work["species_code"] = work["species_code"].astype(str).str.strip()

    work = work.dropna(subset=["event_timestamp", "week_start", "deployment_id", "species_code"])
    work = work[work["deployment_id"] != ""]
    work = work[work["species_code"] != ""]

    if work.empty:
        raise ValueError("No valid rows after timestamp and key-field cleaning.")

    work["year"] = work["event_timestamp"].dt.year
    work["day_of_year"] = work["event_timestamp"].dt.dayofyear
    work["week_start_date"] = work["week_start"].dt.strftime("%Y-%m-%d")

    # Aggregate to deployment/species/week level to reduce pseudo-replication from dense point streams.
    grouped = (
        work.groupby(["species_code", "deployment_id", "week_start_date", "year"], as_index=False)
        .agg(day_of_year=("day_of_year", "mean"))
        .copy()
    )

    grouped["year"] = grouped["year"].astype(int)
    grouped["day_of_year"] = grouped["day_of_year"].astype(float)
    grouped["centered_year"] = grouped["year"] - int(grouped["year"].median())

    qc = {
        "source_rows": int(source_rows),
        "rows_after_cleaning": int(len(work)),
        "rows_after_grouping": int(len(grouped)),
    }
    return grouped, qc


def apply_filters(df: pd.DataFrame, args: argparse.Namespace) -> tuple[pd.DataFrame, dict[str, int]]:
    out = df.copy()

    if args.species_code:
        wanted = {item.strip() for item in args.species_code if item.strip()}
        out = out[out["species_code"].isin(wanted)]

    if args.from_year is not None:
        out = out[out["year"] >= args.from_year]
    if args.to_year is not None:
        out = out[out["year"] <= args.to_year]

    if out.empty:
        raise ValueError("No rows remain after species/year filters.")

    counts = out["species_code"].value_counts()
    keep_species = counts[counts >= args.min_rows_per_species].index
    out = out[out["species_code"].isin(keep_species)]

    if out.empty:
        raise ValueError(
            "No rows remain after min-rows-per-species filter. "
            "Lower --min-rows-per-species or broaden filters."
        )

    summary = {
        "rows_after_filters": int(len(out)),
        "species_count": int(out["species_code"].nunique()),
        "deployment_count": int(out["deployment_id"].nunique()),
    }
    return out, summary


def parse_climate_vars(text: str) -> list[str]:
    return [item.strip() for item in text.split(",") if item.strip()]


def merge_climate(
    df: pd.DataFrame,
    climate_csv: Path,
    requested_vars: list[str],
    min_coverage: float,
) -> tuple[pd.DataFrame, list[str], dict[str, float]]:
    if not climate_csv.exists():
        raise FileNotFoundError(f"Climate CSV not found: {climate_csv}")

    climate = pd.read_csv(climate_csv)
    if "date" not in climate.columns:
        raise ValueError("Climate CSV must contain a 'date' column.")

    climate["date"] = pd.to_datetime(climate["date"], errors="coerce")
    climate = climate.dropna(subset=["date"]).copy()
    climate["week_start_date"] = climate["date"].dt.strftime("%Y-%m-%d")

    if requested_vars:
        missing = [c for c in requested_vars if c not in climate.columns]
        if missing:
            raise ValueError(f"Missing requested climate columns: {missing}")
        candidate_vars = requested_vars
    else:
        candidate_vars = [
            c
            for c in climate.columns
            if c not in {"date", "week_start_date", "lat", "latitude", "lon", "longitude"}
            and pd.api.types.is_numeric_dtype(climate[c])
        ]

    if not candidate_vars:
        raise ValueError("No usable climate variables found.")

    reduced = climate[["week_start_date", *candidate_vars]].copy()
    reduced = reduced.groupby("week_start_date", as_index=False).mean(numeric_only=True)

    merged = df.merge(reduced, on="week_start_date", how="left")

    coverage: dict[str, float] = {}
    usable_vars: list[str] = []
    for col in candidate_vars:
        ratio = float(merged[col].notna().mean())
        coverage[col] = ratio
        if ratio >= min_coverage:
            usable_vars.append(col)

    if not usable_vars:
        raise ValueError(
            "No climate variables met the required coverage threshold. "
            "Lower --min-climate-coverage or provide denser climate data."
        )

    merged = merged.dropna(subset=usable_vars).copy()
    if merged.empty:
        raise ValueError("No rows remain after dropping missing climate values.")

    return merged, usable_vars, coverage


def build_formula(df: pd.DataFrame, climate_vars: list[str]) -> str:
    terms: list[str] = ["centered_year"]

    species_count = int(df["species_code"].nunique())
    if species_count > 1:
        terms.append("C(species_code)")
        terms.append("centered_year:C(species_code)")

    for item in climate_vars:
        terms.append(item)

    return "day_of_year ~ " + " + ".join(terms)


def fit_ols(df: pd.DataFrame, formula: str):
    return smf.ols(formula=formula, data=df).fit(cov_type="HC3")


def fit_mixed(df: pd.DataFrame, formula: str):
    if df["deployment_id"].nunique() < 2:
        raise ValueError("Mixed-effects model requires at least 2 deployment groups.")
    return smf.mixedlm(formula=formula, data=df, groups=df["deployment_id"]).fit(
        reml=False,
        method="lbfgs",
        maxiter=300,
        disp=False,
    )


def summarize_effects(result, model_name: str, alpha: float) -> pd.DataFrame:
    conf = result.conf_int(alpha=alpha)
    output = pd.DataFrame(
        {
            "model": model_name,
            "term": result.params.index,
            "estimate": result.params.values,
            "std_error": result.bse.values,
            "p_value": result.pvalues.values,
            "ci_lower": conf.iloc[:, 0].values,
            "ci_upper": conf.iloc[:, 1].values,
        }
    )
    output["nobs"] = float(getattr(result, "nobs", float("nan")))
    output["aic"] = float(getattr(result, "aic", float("nan")))
    output["bic"] = float(getattr(result, "bic", float("nan")))
    output["converged"] = bool(getattr(result, "converged", True))
    return output


def save_plots(df: pd.DataFrame, result, plot_dir: Path) -> tuple[Path, Path]:
    plot_dir.mkdir(parents=True, exist_ok=True)

    timing_plot = plot_dir / "migration_timing_trends.png"
    residual_plot = plot_dir / "migration_timing_residuals.png"

    yearly = (
        df.groupby(["species_code", "year"], as_index=False)["day_of_year"]
        .mean()
        .sort_values(["species_code", "year"])
    )

    top_species = (
        df["species_code"].value_counts().head(8).index.tolist()
    )
    yearly = yearly[yearly["species_code"].isin(top_species)]

    fig, ax = plt.subplots(figsize=(11, 5))
    for species_code, group in yearly.groupby("species_code"):
        ax.plot(group["year"], group["day_of_year"], marker="o", linewidth=1.5, label=species_code)

    ax.set_title("Migration timing by year (mean day-of-year)")
    ax.set_xlabel("Year")
    ax.set_ylabel("Day of year")
    if not yearly.empty:
        ax.legend(loc="best", fontsize=8, ncol=2)
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(timing_plot, dpi=150)
    plt.close(fig)

    observed = pd.Series(df["day_of_year"], dtype=float).reset_index(drop=True)

    try:
        fitted = pd.Series(result.fittedvalues, dtype=float).reset_index(drop=True)
        residuals = pd.Series(result.resid, dtype=float).reset_index(drop=True)
    except Exception:
        # Mixed model fits can succeed but random-effect reconstruction may fail
        # when covariance is singular; fallback to fixed-effect prediction residuals.
        predicted = pd.Series(result.predict(df), dtype=float).reset_index(drop=True)
        aligned_len = min(len(observed), len(predicted))
        if aligned_len == 0:
            raise ValueError("Could not compute residuals for plotting.")
        fitted = predicted.iloc[:aligned_len]
        residuals = observed.iloc[:aligned_len] - predicted.iloc[:aligned_len]

    fig2, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.5))
    ax1.hist(residuals, bins=30)
    ax1.set_title("Residual distribution")
    ax1.set_xlabel("Residual")
    ax1.set_ylabel("Frequency")

    ax2.scatter(fitted, residuals, s=14, alpha=0.65)
    ax2.axhline(0, linewidth=1)
    ax2.set_title("Residuals vs fitted")
    ax2.set_xlabel("Fitted")
    ax2.set_ylabel("Residual")

    fig2.tight_layout()
    fig2.savefig(residual_plot, dpi=150)
    plt.close(fig2)

    return timing_plot, residual_plot


def write_outputs(
    effects_df: pd.DataFrame,
    output_dir: Path,
    summary_payload: dict[str, object],
) -> tuple[Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "migration_timing_effects.csv"
    json_path = output_dir / "migration_timing_effects.json"
    summary_path = output_dir / "migration_timing_summary.md"

    effects_df.to_csv(csv_path, index=False)

    json_payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        **summary_payload,
        "effects": effects_df.to_dict(orient="records"),
    }
    json_path.write_text(json.dumps(json_payload, indent=2), encoding="utf-8")

    lines = [
        "# Migration Timing Shift Summary",
        "",
        f"- Generated at: {json_payload['generated_at']}",
        f"- Database: {summary_payload['db_path']}",
        f"- Rows used in analysis: {summary_payload['rows_used']}",
        f"- Species count: {summary_payload['species_count']}",
        f"- Deployment groups: {summary_payload['deployment_count']}",
        f"- Formula: {summary_payload['formula']}",
        f"- Models requested: {summary_payload['model_requested']}",
        f"- Models succeeded: {', '.join(summary_payload['models_succeeded']) if summary_payload['models_succeeded'] else 'none'}",
        "",
        "## Key Notes",
        "",
        "- This analysis is observational and should not be interpreted as causal climate proof.",
        "- Mixed-effects model uses deployment_id random intercept to reduce repeated-measure bias.",
        "- If climate variables are included, they are currently joined by date and require separate spatial QA.",
        "",
        "## Selected Effects",
        "",
        "| Model | Term | Estimate | p-value |",
        "|---|---|---:|---:|",
    ]

    key_terms = effects_df[
        effects_df["term"].str.contains("centered_year", regex=False)
        | effects_df["term"].str.contains("species_code", regex=False)
    ].copy()
    if key_terms.empty:
        key_terms = effects_df.head(10).copy()

    for _, row in key_terms.iterrows():
        lines.append(
            f"| {row['model']} | {row['term']} | {row['estimate']:.6f} | {row['p_value']:.6g} |"
        )

    summary_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return csv_path, json_path, summary_path


def main() -> int:
    args = parse_args()

    if not (0 < args.alpha < 1):
        print("Validation error: --alpha must be between 0 and 1.")
        return EXIT_VALIDATION_ERROR
    if not (0 <= args.min_climate_coverage <= 1):
        print("Validation error: --min-climate-coverage must be between 0 and 1.")
        return EXIT_VALIDATION_ERROR

    db_path = resolve_path(args.db_path)
    output_dir = resolve_path(args.output_dir)
    plot_dir = resolve_path(args.plot_dir)

    try:
        base_df = load_base_data(db_path)
        analysis_df, qc = build_analysis_frame(base_df)
        analysis_df, filter_summary = apply_filters(analysis_df, args)

        requested_climate_vars = parse_climate_vars(args.climate_vars)
        climate_vars: list[str] = []
        climate_coverage: dict[str, float] = {}

        if args.climate_csv:
            climate_csv = resolve_path(args.climate_csv)
            analysis_df, climate_vars, climate_coverage = merge_climate(
                analysis_df,
                climate_csv,
                requested_climate_vars,
                args.min_climate_coverage,
            )

        formula = build_formula(analysis_df, climate_vars)

        results: dict[str, object] = {}
        model_errors: dict[str, str] = {}

        if args.model in {"ols", "both"}:
            try:
                results["ols"] = fit_ols(analysis_df, formula)
            except Exception as exc:  # noqa: BLE001
                model_errors["ols"] = str(exc)

        if args.model in {"mixed", "both"}:
            try:
                results["mixed"] = fit_mixed(analysis_df, formula)
            except Exception as exc:  # noqa: BLE001
                model_errors["mixed"] = str(exc)

        if not results:
            print("Model error: no requested model completed successfully.")
            for name, msg in model_errors.items():
                print(f"- {name}: {msg}")
            return EXIT_MODEL_ERROR

        effects_frames = [
            summarize_effects(result, model_name, args.alpha)
            for model_name, result in results.items()
        ]
        effects_df = pd.concat(effects_frames, ignore_index=True)

        preferred_result = results.get("mixed", results.get("ols"))
        timing_plot, residual_plot = save_plots(analysis_df, preferred_result, plot_dir)

        summary_payload = {
            "db_path": str(db_path),
            "rows_used": int(len(analysis_df)),
            "species_count": int(analysis_df["species_code"].nunique()),
            "deployment_count": int(analysis_df["deployment_id"].nunique()),
            "formula": formula,
            "model_requested": args.model,
            "models_succeeded": list(results.keys()),
            "model_errors": model_errors,
            "qc": qc,
            "filter_summary": filter_summary,
            "climate_vars": climate_vars,
            "climate_coverage": climate_coverage,
            "plot_files": [str(timing_plot), str(residual_plot)],
        }

        csv_path, json_path, summary_path = write_outputs(effects_df, output_dir, summary_payload)

        print("=== STATS MODELS SUMMARY ===")
        print(f"Database: {db_path}")
        print(f"Rows used: {len(analysis_df)}")
        print(f"Species: {analysis_df['species_code'].nunique()}")
        print(f"Deployments: {analysis_df['deployment_id'].nunique()}")
        print(f"Formula: {formula}")
        print(f"Models succeeded: {list(results.keys())}")
        if model_errors:
            print(f"Model errors: {model_errors}")
        print(f"Effects CSV: {csv_path}")
        print(f"Effects JSON: {json_path}")
        print(f"Summary MD: {summary_path}")
        print(f"Plot: {timing_plot}")
        print(f"Plot: {residual_plot}")

        return 0

    except (FileNotFoundError, ValueError) as exc:
        print(f"Validation error: {exc}")
        return EXIT_VALIDATION_ERROR
    except Exception as exc:  # noqa: BLE001
        print(f"Unexpected error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
