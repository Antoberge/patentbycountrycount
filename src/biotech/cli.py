from __future__ import annotations

import json
import os
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import typer
from rich import print
from dotenv import load_dotenv
import matplotlib.pyplot as plt

from .bq_fetch import BQConfig, run_query_from_file
from .transform import stata_like_pct_nbr
from .regpat import load_regpat_filtered
from .analysis import fractional_counts_by_inventor_country


EU27_CODES = [
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE",
    "GR", "HU", "IE", "IT", "LV", "LT", "LU", "MT", "NL", "PL", "PT",
    "RO", "SK", "SI", "ES", "SE",
]
GROUP_DEFINITIONS = [
    ("US", ["US"]),
    ("JP", ["JP"]),
    ("CN", ["CN"]),
    ("UK", ["UK", "GB"]),
    ("EU27", EU27_CODES),
]


app = typer.Typer(add_completion=False)


@app.command()
def run(
    query_file: Path = typer.Option(..., exists=True, help="Path to BigQuery SQL file."),
    regpat_file: Path = typer.Option(..., exists=True, help="Path to OECD regpat.txt"),
    out_dir: Path = typer.Option(Path("data/output"), help="Output directory."),
    cache_dir: Path = typer.Option(Path("data/processed"), help="Cache directory."),
    chunksize: int = typer.Option(1_000_000, help="Chunk size for regpat reading."),
    regpat_sep: str = typer.Option("\t", help="Column separator for regpat file (default tab)."),
):
    """
    Runs the full pipeline:
      1) BQ query -> df
      2) Stata-like cleaning -> pct list
      3) Filter RegPat to those pct_nbr
      4) Fractional counts by inventor country
    """
    load_dotenv()

    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise typer.BadParameter("Missing GCP_PROJECT_ID. Put it in your .env or environment variables.")

    location = os.getenv("BQ_LOCATION", "US")

    out_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # 1) Fetch BQ data
    print(f"[bold]Running BigQuery query[/bold] from {query_file} ...")
    bq_df = run_query_from_file(query_file, BQConfig(project_id=project_id, location=location))
    bq_cache = cache_dir / "bq_raw.parquet"
    bq_df.to_parquet(bq_cache, index=False)
    print(f"Saved BQ raw to {bq_cache}")

    # 2) Transform to pct_nbr
    print("[bold]Applying Stata-like cleaning[/bold] to build pct_nbr ...")
    pct_df = stata_like_pct_nbr(bq_df, publication_col="publication_number")
    pct_cache = cache_dir / "pct_from_bq.csv"
    pct_df.to_csv(pct_cache, index=False)
    print(f"Saved pct list to {pct_cache} (n={len(pct_df):,})")

    # 3) Load & filter regpat
    print("[bold]Loading RegPat in chunks and filtering[/bold] ...")
    regpat_filtered = load_regpat_filtered(
        regpat_file,
        pct_df["pct_nbr"].tolist(),
        chunksize=chunksize,
        separator=regpat_sep,
    )
    regpat_filtered = regpat_filtered.merge(
        pct_df[["pct_nbr", "filing_date"]],
        on="pct_nbr",
        how="left",
    )
    regpat_cache = cache_dir / "regpat_filtered.parquet"
    regpat_filtered.to_parquet(regpat_cache, index=False)
    print(f"Saved filtered RegPat to {regpat_cache} (rows={len(regpat_filtered):,})")

    # 4) Fractional counts
    print("[bold]Computing fractional counts by inventor country[/bold] ...")
    counts = fractional_counts_by_inventor_country(regpat_filtered)
    out_csv = out_dir / "inventor_country_yearly_fractional_counts.csv"
    counts.to_csv(out_csv, index=False)
    print(f"Saved results to {out_csv}")

    # Metadata
    meta = {
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "gcp_project_id": project_id,
        "bq_location": location,
        "query_file": str(query_file),
        "regpat_file": str(regpat_file),
        "n_pct_unique": int(len(pct_df)),
        "n_regpat_rows_kept": int(len(regpat_filtered)),
        "outputs": {"inventor_country_yearly_fractional_counts_csv": str(out_csv)},
    }
    meta_path = out_dir / "run_metadata.json"
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(f"Saved metadata to {meta_path}")


@app.command()
def report(
    input_csv: Path = typer.Option(
        Path("data/output/inventor_country_yearly_fractional_counts.csv"),
        exists=True,
        help="CSV produced by the run command.",
    ),
    out_dir: Path = typer.Option(Path("reports"), help="Folder for charts and tables."),
    recent_start: int = typer.Option(2010, help="Start year for recent totals."),
):
    """Generate plots and summary tables from the fractional counts CSV."""

    out_dir.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(input_csv)
    required_cols = {"inventor_country", "filing_year", "fractional_patents"}
    if not required_cols.issubset(df.columns):
        raise typer.BadParameter(
            f"Input CSV must contain {', '.join(sorted(required_cols))}."
        )
    df["filing_year"] = df["filing_year"].astype(int)

    ts = _build_group_series(df)
    ts_path = out_dir / "timeseries_selected_countries.png"
    _plot_timeseries(ts, ts_path, "Fractional patents by country group")
    print(f"Saved time-series chart to {ts_path}")

    stack_path = out_dir / "timeseries_selected_countries_share.png"
    _plot_stacked_share(ts, stack_path, "Share of fractional patents by country group")
    print(f"Saved stacked share chart to {stack_path}")

    table = _build_top_table(df, recent_start)
    table_path = out_dir / "top_patenters.csv"
    table.to_csv(table_path, index=False)
    print(f"Saved top patenters table to {table_path}")


def _build_group_series(df: pd.DataFrame) -> pd.DataFrame:
    pivot = (
        df.pivot_table(
            index="filing_year",
            columns="inventor_country",
            values="fractional_patents",
            aggfunc="sum",
            fill_value=0,
        )
        .sort_index()
    )
    groups = {}
    for label, codes in GROUP_DEFINITIONS:
        existing = [code for code in codes if code in pivot.columns]
        if existing:
            groups[label] = pivot[existing].sum(axis=1)
        else:
            groups[label] = pd.Series(0.0, index=pivot.index)
    if groups:
        selected_sum = sum(groups.values())
        selected_sum = selected_sum.reindex(pivot.index, fill_value=0)
    else:
        selected_sum = pd.Series(0.0, index=pivot.index)
    total = pivot.sum(axis=1)
    groups["Rest of World"] = (total - selected_sum).clip(lower=0)
    return pd.DataFrame(groups)


def _plot_timeseries(ts: pd.DataFrame, path: Path, title: str) -> None:
    plt.figure(figsize=(11, 5))
    for col in ts.columns:
        plt.plot(ts.index, ts[col], label=col)
    plt.xlabel("Filing year")
    plt.ylabel("Fractional patents")
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    plt.savefig(path, dpi=300)
    plt.close()


def _plot_stacked_share(ts: pd.DataFrame, path: Path, title: str) -> None:
    share = ts.div(ts.sum(axis=1), axis=0).fillna(0)
    plt.figure(figsize=(11, 5))
    plt.stackplot(
        share.index,
        [share[col] for col in share.columns],
        labels=share.columns,
    )
    plt.xlabel("Filing year")
    plt.ylabel("Share of fractional patents")
    plt.title(title)
    plt.legend(loc="upper center", ncol=3)
    plt.tight_layout()
    plt.savefig(path, dpi=300)
    plt.close()


def _build_top_table(df: pd.DataFrame, recent_start: int) -> pd.DataFrame:
    min_year = int(df["filing_year"].min())
    max_year = int(df["filing_year"].max())
    overall_col = f"fractional_{min_year}_{max_year}"
    recent_col = f"fractional_{recent_start}_{max_year}"

    overall = df.groupby("inventor_country")["fractional_patents"].sum()
    recent = (
        df[df["filing_year"] >= recent_start]
        .groupby("inventor_country")["fractional_patents"]
        .sum()
    )

    table = pd.DataFrame({overall_col: overall, recent_col: recent}).fillna(0)
    table = table.sort_values(overall_col, ascending=False).reset_index()
    table = table.rename(columns={"inventor_country": "country"})
    return table


if __name__ == "__main__":
    app()
