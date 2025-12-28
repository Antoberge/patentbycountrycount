from __future__ import annotations

import pandas as pd


def fractional_counts_by_inventor_country(
    regpat_filtered: pd.DataFrame,
    filing_date_column: str = "filing_date",
) -> pd.DataFrame:
    """
    Fractional patent counts by inventor country *and year*.

    filing_date_column must contain integers or strings shaped YYYYMMDD.
    """
    df = regpat_filtered.copy()
    df = df.dropna(subset=["ctry_code", "inv_share", filing_date_column])
    df = df[df["inv_share"] > 0]

    df["year"] = df[filing_date_column].astype(str).str.slice(0, 4)
    df = df[df["year"].str.fullmatch(r"\d{4}")]

    out = (
        df.groupby(["ctry_code", "year"], as_index=False)["inv_share"]
        .sum()
        .rename(
            columns={
                "ctry_code": "inventor_country",
                "inv_share": "fractional_patents",
                "year": "filing_year",
            }
        )
        .sort_values(["filing_year", "fractional_patents"], ascending=[True, False])
        .reset_index(drop=True)
    )
    out["filing_year"] = out["filing_year"].astype(int)
    return out
