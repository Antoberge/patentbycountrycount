from __future__ import annotations

import pandas as pd


def _fix_wo_century(pub: str) -> str:
    """
    Replicates:
      replace publication_number = "WO-20"+substr(publication_number,4,.) if substr(...,1,4)=="WO-0"
      replace publication_number = "WO-19"+substr(publication_number,4,.) if substr(...,1,4) in {"WO-7","WO-8","WO-9"}
    """
    if not isinstance(pub, str):
        return pub

    if pub.startswith("WO-0"):
        # "WO-20" + pub[3:] where pub[3:] starts at the 4th char in Stata (1-based)
        return "WO-20" + pub[3:]
    if pub.startswith("WO-7") or pub.startswith("WO-8") or pub.startswith("WO-9"):
        return "WO-19" + pub[3:]
    return pub


def stata_like_pct_nbr(
    df: pd.DataFrame,
    publication_col: str = "publication_number",
    extra_columns: list[str] | None = None,
) -> pd.DataFrame:
    out = df.copy()

    # Fix century in WO numbers
    out[publication_col] = out[publication_col].map(_fix_wo_century)

    # Remove hyphens
    pct = out[publication_col].astype(str).str.replace("-", "", regex=False)

    # Split at 'A' and keep left part
    pct_left = pct.str.split("A", n=1, expand=True)[0]

    # Pad if length == 11 by inserting '0' after 6th character
    lengths = pct_left.str.len()
    pct_left = pct_left.where(lengths != 11, pct_left.str.slice(0, 6) + "0" + pct_left.str.slice(6))

    data = {"pct_nbr": pct_left}
    extras = extra_columns or []
    for col in extras:
        if col in out.columns:
            data[col] = out[col].values

    pct_df = pd.DataFrame(data)
    subset_cols = ["pct_nbr"]
    if "filing_date" in pct_df.columns:
        subset_cols.append("filing_date")
    pct_df = pct_df.dropna(subset=subset_cols)
    pct_df = pct_df[pct_df["pct_nbr"].str.len() >= 10]
    pct_df = pct_df.drop_duplicates(subset=["pct_nbr"], keep="first").reset_index(drop=True)
    return pct_df
