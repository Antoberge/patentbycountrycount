from __future__ import annotations

from pathlib import Path
from typing import Iterable, Optional
import pandas as pd


REGPAT_USECOLS = ["pct_nbr", "ctry_code", "inv_share"]


def load_regpat_filtered(
    regpat_file: Path,
    pct_nbrs: Iterable[str],
    chunksize: int = 1_000_000,
    separator: str = "\t",
) -> pd.DataFrame:
    """
    Loads OECD regpat.txt (tab-delimited) in chunks and keeps only pct_nbr in pct_nbrs.
    This is important because regpat can be huge.

    Expected columns include:
      pct_nbr, ctry_code, inv_share
    """
    pct_set = set(pct_nbrs)

    kept = []
    for chunk in pd.read_csv(
        regpat_file,
        sep=separator,
        dtype={"pct_nbr": "string", "ctry_code": "string"},
        usecols=lambda c: c in set(REGPAT_USECOLS),
        chunksize=chunksize,
        low_memory=False,
    ):
        chunk = chunk.dropna(subset=["pct_nbr", "ctry_code"])
        chunk = chunk[chunk["pct_nbr"].isin(pct_set)]
        if not chunk.empty:
            # inv_share might be read as string depending on file quirks
            chunk["inv_share"] = pd.to_numeric(chunk["inv_share"], errors="coerce")
            kept.append(chunk)

    if not kept:
        return pd.DataFrame(columns=REGPAT_USECOLS)

    return pd.concat(kept, ignore_index=True)
