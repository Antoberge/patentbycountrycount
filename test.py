import pandas as pd

pct = pd.read_csv("data/processed/pct_from_bq.csv")          # from step 2 (pct list)
reg = pd.read_parquet("data/processed/regpat_filtered.parquet")

pct["filing_year"] = pct["filing_date"].astype(str).str[:4].astype(int)
matched_pct = reg["pct_nbr"].unique()
pct["matched"] = pct["pct_nbr"].isin(matched_pct)

summary = pct.groupby(["filing_year", "matched"]).size().unstack(fill_value=0)
print(summary.tail(15))