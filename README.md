# RegPat + BigQuery Pipeline

End-to-end workflow for extracting biotech-related PCT publications via BigQuery, reproducing the Stata cleaning that builds `pct_nbr`, joining to OECD RegPat, and producing inventor-country fractional counts plus charts.

## Contents
- `queries/biotech.sql` – BigQuery query to customize
- `src/biotech/` – CLI, helper modules, analytics
- `data/raw/` – place `regpat.txt` (and other raw files) here
- `data/processed/` – cached BigQuery and RegPat intermediates
- `data/output/` – final CSV + metadata from the run command
- `reports/` – charts/tables created by the reporting command

## Prerequisites
1. Python env
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
2. Credentials
   - Copy `.env.example` to `.env`
   - Fill `GCP_PROJECT_ID`, `GOOGLE_APPLICATION_CREDENTIALS` (absolute path to service-account JSON), optional `BQ_LOCATION`
   - Service account must read `patents-public-data`
3. Data inputs
   - Save OECD `regpat.txt` to `data/raw/`
   - Update `queries/biotech.sql` (or pass another SQL file)

## Run the pipeline
```bash
PYTHONPATH=src python -m biotech.cli run \
  --query-file queries/biotech.sql \
  --regpat-file data/raw/regpat.txt \
  --out-dir data/output \
  --cache-dir data/processed \
  --chunksize 1000000 \
  --regpat-sep '|'
```
Outputs:
- `data/output/inventor_country_yearly_fractional_counts.csv`
- `data/output/run_metadata.json`
- cached intermediates in `data/processed/`

## Generate charts / tables
```bash
PYTHONPATH=src python -m biotech.cli report \
  --input-csv data/output/inventor_country_yearly_fractional_counts.csv \
  --out-dir reports \
  --recent-start 2010
```
This produces line charts, stacked-share charts, and `reports/top_patenters.csv` (totals for the full period and since 2010).

## Push to GitHub
1. Ensure the remote points to your repo (`git remote -v`).
2. Stage/commit (data/ + models/ remain ignored by `.gitignore`).
   ```bash
   git add .
   git commit -m "Run biotech pipeline"
   git push -u origin main
   ```
