# Pipeline quickstart

## 1. Environment
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 2. Credentials
1. Copy `.env.example` → `.env`.
2. Fill `GCP_PROJECT_ID`, `GOOGLE_APPLICATION_CREDENTIALS` (absolute path to the service-account JSON) and optionally `BQ_LOCATION`.
3. Ensure the service account can read the tables referenced in `queries/`.

## 3. Data inputs
- Drop OECD `regpat.txt` into `data/raw/`.
- Place your BigQuery SQL query in `queries/biotech.sql` (or pass another file via `--query-file`).

## 4. Run

```bash
PYTHONPATH=src python -m biotech.cli run \
  --query-file queries/biotech.sql \
  --regpat-file data/raw/regpat.txt \
  --out-dir data/output \
  --cache-dir data/processed
```

Flags:
- `--chunksize` adjusts RegPat streaming size (default `1_000_000`).
- Change `--out-dir` / `--cache-dir` if you want different folders.

Outputs:
- `data/output/inventor_country_fractional_counts.csv`
- `data/output/run_metadata.json`
- cached intermediates under `data/processed/`

## 5. Generate charts/tables

```bash
PYTHONPATH=src python -m biotech.cli report \
  --input-csv data/output/inventor_country_yearly_fractional_counts.csv \
  --out-dir reports \
  --recent-start 2010
```

This saves line charts, stacked-share charts, and a CSV of top patenting countries to the chosen output directory.
