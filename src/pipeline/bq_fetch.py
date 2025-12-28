from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import pandas as pd
from google.cloud import bigquery


@dataclass(frozen=True)
class BQConfig:
    project_id: str
    location: str = "US"


def run_query_from_file(query_file: Path, cfg: BQConfig) -> pd.DataFrame:
    query = query_file.read_text(encoding="utf-8")
    client = bigquery.Client(project=cfg.project_id, location=cfg.location)

    job = client.query(query)
    df = job.result().to_dataframe(create_bqstorage_client=True)

    if "publication_number" not in df.columns:
        raise ValueError("Your BigQuery result must include a 'publication_number' column.")

    return df
