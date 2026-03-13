"""
Export SQLite database to parquet files and upload to Hugging Face Datasets.

Usage:
    python scripts/upload_to_hf.py

Requires:
    pip install huggingface_hub pyarrow

You must be logged in to Hugging Face:
    huggingface-cli login
"""

import sqlite3
import pandas as pd
from pathlib import Path
from huggingface_hub import HfApi, create_repo

DB_PATH = Path(__file__).parent.parent / "data" / "usaa_y9c.db"
HF_REPO_ID = "Wgurtler/y9c-data"
EXPORT_DIR = Path(__file__).parent.parent / "data" / "hf_export"


def export_financial_data(conn):
    print("Exporting financial data...")
    query = """
        SELECT fd.rssd_id, fd.report_date, fd.year, fd.quarter,
               fd.mdrm_code, fd.value,
               ad.account_name, ad.statement_type, ad.category
        FROM financial_data fd
        JOIN account_definitions ad ON fd.mdrm_code = ad.mdrm_code
        ORDER BY fd.rssd_id, fd.year, fd.quarter
    """
    df = pd.read_sql_query(query, conn)
    df["report_date"] = pd.to_datetime(df["report_date"])
    out = EXPORT_DIR / "financial_data.parquet"
    df.to_parquet(out, index=False, compression="snappy")
    print(f"  Saved {len(df):,} rows -> {out} ({out.stat().st_size / 1e6:.1f} MB)")
    return out


def export_institutions(conn):
    print("Exporting institutions...")
    df = pd.read_sql_query("SELECT * FROM institutions", conn)
    out = EXPORT_DIR / "institutions.parquet"
    df.to_parquet(out, index=False, compression="snappy")
    print(f"  Saved {len(df):,} rows -> {out}")
    return out


def upload_to_hf(files):
    print(f"\nUploading to Hugging Face: {HF_REPO_ID}")
    api = HfApi()

    create_repo(
        repo_id=HF_REPO_ID,
        repo_type="dataset",
        exist_ok=True,
        private=False,
    )

    for path in files:
        print(f"  Uploading {path.name}...")
        api.upload_file(
            path_or_fileobj=str(path),
            path_in_repo=path.name,
            repo_id=HF_REPO_ID,
            repo_type="dataset",
        )
        print(f"  Done: {path.name}")

    print(f"\nDataset available at: https://huggingface.co/datasets/{HF_REPO_ID}")


if __name__ == "__main__":
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    try:
        files = [
            export_financial_data(conn),
            export_institutions(conn),
        ]
    finally:
        conn.close()

    upload_to_hf(files)
