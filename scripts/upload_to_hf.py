"""
Export SQLite and FRED data to parquet files and upload to Hugging Face Datasets.

Usage:
    python scripts/upload_to_hf.py

Requires:
    pip install huggingface_hub pyarrow

You must be logged in to Hugging Face:
    huggingface-cli login
"""

import sqlite3
import sys
from pathlib import Path

import pandas as pd
from huggingface_hub import HfApi, create_repo

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.y9c.fred_data import load_fred_data
import run_forecasts

DB_PATH = Path(__file__).parent.parent / "data" / "usaa_y9c.db"
HF_REPO_ID = "Wgurtler/y9c-data"
EXPORT_DIR = Path(__file__).parent.parent / "data" / "hf_export"


def get_financial_cutoff_year(conn, years):
    latest_year = pd.read_sql_query(
        "SELECT MAX(year) AS latest_year FROM financial_data",
        conn,
    ).iloc[0]["latest_year"]

    if pd.isna(latest_year):
        raise ValueError("No financial_data records found in the database.")

    return int(latest_year) - years + 1


def export_financial_data(conn, years=None):
    if years is None:
        print("Exporting financial data (full history)...")
        year_filter = ""
    else:
        cutoff = get_financial_cutoff_year(conn, years)
        print(f"Exporting financial data (last {years} years: {cutoff}+)...")
        year_filter = f"WHERE fd.year >= {cutoff}"

    query = f"""
        SELECT fd.rssd_id, fd.report_date, fd.year, fd.quarter,
               fd.mdrm_code, fd.value,
               ad.account_name, ad.statement_type, ad.category
        FROM financial_data fd
        JOIN account_definitions ad ON fd.mdrm_code = ad.mdrm_code
        {year_filter}
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


def export_fred_data(start_date="2002-01-01"):
    print("Exporting FRED economic data...")
    fred = load_fred_data(start_date=start_date)

    if not fred:
        print("  Skipping FRED export: no FRED data available")
        return None

    df = pd.DataFrame(fred)
    df.index.name = "report_date"
    df = df.reset_index()
    df["report_date"] = pd.to_datetime(df["report_date"])

    out = EXPORT_DIR / "fred_data.parquet"
    df.to_parquet(out, index=False, compression="snappy")
    print(f"  Saved {len(df):,} rows -> {out}")
    return out


def add_optional_exports(files):
    optional_names = [
        "bank_financial_data.parquet",
        "bank_institutions.parquet",
        "forecast_metrics.parquet",
        "forecast_predictions.parquet",
        "forecast_future.parquet",
        "forecast_importance.parquet",
    ]

    for name in optional_names:
        path = EXPORT_DIR / name
        if path.exists():
            print(f"Including optional export: {name}")
            files.append(path)


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
            export_financial_data(conn, years=None),
            export_institutions(conn),
        ]
    finally:
        conn.close()

    fred_file = export_fred_data()
    if fred_file is not None:
        files.append(fred_file)

    print("\nRe-running XGBoost forecasts on the freshly exported data...")
    try:
        run_forecasts.main(export_dir=EXPORT_DIR)
    except Exception as exc:
        print(f"  WARNING: forecast batch run failed, skipping forecast exports: {exc}")

    add_optional_exports(files)

    upload_to_hf(files)
