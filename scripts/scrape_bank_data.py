"""
Scrape FDIC Call Report financials for subsidiary banks linked to tracked BHCs.

Outputs:
- data/hf_export/bank_financial_data.parquet
- data/hf_export/bank_institutions.parquet

The resulting schema matches the Streamlit app's expected long-format fields.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests

FDIC_FINANCIALS_API = "https://banks.data.fdic.gov/api/financials"
HF_REPO_ID = "Wgurtler/y9c-data"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
EXPORT_DIR = PROJECT_ROOT / "data" / "hf_export"
MIN_REPORT_DATE = 20000331
PAGE_SIZE = 10_000

# FDIC field -> (MDRM code, account_name, statement_type, category)
FIELD_MAP = {
    "ASSET": ("BHCK2170", "Total assets", "balance_sheet", "assets"),
    "LIAB": ("BHCK2948", "Total liabilities", "balance_sheet", "liabilities"),
    "EQ": ("BHCK3210", "Total equity capital", "balance_sheet", "equity"),
    "LNLSNET": ("BHCKB528", "Loans and leases, net of allowance and reserve", "balance_sheet", "assets"),
    "DEP": ("BHDM6636", "Deposits in domestic offices - Interest-bearing", "balance_sheet", "liabilities"),
    "INTINC": ("BHCK4010", "Total interest income", "income_statement", "interest_income"),
    "EINTEXP": ("BHCK4073", "Total interest expense", "income_statement", "interest_expense"),
    "NONII": ("BHCK4079", "Total noninterest income", "income_statement", "noninterest_income"),
    "NONIX": ("BHCK4093", "Total noninterest expense", "income_statement", "noninterest_expense"),
    "NETINC": ("BHCK4301", "Income before income taxes and extraordinary items", "income_statement", "income"),
    "RBC1AAJ": ("FDIC_RBC1AAJ", "Tier 1 risk-based capital ratio (%)", "capital", "capital_ratio"),
    "RBCRWAJ": ("FDIC_RBCRWAJ", "Total risk-based capital ratio (%)", "capital", "capital_ratio"),
    "IDT1RWAJR": ("FDIC_IDT1RWAJR", "Common equity tier 1 (CET1) capital ratio (%)", "capital", "capital_ratio"),
    "IDT1CER": ("FDIC_IDT1CER", "Tier 1 capital ratio (%)", "capital", "capital_ratio"),
}


def _load_parent_bhc_ids() -> list[str]:
    local_fin_path = EXPORT_DIR / "financial_data.parquet"
    if local_fin_path.exists():
        fin = pd.read_parquet(local_fin_path, columns=["rssd_id"])
    else:
        fin = pd.read_parquet(
            f"hf://datasets/{HF_REPO_ID}/financial_data.parquet",
            columns=["rssd_id"],
        )

    ids = fin["rssd_id"].dropna().astype(str).str.strip()
    return sorted(set(ids))


def _fdic_fetch_parent(parent_rssd: str) -> list[dict]:
    fields = [
        "RSSDID", "RSSDHCR", "CERT", "NAME", "CITY", "STALP", "REPDTE",
        *FIELD_MAP.keys(),
    ]

    all_rows: list[dict] = []
    offset = 0

    while True:
        params = {
            "format": "json",
            "limit": PAGE_SIZE,
            "offset": offset,
            "filters": f"RSSDHCR:{parent_rssd}",
            "fields": ",".join(fields),
        }
        resp = requests.get(FDIC_FINANCIALS_API, params=params, timeout=90)
        resp.raise_for_status()
        payload = resp.json()

        rows = payload.get("data", [])
        if not rows:
            break

        all_rows.extend(row["data"] for row in rows)
        if len(rows) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    return all_rows


def _parse_report_date(rep: int | str) -> tuple[str, int, int] | None:
    if rep is None:
        return None
    text = str(rep)
    if len(text) != 8:
        return None

    try:
        dt = datetime.strptime(text, "%Y%m%d")
    except ValueError:
        return None

    year = dt.year
    quarter = (dt.month - 1) // 3 + 1
    return dt.strftime("%Y-%m-%d"), year, quarter


def _to_float(v) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _rows_to_financials(rows: Iterable[dict]) -> list[dict]:
    out: list[dict] = []
    for row in rows:
        rep = row.get("REPDTE")
        if rep is None or int(rep) < MIN_REPORT_DATE:
            continue

        parsed = _parse_report_date(rep)
        if parsed is None:
            continue

        report_date, year, quarter = parsed
        bank_rssd = str(row.get("RSSDID") or "").strip()
        if not bank_rssd:
            continue

        for fdic_field, (mdrm, name, stmt, cat) in FIELD_MAP.items():
            val = _to_float(row.get(fdic_field))
            if val is None:
                continue

            out.append(
                {
                    "rssd_id": bank_rssd,
                    "report_date": report_date,
                    "year": year,
                    "quarter": quarter,
                    "mdrm_code": mdrm,
                    "value": val,
                    "account_name": name,
                    "statement_type": stmt,
                    "category": cat,
                }
            )

        # Derived metric for net interest income when components exist.
        int_inc = _to_float(row.get("INTINC"))
        int_exp = _to_float(row.get("EINTEXP"))
        if int_inc is not None and int_exp is not None:
            out.append(
                {
                    "rssd_id": bank_rssd,
                    "report_date": report_date,
                    "year": year,
                    "quarter": quarter,
                    "mdrm_code": "BHCK4074",
                    "value": int_inc - int_exp,
                    "account_name": "Net interest income",
                    "statement_type": "income_statement",
                    "category": "net_interest_income",
                }
            )

    return out


def _rows_to_institutions(rows: Iterable[dict]) -> pd.DataFrame:
    recs = []
    for row in rows:
        bank_rssd = str(row.get("RSSDID") or "").strip()
        if not bank_rssd:
            continue

        recs.append(
            {
                "rssd_id": bank_rssd,
                "name": row.get("NAME"),
                "city": row.get("CITY"),
                "state": row.get("STALP"),
                "entity_type": "bank",
                "primary_regulator": "FDIC",
                "parent_rssd_id": str(row.get("RSSDHCR") or "").strip() or None,
                "cert": row.get("CERT"),
            }
        )

    if not recs:
        return pd.DataFrame(columns=[
            "rssd_id", "name", "city", "state", "entity_type",
            "primary_regulator", "parent_rssd_id", "cert",
        ])

    inst = pd.DataFrame(recs)
    inst = inst.drop_duplicates(subset=["rssd_id"]).sort_values("rssd_id").reset_index(drop=True)
    return inst


def main():
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    parent_ids = _load_parent_bhc_ids()
    print(f"Loaded {len(parent_ids)} parent BHC IDs")

    all_raw_rows: list[dict] = []
    for idx, parent in enumerate(parent_ids, 1):
        if idx % 25 == 0 or idx == 1 or idx == len(parent_ids):
            print(f"[{idx}/{len(parent_ids)}] Pulling parent RSSDHCR={parent}")

        try:
            rows = _fdic_fetch_parent(parent)
            all_raw_rows.extend(rows)
        except Exception as exc:
            print(f"  Failed parent {parent}: {exc}")

    print(f"Fetched {len(all_raw_rows):,} raw FDIC rows")

    financial_rows = _rows_to_financials(all_raw_rows)
    fin = pd.DataFrame(financial_rows)
    if fin.empty:
        raise RuntimeError("No bank financial rows were produced.")

    fin = fin.drop_duplicates(subset=["rssd_id", "report_date", "mdrm_code"])
    fin = fin.sort_values(["rssd_id", "year", "quarter", "mdrm_code"]).reset_index(drop=True)

    inst = _rows_to_institutions(all_raw_rows)

    fin_out = EXPORT_DIR / "bank_financial_data.parquet"
    inst_out = EXPORT_DIR / "bank_institutions.parquet"
    fin.to_parquet(fin_out, index=False, compression="snappy")
    inst.to_parquet(inst_out, index=False, compression="snappy")

    print(f"Saved {len(fin):,} bank financial rows -> {fin_out}")
    print(f"Saved {len(inst):,} bank institutions -> {inst_out}")
    print(f"Date range: {fin['report_date'].min()} to {fin['report_date'].max()}")
    print(f"Unique banks: {fin['rssd_id'].nunique():,}")


if __name__ == "__main__":
    main()
