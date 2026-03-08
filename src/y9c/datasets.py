"""
Reusable data loading utilities for Y-9C and FRED economic data.

Functions
---------
load_y9c_wide       – Y-9C financials pivoted to wide format.
load_economic_wide  – FRED macro series in wide format.
load_joined         – Both datasets merged on quarter-end date.

Usage
-----
from src.y9c.datasets import load_joined, load_y9c_wide, load_economic_wide

df = load_joined()
df = load_joined(rssd_ids=["1447376"])   # USAA only
"""

import pandas as pd

try:
    from .database import get_connection
    from .fred_data import load_fred_data
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))
    from src.y9c.database import get_connection
    from src.y9c.fred_data import load_fred_data


# ---------------------------------------------------------------------------
# Y-9C
# ---------------------------------------------------------------------------

def load_y9c_wide(
    rssd_ids: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    use_account_names: bool = True,
) -> pd.DataFrame:
    """
    Load Y-9C financial data in wide format.

    Each row is one (rssd_id, report_date) pair.
    Each column is one MDRM code (or human-readable account name when
    *use_account_names* is True).

    Parameters
    ----------
    rssd_ids : list of str, optional
        Filter to specific institutions.  ``None`` returns all.
    start_year / end_year : int, optional
        Inclusive year bounds.
    use_account_names : bool
        Replace MDRM codes with readable account names in column headers.

    Returns
    -------
    pd.DataFrame
        Wide DataFrame with meta columns rssd_id, institution_name,
        report_date, year, quarter followed by one financial column each.
    """
    conn = get_connection()

    query = """
        SELECT
            fd.rssd_id,
            i.name             AS institution_name,
            fd.report_date,
            fd.year,
            fd.quarter,
            fd.mdrm_code,
            ad.account_name,
            fd.value
        FROM financial_data fd
        LEFT JOIN institutions i
               ON fd.rssd_id = i.rssd_id
        LEFT JOIN account_definitions ad
               ON fd.mdrm_code = ad.mdrm_code
        WHERE 1=1
    """
    params: list = []

    if rssd_ids:
        placeholders = ",".join("?" for _ in rssd_ids)
        query += f" AND fd.rssd_id IN ({placeholders})"
        params.extend(rssd_ids)

    if start_year is not None:
        query += " AND fd.year >= ?"
        params.append(start_year)

    if end_year is not None:
        query += " AND fd.year <= ?"
        params.append(end_year)

    long = pd.read_sql_query(query, conn, params=params)
    conn.close()

    if long.empty:
        return pd.DataFrame()

    long["report_date"] = pd.to_datetime(long["report_date"])

    value_col = "account_name" if use_account_names else "mdrm_code"
    meta_cols = ["rssd_id", "institution_name", "report_date", "year", "quarter"]

    wide = (
        long
        .pivot_table(
            index=meta_cols,
            columns=value_col,
            values="value",
            aggfunc="first",
        )
        .reset_index()
    )
    wide.columns.name = None
    return wide


# ---------------------------------------------------------------------------
# FRED / economic
# ---------------------------------------------------------------------------

def load_economic_wide(start_date: str = "2002-01-01") -> pd.DataFrame:
    """
    Load FRED economic series in wide format.

    Each row is one quarter-end date; each column is one economic series.

    Parameters
    ----------
    start_date : str
        Earliest observation date (ISO format) passed to FRED.

    Returns
    -------
    pd.DataFrame
        Columns: report_date, <series name>, ...
        Returns an empty DataFrame when FRED data is unavailable.
    """
    fred = load_fred_data(start_date=start_date)
    if not fred:
        return pd.DataFrame()

    econ = pd.DataFrame(fred)
    econ.index.name = "report_date"
    econ = econ.reset_index()
    econ["report_date"] = pd.to_datetime(econ["report_date"])
    # Normalise to quarter-end
    econ["report_date"] = econ["report_date"].dt.to_period("Q").dt.to_timestamp("Q")
    return econ


# ---------------------------------------------------------------------------
# Joined dataset
# ---------------------------------------------------------------------------

def load_joined(
    rssd_ids: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    use_account_names: bool = True,
    start_date: str = "2002-01-01",
) -> pd.DataFrame:
    """
    Load Y-9C data and FRED economic data joined by quarter-end date.

    Y-9C report dates are normalised to quarter-end to align with FRED
    quarterly series.  Economic columns carry the same value for every
    institution in a given quarter.

    Parameters
    ----------
    rssd_ids : list of str, optional
        Filter Y-9C data to specific institutions.
    start_year / end_year : int, optional
        Inclusive year bounds for Y-9C data.
    use_account_names : bool
        Use human-readable column names for Y-9C fields.
    start_date : str
        Earliest observation date for FRED series.

    Returns
    -------
    pd.DataFrame
        One row per (institution, quarter).  Y-9C financial columns on the
        left; FRED economic columns on the right.
    """
    y9c = load_y9c_wide(
        rssd_ids=rssd_ids,
        start_year=start_year,
        end_year=end_year,
        use_account_names=use_account_names,
    )

    if y9c.empty:
        print("No Y-9C data found — check database and filters.")
        return y9c

    # Normalise Y-9C dates to quarter-end to match FRED
    y9c["report_date"] = y9c["report_date"].dt.to_period("Q").dt.to_timestamp("Q")

    econ = load_economic_wide(start_date=start_date)

    if econ.empty:
        print("FRED data unavailable — returning Y-9C data only.")
        return y9c

    joined = y9c.merge(econ, on="report_date", how="left")

    print(
        f"Joined: {len(joined):,} rows | "
        f"{y9c.shape[1]} Y-9C cols + {econ.shape[1] - 1} econ cols"
    )
    return joined
