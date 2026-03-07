"""
FRED Economic Data Module

Fetches and caches key economic series from the St. Louis Fed API.
API key loaded from .env file at project root.
"""
import os
from pathlib import Path

# Series definitions: display_name -> (fred_id, unit_label, is_cpi_yoy)
FRED_SERIES = {
    "Fed Funds Rate":           ("FEDFUNDS",      "%",       False),
    "2Y Treasury Yield":        ("DGS2",           "%",       False),
    "10Y Treasury Yield":       ("DGS10",          "%",       False),
    "Yield Curve (10Y-2Y)":     ("T10Y2Y",         "%",       False),
    "Unemployment Rate":        ("UNRATE",         "%",       False),
    "CPI Inflation (YoY %)":    ("CPIAUCSL",       "% YoY",   True),
    "HY Credit Spread (OAS)":   ("BAMLH0A0HYM2",   "%",       False),
    "IG Credit Spread (OAS)":   ("BAMLC0A0CM",     "%",       False),
    "Mortgage Delinquency":     ("DRSFRMACBS",     "%",       False),
    "Credit Card Delinquency":  ("DRCCLACBS",      "%",       False),
}

FRED_SERIES_NAMES = list(FRED_SERIES.keys())

_CACHE = None


def load_fred_data(start_date="2002-01-01"):
    """Fetch all configured FRED series, resampled to quarterly. Cached after first call."""
    global _CACHE
    if _CACHE is not None:
        return _CACHE

    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent / ".env")
    from fredapi import Fred

    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        print("FRED_API_KEY not found in .env — economic data unavailable")
        _CACHE = {}
        return _CACHE

    fred = Fred(api_key=api_key)
    data = {}

    for name, (series_id, unit, is_cpi_yoy) in FRED_SERIES.items():
        try:
            s = fred.get_series(series_id, observation_start=start_date).dropna()
            s_q = s.resample("QE").mean()
            if is_cpi_yoy:
                s_q = s_q.pct_change(4) * 100
                s_q = s_q.dropna()
            data[name] = s_q
        except Exception as e:
            print(f"FRED: failed to fetch {series_id} ({name}): {e}")

    _CACHE = data
    print(f"FRED: loaded {len(data)} series")
    return data


def get_unit(series_name):
    entry = FRED_SERIES.get(series_name)
    return entry[1] if entry else "%"
