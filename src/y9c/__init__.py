"""
USAA Y-9C Financial Data Scraper

A package for downloading, processing, and analyzing FR Y-9C regulatory filings
for bank holding companies.
"""

from .config import (
    USAA_HOLDING_COMPANY_RSSD,
    USAA_FSB_RSSD,
    BALANCE_SHEET_ITEMS,
    INCOME_STATEMENT_ITEMS,
    get_all_mdrm_codes,
    get_mdrm_codes_list,
)

from .database import (
    initialize_database,
    get_connection,
    get_balance_sheet,
    get_income_statement,
    get_time_series,
    get_all_periods,
    export_to_csv,
    bulk_insert_financial_data,
)

from .loader import (
    load_quarter,
    load_all_data,
    incremental_update,
    validate_data,
)

from .downloader import (
    download_all_y9c_data,
    check_existing_data,
    generate_download_instructions,
)

__version__ = "1.0.0"
__all__ = [
    # Config
    "USAA_HOLDING_COMPANY_RSSD",
    "USAA_FSB_RSSD",
    "BALANCE_SHEET_ITEMS",
    "INCOME_STATEMENT_ITEMS",
    "get_all_mdrm_codes",
    "get_mdrm_codes_list",
    # Database
    "initialize_database",
    "get_connection",
    "get_balance_sheet",
    "get_income_statement",
    "get_time_series",
    "get_all_periods",
    "export_to_csv",
    "bulk_insert_financial_data",
    # Loader
    "load_quarter",
    "load_all_data",
    "incremental_update",
    "validate_data",
    # Downloader
    "download_all_y9c_data",
    "check_existing_data",
    "generate_download_instructions",
]
