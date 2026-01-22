"""
Y-9C Data Configuration

Contains USAA identifiers, data mappings, and line item definitions for
income statement and balance sheet construction.

This file defines the MDRM codes needed to build financial statements
from FR Y-9C regulatory filings.
"""

# USAA Identifiers
USAA_HOLDING_COMPANY_RSSD = "1447376"  # United Services Automobile Association (parent)
USAA_FSB_RSSD = "619877"  # USAA Federal Savings Bank (subsidiary)

# Data source configuration
DATA_START_YEAR = 2000
DATA_END_YEAR = 2025  # Will be updated dynamically

# Chicago Fed historical data URL pattern (pre-2021)
CHICAGO_FED_URL = "https://www.chicagofed.org/api/sitecore/BHCHome/BHCDownload?SelectedQuarter={quarter}&SelectedYear={year}"

# NIC/FFIEC data URL pattern (2021+)
NIC_BULK_URL = "https://www.ffiec.gov/npw/FinancialReport/ReturnFinancialReportZip?rpt=BHC&date={date}"

# =============================================================================
# BALANCE SHEET LINE ITEMS (Schedule HC)
# =============================================================================
BALANCE_SHEET_ITEMS = {
    "assets": {
        "BHCK0081": "Cash and balances due from depository institutions - Noninterest-bearing",
        "BHCK0395": "Interest-bearing balances in U.S. offices",
        "BHCK0397": "Interest-bearing balances in foreign offices",
        "BHCK1754": "Held-to-maturity securities",
        "BHCK1773": "Available-for-sale debt securities",
        "BHCKJJ34": "Equity securities with readily determinable fair values",
        "BHDMB987": "Federal funds sold in domestic offices",
        "BHCKB989": "Securities purchased under agreements to resell",
        "BHCK5369": "Loans and leases held for sale",
        "BHCK2122": "Loans and leases, net of unearned income",
        "BHCK3123": "LESS: Allowance for loan and lease losses",
        "BHCKC781": "LESS: Allocated transfer risk reserve",
        "BHCKB528": "Loans and leases, net of allowance and reserve",
        "BHCK3545": "Trading assets",
        "BHCK2145": "Premises and fixed assets",
        "BHCK2150": "Other real estate owned",
        "BHCK2130": "Investments in unconsolidated subsidiaries",
        "BHCK2155": "Direct and indirect investments in real estate",
        "BHCK3163": "Intangible assets - Goodwill",
        "BHCK0426": "Intangible assets - Other",
        "BHCK2160": "Other assets",
        "BHCK2170": "Total assets",
    },
    "liabilities": {
        "BHDM6631": "Deposits in domestic offices - Noninterest-bearing",
        "BHDM6636": "Deposits in domestic offices - Interest-bearing",
        "BHFN6631": "Deposits in foreign offices - Noninterest-bearing",
        "BHFN6636": "Deposits in foreign offices - Interest-bearing",
        "BHDMB993": "Federal funds purchased in domestic offices",
        "BHCKB995": "Securities sold under agreements to repurchase",
        "BHCK3190": "Trading liabilities",
        "BHCK2332": "Other borrowed money (original maturity > 1 year)",
        "BHCKB571": "Other borrowed money (original maturity <= 1 year)",
        "BHCK3200": "Subordinated notes and debentures",
        "BHCK2750": "Other liabilities",
        "BHCK2948": "Total liabilities",
    },
    "equity": {
        "BHCK3838": "Perpetual preferred stock",
        "BHCK3230": "Common stock",
        "BHCK3839": "Surplus",
        "BHCK3632": "Retained earnings",
        "BHCKB530": "Accumulated other comprehensive income",
        "BHCKA130": "Other equity capital components",
        "BHCK3210": "Total equity capital",
        "BHCKG105": "Total equity attributable to parent",
        "BHCK3000": "Noncontrolling (minority) interests in consolidated subsidiaries",
        "BHCK3300": "Total liabilities and equity capital",
    },
}

# =============================================================================
# INCOME STATEMENT LINE ITEMS (Schedule HI)
# =============================================================================
INCOME_STATEMENT_ITEMS = {
    "interest_income": {
        "BHCK4107": "Interest income - Loans secured by real estate",
        "BHCK4069": "Interest income - Commercial and industrial loans",
        "BHCKF821": "Interest income - Loans to individuals",
        "BHCKB488": "Interest income - All other loans",
        "BHCK4065": "Interest income - Lease financing receivables",
        "BHCK4115": "Interest income - Balances due from depository institutions",
        "BHCK4060": "Interest income - Securities (taxable)",
        "BHCK4062": "Interest income - Securities (tax-exempt)",
        "BHCKF556": "Interest income - Trading assets",
        "BHCK4020": "Interest income - Federal funds sold and repos",
        "BHCKB491": "Interest income - Other",
        "BHCK4010": "Total interest income",
    },
    "interest_expense": {
        "BHCK4170": "Interest expense - Deposits in domestic offices",
        "BHCK4172": "Interest expense - Deposits in foreign offices",
        "BHCK4180": "Interest expense - Federal funds purchased and repos",
        "BHCK4185": "Interest expense - Trading liabilities",
        "BHCK4200": "Interest expense - Other borrowed money",
        "BHCK4075": "Interest expense - Subordinated notes and debentures",
        "BHCK4073": "Total interest expense",
    },
    "net_interest_income": {
        "BHCK4074": "Net interest income",
    },
    "provision": {
        "BHCK4230": "Provision for loan and lease losses",
        "BHCKJJ33": "Provision for credit losses",
    },
    "noninterest_income": {
        "BHCK4070": "Income from fiduciary activities",
        "BHCKC886": "Service charges on deposit accounts",
        "BHCKC888": "Trading revenue",
        "BHCKC887": "Investment banking, advisory, brokerage fees",
        "BHCK4042": "Venture capital revenue",
        "BHCKB493": "Net servicing fees",
        "BHCKB494": "Net securitization income",
        "BHCKC013": "Insurance commissions and fees",
        "BHCKC014": "Net gains from sales of loans",
        "BHCKC016": "Net gains from sales of other real estate",
        "BHCKC015": "Net gains from sales of other assets",
        "BHCKB497": "Other noninterest income",
        "BHCK4079": "Total noninterest income",
    },
    "noninterest_expense": {
        "BHCK4135": "Salaries and employee benefits",
        "BHCK4217": "Expenses of premises and fixed assets",
        "BHCKC216": "Goodwill impairment losses",
        "BHCKC232": "Amortization expense - intangible assets",
        "BHCK4092": "Other noninterest expense",
        "BHCK4093": "Total noninterest expense",
    },
    "income": {
        "BHCK4301": "Income before income taxes and extraordinary items",
        "BHCK4302": "Applicable income taxes",
        "BHCK4300": "Income before discontinued operations",
        "BHCKFT28": "Discontinued operations (net of tax)",
        "BHCKG104": "Net income including noncontrolling interests",
        "BHCK4340": "Net income attributable to holding company",
    },
}

# =============================================================================
# SUPPLEMENTARY SCHEDULES
# =============================================================================
INSURANCE_SCHEDULE_ITEMS = {
    "insurance_assets": {
        "BHCKC249": "Separate account assets",
        "BHCKK194": "Insurance assets - General account",
    },
    "insurance_liabilities": {
        "BHCKC250": "Separate account liabilities",
        "BHCKK195": "Insurance liabilities - General account",
    },
    "insurance_income": {
        "BHCKC386": "Underwriting income - life insurance",
        "BHCKC387": "Underwriting income - P&C insurance",
        "BHCKC388": "Insurance commissions and fees",
    },
}

MEMORANDA_ITEMS = {
    "BHCKJJ24": "Total loans secured by 1-4 family residential properties",
    "BHCK1415": "Total commercial and industrial loans",
    "BHCK1590": "Total consumer loans",
    "BHCK2011": "Total real estate loans",
    "BHCK1763": "Average total assets (quarterly)",
    "BHCK3368": "Average total equity (quarterly)",
}


def get_all_mdrm_codes():
    """Return a flat dictionary of all MDRM codes with their descriptions."""
    all_codes = {}

    for category, items in BALANCE_SHEET_ITEMS.items():
        for code, description in items.items():
            all_codes[code] = {
                "description": description,
                "statement": "balance_sheet",
                "category": category,
            }

    for category, items in INCOME_STATEMENT_ITEMS.items():
        for code, description in items.items():
            all_codes[code] = {
                "description": description,
                "statement": "income_statement",
                "category": category,
            }

    for category, items in INSURANCE_SCHEDULE_ITEMS.items():
        for code, description in items.items():
            all_codes[code] = {
                "description": description,
                "statement": "insurance_schedule",
                "category": category,
            }

    for code, description in MEMORANDA_ITEMS.items():
        all_codes[code] = {
            "description": description,
            "statement": "memoranda",
            "category": "memoranda",
        }

    return all_codes


def get_mdrm_codes_list():
    """Return a list of all MDRM codes for filtering raw data."""
    return list(get_all_mdrm_codes().keys())


if __name__ == "__main__":
    all_codes = get_all_mdrm_codes()
    print(f"Total MDRM codes configured: {len(all_codes)}")
    print("\nBy statement type:")
    statements = {}
    for code, info in all_codes.items():
        stmt = info["statement"]
        statements[stmt] = statements.get(stmt, 0) + 1
    for stmt, count in statements.items():
        print(f"  {stmt}: {count} codes")
