# <<< fetch_sec_annual_financials.py >>>

import requests
import json
import time
import logging
import os
import warnings # To suppress InsecureRequestWarning
import re # Import regex for camel_to_snake
import argparse # Import argparse for command-line arguments
from datetime import datetime
from decimal import Decimal, InvalidOperation

# --- Database Setup (using SQLAlchemy) ---
from sqlalchemy import create_engine, Column, Integer, String, Date, DECIMAL, TIMESTAMP, PrimaryKeyConstraint, BigInteger
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.mysql import INTEGER # For UNSIGNED, specific to MySQL/MariaDB dialect
from sqlalchemy.sql import func

# --- Configuration ---
# !!! IMPORTANT: REPLACE with your actual company/app details and contact email !!!
USER_AGENT = "YourCompanyName/SECDataFetcher YourEmail@example.com" # REPLACE with your details

if "YourCompanyName" in USER_AGENT or "YourEmail@example.com" in USER_AGENT:
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("!! WARNING: Default User-Agent detected.                         !!")
    print("!! Please REPLACE the USER_AGENT variable in the script          !!")
    print("!! with your actual Company/Application Name and Contact Email.  !!")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    time.sleep(5)

DB_CONNECTION_STRING = os.environ.get(
    "DB_CONNECTION_STRING",
    'mysql+mysqlconnector://nextcloud:Ks120909090909#@127.0.0.1:3306/nextcloud' # Example, replace
)

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANY_FACTS_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# Financial Tags to Extract (XBRL taxonomy:tag format) - Includes expanded list
DESIRED_TAGS = {
    # Assets
    "Assets": "us-gaap:Assets", "AssetsCurrent": "us-gaap:AssetsCurrent",
    "CashAndEquivalents": "us-gaap:CashAndCashEquivalentsAtCarryingValue",
    "MarketableSecuritiesCurrent": "us-gaap:MarketableSecuritiesCurrent",
    "MarketableSecuritiesNoncurrent": "us-gaap:MarketableSecuritiesNoncurrent",
    "AccountsReceivableNet": "us-gaap:AccountsReceivableNetCurrent",
    "PrepaidExpenseCurrent": "us-gaap:PrepaidExpenseCurrent",
    "InventoryNet": "us-gaap:InventoryNet",
    "OtherAssetsCurrent": "us-gaap:OtherAssetsCurrent",
    "DeferredTaxAssetsNetCurrent": "us-gaap:DeferredTaxAssetsNetCurrent",
    "PropertyPlantEquipmentNet": "us-gaap:PropertyPlantAndEquipmentNet",
    "AccumulatedDepreciation": "us-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment", # Balance Sheet Accumulated value
    "Goodwill": "us-gaap:Goodwill",
    "IntangibleAssetsNetExcludingGoodwill": "us-gaap:IntangibleAssetsNetExcludingGoodwill",
    "Investments": "us-gaap:Investments",
    "OtherAssetsNoncurrent": "us-gaap:OtherAssetsNoncurrent",
    "DeferredTaxAssetsNetNoncurrent": "us-gaap:DeferredTaxAssetsNetNoncurrent",
    # Liabilities
    "Liabilities": "us-gaap:Liabilities", "LiabilitiesCurrent": "us-gaap:LiabilitiesCurrent",
    "AccountsPayable": "us-gaap:AccountsPayableCurrent",
    "IncomeTaxesPayable": "us-gaap:IncomeTaxesPayable",
    "AccruedLiabilitiesCurrent": "us-gaap:AccruedLiabilitiesCurrent",
    "DeferredRevenueCurrent": "us-gaap:DeferredRevenueCurrent",
    "DebtCurrent": "us-gaap:DebtCurrent",
    "OtherLiabilitiesCurrent": "us-gaap:OtherLiabilitiesCurrent",
    "LongTermDebtNoncurrent": "us-gaap:LongTermDebtNoncurrent",
    "DeferredRevenueNoncurrent": "us-gaap:DeferredRevenueNoncurrent",
    "OtherLiabilitiesNoncurrent": "us-gaap:OtherLiabilitiesNoncurrent",
    "DeferredTaxLiabilitiesNoncurrent": "us-gaap:DeferredTaxLiabilitiesNoncurrent",
    "CommitmentsAndContingencies": "us-gaap:CommitmentsAndContingencies",
    # Equity
    "StockholdersEquity": "us-gaap:StockholdersEquity",
    "RetainedEarningsAccumulatedDeficit": "us-gaap:RetainedEarningsAccumulatedDeficit",
    "AccumulatedOtherComprehensiveIncomeLossNetOfTax": "us-gaap:AccumulatedOtherComprehensiveIncomeLossNetOfTax",
    "CommonStockValue": "us-gaap:CommonStockValue",
    "AdditionalPaidInCapital": "us-gaap:AdditionalPaidInCapital",
    "TreasuryStockValue": "us-gaap:TreasuryStockValue",
    "PreferredStockValue": "us-gaap:PreferredStockValue",
    "NoncontrollingInterest": "us-gaap:NoncontrollingInterest",
    # Income Statement
    "Revenues": "us-gaap:Revenues",
    "CostOfRevenue": "us-gaap:CostOfRevenue",
    "GrossProfit": "us-gaap:GrossProfit",
    "DepreciationExpense": "us-gaap:Depreciation", # From Income Stmt (often component of COGS/OpEx)
    "AmortizationOfIntangibleAssets": "us-gaap:AmortizationOfIntangibleAssets", # From Income Stmt
    "OperatingExpenses": "us-gaap:OperatingExpenses",
    "SellingGeneralAndAdministrativeExpense": "us-gaap:SellingGeneralAndAdministrativeExpense",
    "ResearchAndDevelopmentExpense": "us-gaap:ResearchAndDevelopmentExpense",
    "OperatingIncomeLoss": "us-gaap:OperatingIncomeLoss",
    "OtherOperatingIncomeExpenseNet": "us-gaap:OtherOperatingIncomeExpenseNet",
    "InterestExpense": "us-gaap:InterestExpense",
    "InterestIncome": "us-gaap:InterestIncome",
    "InterestIncomeExpenseNet": "us-gaap:InterestIncomeExpenseNet",
    "OtherNonoperatingIncomeExpense": "us-gaap:OtherNonoperatingIncomeExpense",
    "IncomeBeforeTax": "us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxExtraordinaryItemsNoncontrollingInterest",
    "IncomeTaxExpenseBenefit": "us-gaap:IncomeTaxExpenseBenefit",
    "IncomeTaxExpenseBenefitContinuingOperations": "us-gaap:IncomeTaxExpenseBenefitContinuingOperations",
    "IncomeLossFromContinuingOperationsAfterTax": "us-gaap:IncomeLossFromContinuingOperationsAfterTax",
    "IncomeLossFromDiscontinuedOperationsNetOfTax": "us-gaap:IncomeLossFromDiscontinuedOperationsNetOfTax",
    "NetIncomeLoss": "us-gaap:NetIncomeLoss",
    "ComprehensiveIncomeNetOfTax": "us-gaap:ComprehensiveIncomeNetOfTax",
    # EPS & Shares
    "EPSBasic": "us-gaap:EarningsPerShareBasic",
    "EPSDiluted": "us-gaap:EarningsPerShareDiluted",
    "SharesOutstanding": "dei:EntityCommonStockSharesOutstanding",
    "SharesBasicWeightedAvg": "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic",
    "SharesDilutedWeightedAvg": "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding",
    # Cash Flow
    "OperatingCashFlow": "us-gaap:NetCashProvidedByUsedInOperatingActivities",
    "CapitalExpenditures": "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
    "DepreciationAndAmortization": "us-gaap:DepreciationAndAmortization", # Combined D&A from CF Stmt
    "DividendsPaid": "us-gaap:PaymentsOfDividends",
}

REQUEST_DELAY = 0.11

# --- Logging Setup ---
# Set default level to INFO. Change to DEBUG to see detailed logs.
LOG_LEVEL = logging.INFO # Change to logging.DEBUG for detailed revenue logs
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s', # Added function name
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Helper Functions --- (camel_to_snake, get_sec_data, parse_date, safe_decimal, safe_int_or_bigint - NO CHANGES NEEDED)
def camel_to_snake(name):
    """Converts CamelCase names to snake_case."""
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()

def get_sec_data(url, headers, verify_ssl=True):
    """Fetches data from SEC API with error handling, retries, and rate limit awareness."""
    retries = 3; base_delay = 1; last_exception = None
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=30, verify=verify_ssl)
            if response.status_code == 403: logging.error(f"HTTP 403 Forbidden for {url}. CRITICAL: CHECK USER-AGENT!"); return None
            if response.status_code == 404: logging.warning(f"HTTP 404 Not Found for {url}."); return None
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' in content_type: return response.json()
            else:
                logging.warning(f"Unexpected Content-Type '{content_type}' from {url}. Trying decode.");
                try: return response.json()
                except json.JSONDecodeError as e: logging.error(f"Decode failed: {e}"); return None
        except requests.exceptions.HTTPError as e:
            last_exception = e
            if response.status_code == 429: wait_time = (base_delay * (2 ** attempt)) + (5 * (attempt + 1)); logging.warning(f"HTTP 429 Rate limit. Retrying in {wait_time:.2f}s..."); time.sleep(wait_time)
            elif 500 <= response.status_code < 600: wait_time = base_delay * (2 ** attempt); logging.warning(f"HTTP {response.status_code} Server Error. Retrying in {wait_time:.2f}s..."); time.sleep(wait_time)
            else: logging.error(f"Unhandled HTTP Error {response.status_code} for {url}: {e}"); return None
        except requests.exceptions.SSLError as e: logging.error(f"SSL Error: {e}"); last_exception = e; return None
        except requests.exceptions.Timeout as e: last_exception = e; wait_time = base_delay * (2 ** attempt); logging.warning(f"Timeout. Retrying in {wait_time:.2f}s..."); time.sleep(wait_time)
        except requests.exceptions.ConnectionError as e: last_exception = e; wait_time = base_delay * (2 ** attempt); logging.warning(f"Connection error: {e}. Retrying in {wait_time:.2f}s..."); time.sleep(wait_time)
        except json.JSONDecodeError as e: last_exception = e; logging.error(f"JSON Decode Error: {e}"); return None
        except Exception as e: last_exception = e; logging.error(f"Unexpected error fetching {url}: {type(e).__name__} - {e}", exc_info=True); wait_time = base_delay * (2 ** attempt); time.sleep(wait_time)
    logging.error(f"Failed fetch {url} after {retries + 1} attempts. Last error: {last_exception}"); return None

def parse_date(date_str):
    if not date_str or not isinstance(date_str, str): return None
    try: return datetime.strptime(date_str, '%Y-%m-%d').date()
    except (ValueError, TypeError): logging.debug(f"Could not parse date string: '{date_str}'"); return None

def safe_decimal(value):
    if value is None: return None
    if isinstance(value, Decimal): return value
    try:
        if isinstance(value, str): value = value.strip();
        if not value: return None
        try: return Decimal(value)
        except InvalidOperation: float_val = float(value); return Decimal(str(float_val)) # Handle sci notation via float
    except (ValueError, TypeError, InvalidOperation) as e: logging.warning(f"Could not convert '{value}' (type: {type(value)}) to Decimal: {e}"); return None

def safe_int_or_bigint(value):
    if value is None: return None
    if isinstance(value, int): return value
    try:
        dec_val = Decimal(str(value).strip())
        if dec_val % 1 != 0: logging.debug(f"Truncating fractional part for integer conversion: {value}")
        return int(dec_val)
    except (InvalidOperation, ValueError, TypeError) as e: logging.warning(f"Could not convert '{value}' to integer: {e}"); return None

# --- SQLAlchemy Model Definition --- (Dynamically updated by DESIRED_TAGS)
Base = declarative_base()
class AnnualData(Base):
    __tablename__ = 'sec_annual_data'
    __table_args__ = {'comment': 'Stores selected annual financial data derived from SEC EDGAR API'}

    # Core identifying columns
    cik = Column(INTEGER(unsigned=True), primary_key=True, comment='Company Identifier (CIK)')
    year = Column(Integer, primary_key=True, comment='Fiscal Year')

    # Metadata columns
    ticker = Column(String(20), nullable=True, index=True, comment='Trading Symbol at time of fetch')
    company_name = Column(String(255), nullable=True, comment='Company Name from SEC data')
    form = Column(String(10), nullable=True, comment='Source Form (e.g., 10-K)')
    filed_date = Column(Date, nullable=True, comment='Filing Date of the source form')
    period_end_date = Column(Date, nullable=True, comment='Period End Date for the fiscal year')

    # Dynamically create financial data columns based on DESIRED_TAGS
    _column_definitions = {}
    for key, full_tag in DESIRED_TAGS.items():
        col_name = camel_to_snake(key)
        col_type = None
        comment_str = f'{key} ({full_tag})'
        if 'shares' in col_name: col_type = BigInteger().with_variant(INTEGER(unsigned=True), "mysql")
        elif 'eps' in col_name: col_type = DECIMAL(18, 6)
        else: col_type = DECIMAL(28, 4)
        _column_definitions[col_name] = Column(col_type, nullable=True, comment=comment_str)
    locals().update(_column_definitions)

    # Calculated Free Cash Flow Column
    calculated_free_cash_flow = Column(DECIMAL(28, 4), nullable=True, comment='Calculated Free Cash Flow (OperatingCashFlow - CapitalExpenditures)')

    # Audit timestamp column
    updated_at = Column(TIMESTAMP, nullable=False, server_default=func.now(), onupdate=func.now(), comment='Record last updated timestamp')

    __table_args__ = (
        PrimaryKeyConstraint('cik', 'year', name='pk_sec_annual_data'),
        {'comment': 'Stores selected annual financial data derived from SEC EDGAR API'}
    )
    def __repr__(self): return f"<AnnualData(cik={self.cik}, year={self.year}, ticker='{self.ticker}')>"


# --- Processing Function --- (Includes targeted logging for Revenues)
def process_company_facts(cik, company_data):
    """Processes the raw JSON facts data for a single company."""
    if not company_data or 'facts' not in company_data:
        logging.debug(f"CIK {cik}: No 'facts' key found.")
        return {}, None
    company_name = company_data.get('entityName', 'N/A')
    facts = company_data['facts']
    annual_results = {} # { year: {'col_name_snake': value, ...}, ... }

    for key, full_tag in DESIRED_TAGS.items():
        db_col_snake = camel_to_snake(key)
        try:
            taxonomy, tag_name = full_tag.split(':')

            # --- >>> TARGETED LOGGING FOR REVENUES <<< ---
            is_revenue_tag = (key == "Revenues")
            if is_revenue_tag:
                logging.debug(f"--- Debugging Revenues for CIK {cik} ---")
                logging.debug(f"Looking for tag: {full_tag} (col: {db_col_snake})")
            # --- >>> END TARGETED LOGGING <<< ---

            if taxonomy not in facts or tag_name not in facts[taxonomy]:
                if is_revenue_tag: logging.debug(f"Tag not found in facts['{taxonomy}'].")
                continue # Skip tag if not present

            tag_data = facts[taxonomy][tag_name]
            units = tag_data.get('units')

            if is_revenue_tag: logging.debug(f"Tag found. Units available: {list(units.keys()) if units else 'None'}")

            if not units: continue # Skip if no units

            # Determine unit key
            unit_key = None; is_share_metric = 'shares' in db_col_snake; is_eps_metric = 'eps' in db_col_snake
            if is_share_metric and 'shares' in units: unit_key = 'shares'
            elif is_eps_metric and 'USD/shares' in units: unit_key = 'USD/shares'
            elif not is_share_metric and not is_eps_metric and 'USD' in units: unit_key = 'USD'
            elif units: first_unit = list(units.keys())[0]; unit_key = first_unit # Fallback

            if is_revenue_tag: logging.debug(f"Attempting to use unit key: '{unit_key}'")

            if not unit_key or unit_key not in units:
                 if is_revenue_tag: logging.debug(f"Unit key '{unit_key}' not found in available units or is None.")
                 continue # Skip if unit key invalid

            unit_data = units[unit_key]
            yearly_data_for_tag = {} # { year: {best_entry_data}, ... }

            for entry in unit_data:
                # --- >>> TARGETED LOGGING FOR REVENUES <<< ---
                if is_revenue_tag:
                    fy_entry_raw = entry.get('fy')
                    fp_entry_raw = entry.get('fp')
                    form_entry_raw = entry.get('form')
                    filed_entry_raw = entry.get('filed')
                    val_entry_raw = entry.get('val')
                    logging.debug(f"  Considering entry: FY={fy_entry_raw}, FP={fp_entry_raw}, Form={form_entry_raw}, Filed={filed_entry_raw}, Val='{val_entry_raw}'")
                # --- >>> END TARGETED LOGGING <<< ---

                if (entry.get('form') and entry.get('fp') == 'FY' and entry.get('fy') is not None):
                    fy = entry.get('fy')
                    filed_date = parse_date(entry.get('filed'))
                    if not filed_date:
                        if is_revenue_tag and entry.get('fp') == 'FY': logging.debug("    Skipping entry: Invalid/missing filed date.")
                        continue

                    # --- >>> TARGETED LOGGING FOR REVENUES <<< ---
                    if is_revenue_tag and entry.get('fp') == 'FY':
                        logging.debug(f"    Entry meets FY criteria (FY={fy}). Parsed filed date: {filed_date}")
                    # --- >>> END TARGETED LOGGING <<< ---

                    # Select best entry logic (prefer 10-K, then latest filing)
                    is_better_candidate = False
                    current_best_str = "" # For logging
                    if fy not in yearly_data_for_tag:
                        is_better_candidate = True
                    else:
                        current_best = yearly_data_for_tag[fy]
                        current_form = current_best.get('form')
                        entry_form = entry.get('form')
                        current_best_str = f"(Current best: Form={current_form}, Filed={current_best.get('filed_date')})"
                        if entry_form == '10-K' and current_form != '10-K': is_better_candidate = True
                        elif (entry_form == current_form) or (entry_form != '10-K' and current_form != '10-K'):
                           if filed_date > current_best['filed_date']: is_better_candidate = True

                    # --- >>> TARGETED LOGGING FOR REVENUES <<< ---
                    if is_revenue_tag and entry.get('fp') == 'FY':
                         logging.debug(f"    Check if better candidate {current_best_str}: {is_better_candidate}")
                    # --- >>> END TARGETED LOGGING <<< ---

                    if is_better_candidate:
                        value_to_store = safe_int_or_bigint(entry.get('val')) if is_share_metric else safe_decimal(entry.get('val'))

                        # --- >>> TARGETED LOGGING FOR REVENUES <<< ---
                        if is_revenue_tag and entry.get('fp') == 'FY':
                            logging.debug(f"    Value from entry: '{entry.get('val')}', Parsed value (safe_decimal/int): {value_to_store}")
                        # --- >>> END TARGETED LOGGING <<< ---

                        if value_to_store is not None:
                            yearly_data_for_tag[fy] = {'val': value_to_store, 'filed_date': filed_date, 'form': entry.get('form'), 'end_date': parse_date(entry.get('end'))}
                            if is_revenue_tag and entry.get('fp') == 'FY': logging.debug(f"    Stored as best candidate for FY {fy}.")
                        else:
                            if is_revenue_tag and entry.get('fp') == 'FY': logging.debug(f"    Value parsing failed; not stored.")
                # --- >>> TARGETED LOGGING FOR REVENUES <<< ---
                # elif is_revenue_tag:
                #     logging.debug(f"    Skipping entry: Doesn't meet core FY criteria (Form, FP, FY).")
                # --- >>> END TARGETED LOGGING <<< ---


            # Integrate best data for this tag into the main results
            for year, data in yearly_data_for_tag.items():
                if year not in annual_results: annual_results[year] = {'cik': cik, 'year': year}
                annual_results[year][db_col_snake] = data['val']
                # Update metadata (form, dates) logic (no changes needed here)
                update_metadata = False
                if 'form' not in annual_results[year]: update_metadata = True
                else:
                    current_meta_form=annual_results[year].get('form'); current_meta_filed=annual_results[year].get('filed_date')
                    tag_entry_form=data['form']; tag_entry_filed=data['filed_date']
                    if tag_entry_form == '10-K' and current_meta_form != '10-K': update_metadata = True
                    elif (tag_entry_form == current_meta_form or (tag_entry_form != '10-K' and current_meta_form != '10-K')):
                        if tag_entry_filed and current_meta_filed and tag_entry_filed > current_meta_filed: update_metadata = True
                        elif tag_entry_filed and not current_meta_filed: update_metadata = True
                if update_metadata:
                     annual_results[year]['form']=data['form']; annual_results[year]['filed_date']=data['filed_date']; annual_results[year]['period_end_date']=data['end_date']

            # --- >>> TARGETED LOGGING FOR REVENUES <<< ---
            if is_revenue_tag: logging.debug(f"--- End Debugging Revenues for CIK {cik} ---")
            # --- >>> END TARGETED LOGGING <<< ---

        except Exception as e:
            logging.error(f"Error processing tag {full_tag} (col: {db_col_snake}) for CIK {cik}: {type(e).__name__} - {e}", exc_info=False)
            if is_revenue_tag: logging.debug(f"--- End Debugging Revenues for CIK {cik} due to error ---") # Log end even on error

    return annual_results, company_name

# --- Main Execution Block ---
if __name__ == "__main__":
    # --- >>> ARGUMENT PARSING <<< ---
    parser = argparse.ArgumentParser(description="Fetch SEC annual financial data and store it in the database.")
    parser.add_argument(
        "--cik",
        type=str,
        help="Optional: Process only the specified CIK (Central Index Key). Provide without leading zeros.",
        required=False
    )
    args = parser.parse_args()
    target_cik = args.cik.lstrip('0') if args.cik else None # Remove leading zeros if provided for matching keys
    # --- >>> END ARGUMENT PARSING <<< ---

    logging.info("="*60 + "\nStarting SEC Annual Financial Data Fetcher\n" + "="*60)
    if target_cik:
        logging.info(f"--- Running in single CIK mode for CIK: {target_cik} ---")
    else:
        logging.info("--- Running in full mode (all companies) ---")

    logging.info(f"Using User-Agent: {USER_AGENT}")
    logging.info(f"Database Target: {DB_CONNECTION_STRING.split('@')[-1] if '@' in DB_CONNECTION_STRING else DB_CONNECTION_STRING}")
    logging.info(f"Logging Level Set To: {logging.getLevelName(LOG_LEVEL)}")
    if LOG_LEVEL > logging.DEBUG:
        logging.info("Set LOG_LEVEL to logging.DEBUG in the script to see detailed trace logs (like Revenue processing).")


    # --- Database Setup & Connection ---
    db_session = None
    try:
        engine = create_engine(DB_CONNECTION_STRING, pool_recycle=3600, pool_pre_ping=True, echo=False)
        logging.info("Ensuring database table 'sec_annual_data' schema...")
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        db_session = Session()
        logging.info("Database connection successful and schema ensured.")
    except Exception as e: logging.error(f"FATAL: Database connection/setup failed: {e}", exc_info=True); exit(1)

    # --- Fetch Company Ticker List ---
    logging.info(f"Fetching company CIK/ticker list from {COMPANY_TICKERS_URL}...")
    ticker_fetch_headers = { 'User-Agent': USER_AGENT }
    company_tickers_data = get_sec_data(COMPANY_TICKERS_URL, ticker_fetch_headers)
    time.sleep(max(REQUEST_DELAY, 0.5))
    if not company_tickers_data:
        logging.error("FATAL: Failed fetch company tickers list. Exiting.");
        if db_session and db_session.is_active: db_session.close()
        exit(1)

    # --- Process Ticker List ---
    all_companies = {}
    try:
        if isinstance(company_tickers_data, dict):
            all_companies = { str(info['cik_str']): {'title': info['title'], 'ticker': info['ticker']} for _, info in company_tickers_data.items() if isinstance(info, dict) and 'cik_str' in info and 'title' in info and 'ticker' in info }
        elif isinstance(company_tickers_data, list):
             all_companies = { str(info['cik_str']): {'title': info['title'], 'ticker': info['ticker']} for info in company_tickers_data if isinstance(info, dict) and 'cik_str' in info and 'title' in info and 'ticker' in info }
        else: raise TypeError(f"Unexpected tickers format: {type(company_tickers_data)}")
        logging.info(f"Processed {len(all_companies)} companies from ticker list.")
    except Exception as e: logging.error(f"FATAL: Error processing tickers data: {e}", exc_info=True); db_session.close(); exit(1)
    if not all_companies: logging.error("FATAL: Ticker list processed but empty. Exiting."); db_session.close(); exit(1)


    # --- >>> SELECT COMPANIES TO PROCESS BASED ON ARGUMENT <<< ---
    companies_to_process = []
    if target_cik:
        # Ensure target_cik matches the string key format in all_companies
        if target_cik in all_companies:
            companies_to_process = [(target_cik, all_companies[target_cik])]
            logging.info(f"Found specified CIK {target_cik} in the ticker list.")
        else:
            logging.error(f"Specified CIK {target_cik} not found in the fetched ticker list. Exiting.")
            if db_session and db_session.is_active: db_session.close()
            exit(1)
    else:
        # Process all companies if no specific CIK was given
        companies_to_process = list(all_companies.items())
    # --- >>> END SELECT COMPANIES <<< ---

    # --- Process Each Selected Company ---
    processed_count = 0; error_count = 0; no_facts_count = 0; db_commit_errors = 0; start_time = time.time()
    total_companies_to_process = len(companies_to_process) # Use the length of the potentially filtered list

    if total_companies_to_process > 0:
        logging.info(f"Starting data fetching for {total_companies_to_process} selected company/companies...")
    else:
        # This case should ideally not be reached if CIK validation works, but handle it.
        logging.warning("No companies selected for processing.")
        if db_session and db_session.is_active: db_session.close()
        exit(0)

    api_headers = { 'User-Agent': USER_AGENT, 'Accept-Encoding': 'gzip, deflate', 'Host': 'data.sec.gov' }

    for cik_str, company_info in companies_to_process: # Loop through selected list
        try: cik = int(cik_str)
        except ValueError: logging.warning(f"Invalid CIK '{cik_str}'. Skipping."); processed_count += 1; continue
        ticker = company_info.get('ticker', 'N/A'); title_from_list = company_info.get('title', '').strip() or 'N/A'; processed_count += 1
        log_prefix = f"({processed_count}/{total_companies_to_process}) CIK {cik_str} ({ticker}):" # Progress uses filtered total

        # ETA Calculation (less relevant for single CIK)
        if total_companies_to_process > 1 and processed_count > 10:
            elapsed_time = time.time() - start_time; avg_time = elapsed_time / (processed_count -1)
            if avg_time > 0: eta_str = time.strftime("%Hh %Mm %Ss", time.gmtime(avg_time * (total_companies_to_process - processed_count))); logging.info(f"{log_prefix} Processing '{title_from_list}'... (ETA: {eta_str})")
            else: logging.info(f"{log_prefix} Processing '{title_from_list}'...")
        else: logging.info(f"{log_prefix} Processing '{title_from_list}'...")

        # Fetch Company Facts
        facts_url = COMPANY_FACTS_URL_TEMPLATE.format(cik=cik_str.zfill(10))
        company_facts_json = get_sec_data(facts_url, api_headers)

        # Process Facts and Merge Data
        if company_facts_json:
            annual_data_by_year, entity_name_from_facts = process_company_facts(cik, company_facts_json)
            effective_company_name = entity_name_from_facts.strip() if entity_name_from_facts and entity_name_from_facts.strip() and entity_name_from_facts != 'N/A' else title_from_list

            if annual_data_by_year:
                years_found = len(annual_data_by_year); years_merged_count = 0
                logging.info(f"{log_prefix} Found {years_found} year(s) data for '{effective_company_name}'. Merging...")

                for year, data_for_year in sorted(annual_data_by_year.items()):
                    try:
                        # Calculate Free Cash Flow
                        ocf = data_for_year.get('operating_cash_flow')
                        capex = data_for_year.get('capital_expenditures')
                        calculated_fcf = ocf - capex if ocf is not None and capex is not None else None
                        if calculated_fcf is None and (ocf is not None or capex is not None):
                             logging.debug(f"{log_prefix} Year {year}: Cannot calculate FCF (OCF: {ocf}, CapEx: {capex})")

                        # Prepare data for ORM
                        record_data_for_orm = {
                            'ticker': ticker,
                            'company_name': effective_company_name,
                            'calculated_free_cash_flow': calculated_fcf,
                            **data_for_year # Unpack all snake_case keyed data
                        }
                        record_data_for_orm.pop('cik', None); record_data_for_orm.pop('year', None); record_data_for_orm.pop('val', None)

                        # --- >>> TARGETED LOGGING BEFORE MERGE <<< ---
                        if LOG_LEVEL == logging.DEBUG:
                            logging.debug(f"Data being prepared for ORM merge (CIK {cik} Year {year}): {record_data_for_orm}")
                        # --- >>> END TARGETED LOGGING <<< ---

                        # Create object and merge
                        record_obj = AnnualData(cik=cik, year=year, **record_data_for_orm)
                        db_session.merge(record_obj)
                        years_merged_count += 1

                        # Periodic commit
                        if years_merged_count > 0 and years_merged_count % 50 == 0:
                            logging.debug(f"{log_prefix} Committing batch ({years_merged_count}/{years_found} years)...")
                            db_session.commit()

                    except Exception as e:
                        logging.error(f"{log_prefix} DB merge error Year {year}: {type(e).__name__} - {e}", exc_info=False)
                        error_count += 1; db_session.rollback()

                try: # Final commit for the CIK
                    db_session.commit()
                    if years_merged_count > 0: logging.info(f"{log_prefix} Merged data for {years_merged_count} year(s).")
                except Exception as e:
                    logging.error(f"{log_prefix} Final commit error: {type(e).__name__} - {e}", exc_info=False)
                    db_commit_errors += 1; db_session.rollback()
            else:
                logging.info(f"{log_prefix} No relevant annual data extracted for '{effective_company_name}'.")
                no_facts_count += 1
        else:
            logging.info(f"{log_prefix} No facts data retrieved for CIK {cik_str} ('{title_from_list}').")
            no_facts_count += 1

        time.sleep(REQUEST_DELAY) # Rate limit pause

    # --- Final Summary ---
    end_time = time.time(); total_duration_str = time.strftime("%Hh %Mm %Ss", time.gmtime(end_time - start_time))
    logging.info("="*60 + "\nProcessing Summary\n" + "="*60)
    logging.info(f"Finished processing {processed_count} selected CIK(s).")
    logging.info(f"Total execution time: {total_duration_str}.")
    logging.info(f"  - CIKs with no data retrieved/extracted: {no_facts_count}")
    logging.info(f"  - Database record merge errors: {error_count}")
    logging.info(f"  - Database final commit errors: {db_commit_errors}")
    if (error_count + db_commit_errors) == 0: logging.info("  No database errors encountered.")
    logging.info("="*60)

    # --- Cleanup ---
    if db_session and db_session.is_active: db_session.close(); logging.info("Database session closed.")
    logging.info("SEC Annual Financial Data Fetcher finished.")