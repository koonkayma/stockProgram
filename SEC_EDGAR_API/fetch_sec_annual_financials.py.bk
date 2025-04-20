# <<< fetch_sec_annual_financials.py >>>

import requests
import json
import time
import logging
import os
import warnings # To suppress InsecureRequestWarning
from datetime import datetime
from decimal import Decimal, InvalidOperation

# --- Database Setup (using SQLAlchemy) ---
from sqlalchemy import create_engine, Column, Integer, String, Date, DECIMAL, TIMESTAMP, PrimaryKeyConstraint, BigInteger
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.mysql import INTEGER # For UNSIGNED, specific to MySQL/MariaDB dialect
from sqlalchemy.sql import func

# --- Configuration ---
# Temporarily test the older User-Agent format - REPLACE with your actual details
USER_AGENT = "YourCompanyName/AppContact YourEmail@example.com" # REPLACE with your details
# Check if the User-Agent looks like the placeholder
if USER_AGENT == "YourCompanyName/AppContact YourEmail@example.com" or USER_AGENT == "YourAppName YourName your.email@example.com":
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("!! WARNING: Default User-Agent set.                              !!")
    print("!! Please REPLACE 'YourCompanyName/AppContact YourEmail@example.com' !!")
    print("!! in the script with your actual identification.                !!")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    time.sleep(5) # Pause to ensure the user sees the warning


DB_CONNECTION_STRING = os.environ.get(
    "DB_CONNECTION_STRING",
    # Replace with your actual MariaDB connection string
    'mysql+mysqlconnector://nextcloud:Ks120909090909#@127.0.0.1:3306/nextcloud'
)

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANY_FACTS_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# Financial Tags to Extract (use 'taxonomy:Tag' format) - FINAL EXPANDED LIST
DESIRED_TAGS = {
    "Assets": "us-gaap:Assets", "NetIncomeLoss": "us-gaap:NetIncomeLoss", "Revenues": "us-gaap:Revenues",
    "Liabilities": "us-gaap:Liabilities", "StockholdersEquity": "us-gaap:StockholdersEquity",
    "CashAndEquivalents": "us-gaap:CashAndCashEquivalentsAtCarryingValue",
    "AccountsReceivableNet": "us-gaap:AccountsReceivableNetCurrent", "InventoryNet": "us-gaap:InventoryNet",
    "PropertyPlantEquipmentNet": "us-gaap:PropertyPlantAndEquipmentNet",
    "AccumulatedDepreciation": "us-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
    "AccountsPayable": "us-gaap:AccountsPayableCurrent", "AccruedLiabilitiesCurrent": "us-gaap:AccruedLiabilitiesCurrent",
    "DebtCurrent": "us-gaap:DebtCurrent", "LongTermDebtNoncurrent": "us-gaap:LongTermDebtNoncurrent",
    "CostOfRevenue": "us-gaap:CostOfRevenue", "GrossProfit": "us-gaap:GrossProfit",
    "OperatingExpenses": "us-gaap:OperatingExpenses", "OperatingIncomeLoss": "us-gaap:OperatingIncomeLoss",
    "InterestExpense": "us-gaap:InterestExpense",
    "IncomeBeforeTax": "us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxExtraordinaryItemsNoncontrollingInterest",
    "IncomeTaxExpenseBenefit": "us-gaap:IncomeTaxExpenseBenefit",
    "EPSBasic": "us-gaap:EarningsPerShareBasic", "EPSDiluted": "us-gaap:EarningsPerShareDiluted",
    "SharesOutstanding": "dei:EntityCommonStockSharesOutstanding",
    "SharesBasicWeightedAvg": "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic",
    "SharesDilutedWeightedAvg": "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding",
    "OperatingCashFlow": "us-gaap:NetCashProvidedByUsedInOperatingActivities",
    "CapitalExpenditures": "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
    "DepreciationAndAmortization": "us-gaap:DepreciationAndAmortization",
    "DividendsPaid": "us-gaap:PaymentsOfDividends",
}
DB_COLUMN_MAPPING = {v: k.lower() for k, v in DESIRED_TAGS.items()}

REQUEST_DELAY = 0.11

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- SQLAlchemy Model Definition ---
Base = declarative_base()
class AnnualData(Base):
    __tablename__ = 'sec_annual_data'
    cik = Column(INTEGER(unsigned=True), primary_key=True)
    year = Column(Integer, primary_key=True)
    ticker = Column(String(20), nullable=True)
    company_name = Column(String(255), nullable=True)
    form = Column(String(10), nullable=True)
    filed_date = Column(Date, nullable=True)
    period_end_date = Column(Date, nullable=True)
    _column_definitions = {}
    for key, full_tag in DESIRED_TAGS.items():
        col_name = key.lower(); col_type = None; comment_str = f'{key} ({full_tag})'
        if 'shares' in col_name: col_type = BigInteger().with_variant(INTEGER(unsigned=True), "mysql")
        elif 'eps' in col_name: col_type = DECIMAL(18, 6)
        else: col_type = DECIMAL(28, 4)
        _column_definitions[col_name] = Column(col_type, nullable=True, comment=comment_str)
    locals().update(_column_definitions)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())
    __table_args__ = (PrimaryKeyConstraint('cik', 'year'), {})
    def __repr__(self): return f"<AnnualData(cik={self.cik}, year={self.year}, ticker='{self.ticker}')>"


# --- Helper Functions ---

# Modified get_sec_data to accept verify flag - BUT WE WILL CALL requests.get directly for ticker
def get_sec_data(url, headers, verify_ssl=True):
    """Fetches data from SEC API, handles basic errors and retries."""
    retries = 3; last_exception = None
    for attempt in range(retries):
        try:
            # Use the verify_ssl flag passed to the function
            response = requests.get(url, headers=headers, timeout=30, verify=verify_ssl)
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' not in content_type:
                logging.warning(f"Unexpected Content-Type '{content_type}' for {url} in get_sec_data.")
                return None
            return response.json()
        except requests.exceptions.SSLError as e:
            logging.error(f"SSL Error for {url}: {e}. If using verify=False, ensure it's intended.")
            last_exception = e
            return None # Don't retry SSL errors usually
        except requests.exceptions.ConnectionError as e: logging.warning(f"Connection error for {url}: {e}. Retrying {attempt + 1}/{retries}..."); last_exception = e; time.sleep(2 ** attempt)
        except requests.exceptions.Timeout as e: logging.warning(f"Timeout error for {url}: {e}. Retrying {attempt + 1}/{retries}..."); last_exception = e; time.sleep(2 ** attempt)
        except requests.exceptions.HTTPError as e:
            last_exception = e
            if response.status_code == 404: logging.warning(f"HTTP 404 for {url}."); return None
            elif response.status_code == 403: logging.error(f"HTTP 403 Forbidden for {url}. CHECK USER-AGENT!"); return None
            elif response.status_code == 429: logging.warning(f"HTTP 429 Rate limit for {url}. Retrying..."); time.sleep(5 * (attempt + 1))
            else: logging.error(f"HTTP Error {response.status_code} for {url}: {e}"); return None
        except json.JSONDecodeError as e: last_exception = e; logging.error(f"Failed to decode JSON from {url}: {e}"); return None
        except Exception as e: last_exception = e; logging.error(f"Unexpected error fetching {url}: {e}"); return None
    logging.error(f"Failed fetch {url} after {retries} retries. Last error: {last_exception}"); return None

def parse_date(date_str):
    if not date_str: return None
    try: return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError: logging.warning(f"Could not parse date: {date_str}"); return None

def safe_decimal(value):
    if isinstance(value, (int, float)):
        try: return Decimal(value)
        except InvalidOperation: logging.warning(f"Could not convert numeric '{value}' to Decimal."); return None
    if isinstance(value, str):
        value = value.strip();
        if not value: return None
        try: return Decimal(value)
        except InvalidOperation: logging.warning(f"Could not convert string '{value}' to Decimal."); return None
    if value is None: return None
    logging.warning(f"Unsupported type '{type(value)}' for safe_decimal: {value}"); return None

def safe_int_or_bigint(value):
    if value is None: return None
    try:
        dec_val = Decimal(value)
        if dec_val % 1 != 0: logging.debug(f"Value '{value}' has fractional part, truncating for integer conversion.")
        return int(dec_val)
    except (InvalidOperation, ValueError, TypeError): logging.warning(f"Could not convert '{value}' (type: {type(value)}) to integer."); return None


# --- Processing Function (process_company_facts remains the same) ---
def process_company_facts(cik, company_data):
    if not company_data or 'facts' not in company_data: return {}, None
    company_name = company_data.get('entityName', 'N/A'); facts = company_data['facts']
    annual_results = {}
    for db_col_key, full_tag in DESIRED_TAGS.items():
        db_col_lower = db_col_key.lower()
        try:
            taxonomy, tag_name = full_tag.split(':')
            if taxonomy not in facts or tag_name not in facts[taxonomy]: continue
            tag_data = facts[taxonomy][tag_name]; units = tag_data.get('units')
            if not units: continue
            unit_key = None; is_share_metric = 'shares' in db_col_lower; is_eps_metric = 'eps' in db_col_lower
            if is_share_metric and 'shares' in units: unit_key = 'shares'
            elif is_eps_metric and 'USD/shares' in units: unit_key = 'USD/shares'
            elif not is_share_metric and not is_eps_metric and 'USD' in units: unit_key = 'USD'
            elif units:
                 unit_key = list(units.keys())[0]; expected_unit = 'shares' if is_share_metric else ('USD/shares' if is_eps_metric else 'USD')
                 if unit_key != expected_unit: logging.debug(f"CIK {cik}, Tag {full_tag}: Expected '{expected_unit}', using '{unit_key}'.")
            if not unit_key: continue
            unit_data = units[unit_key]; yearly_data = {}
            for entry in unit_data:
                if entry.get('fp') != 'FY' or entry.get('fy') is None: continue
                fy = entry.get('fy'); filed_date = parse_date(entry.get('filed'))
                if not filed_date: continue
                is_better_candidate = False
                if fy not in yearly_data: is_better_candidate = True
                else:
                    current_best = yearly_data[fy]; current_form = current_best.get('form'); entry_form = entry.get('form')
                    if entry_form == '10-K' and current_form != '10-K': is_better_candidate = True
                    elif (entry_form == current_form) or (entry_form != '10-K' and current_form != '10-K'):
                       if filed_date > current_best['filed_date']: is_better_candidate = True
                if is_better_candidate:
                    value_to_store = safe_int_or_bigint(entry.get('val')) if is_share_metric else safe_decimal(entry.get('val'))
                    if value_to_store is not None:
                        yearly_data[fy] = {'val': value_to_store, 'filed_date': filed_date, 'form': entry.get('form'), 'end_date': parse_date(entry.get('end'))}
                    else: logging.debug(f"CIK {cik}, Tag {full_tag}, FY {fy}: Value '{entry.get('val')}' invalid.")
            for year, data in yearly_data.items():
                if year not in annual_results: annual_results[year] = {'cik': cik, 'year': year}
                annual_results[year][db_col_lower] = data['val']
                priority_tag_key = "Assets"
                if db_col_key == priority_tag_key or 'form' not in annual_results[year]:
                     annual_results[year]['form'] = data['form']; annual_results[year]['filed_date'] = data['filed_date']; annual_results[year]['period_end_date'] = data['end_date']
        except Exception as e: logging.error(f"Error processing tag {full_tag} (col: {db_col_lower}) for CIK {cik}: {e}", exc_info=True)
    return annual_results, company_name


# --- Main Execution ---
if __name__ == "__main__":
    logging.info("Starting SEC Annual Data Fetcher.")
    logging.info(f"Using User-Agent: {USER_AGENT}")
    logging.info(f"Database Target: {DB_CONNECTION_STRING.split('@')[-1] if '@' in DB_CONNECTION_STRING else DB_CONNECTION_STRING}")

    # --- Database Connection ---
    try:
        engine = create_engine(DB_CONNECTION_STRING, pool_recycle=3600); Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine); db_session = Session()
        logging.info("Database connection successful.")
    except Exception as e: logging.error(f"Database connection failed: {e}", exc_info=True); exit(1)

    # --- Fetch Company List ---
    logging.info("Fetching company CIK list...")

    # Define specific headers JUST for the ticker request
    ticker_fetch_headers = { 'User-Agent': USER_AGENT }

    company_tickers_data = None
    logging.info(f"Attempting request to {COMPANY_TICKERS_URL} with headers: {ticker_fetch_headers}")
    # Suppress only the InsecureRequestWarning for this specific call
    warnings.filterwarnings('ignore', message='Unverified HTTPS request')
    try:
        # *** Add verify=False TEMPORARILY for diagnostics ***
        #response = requests.get(COMPANY_TICKERS_URL, headers=ticker_fetch_headers, timeout=30, verify=False)
        tmp_headers= {'User-Agent': 'YourCompanyName/AppContact YourEmail@example.com'}
        response = requests.get(COMPANY_TICKERS_URL, headers=tmp_headers, timeout=30)
        
        logging.info(f"Ticker URL request completed. Status Code: {response.status_code}")
        logging.debug(f"Ticker URL Response Headers: {response.headers}")
        response.raise_for_status() # Check for 4xx/5xx errors AFTER logging status
        content_type = response.headers.get('Content-Type', '')
        if 'application/json' in content_type:
            company_tickers_data = response.json()
            logging.info("Successfully fetched and decoded company tickers JSON.")
        else:
            logging.warning(f"Ticker URL returned unexpected Content-Type: {content_type}. Cannot decode JSON.")
            # logging.debug(f"Ticker URL Response Text (non-JSON): {response.text[:500]}")
    except requests.exceptions.HTTPError as e:
         # Log full response text for 4xx/5xx errors to see potential HTML error messages
         logging.error(f"HTTP Error fetching tickers: {e.response.status_code} for {COMPANY_TICKERS_URL}. Response text: {e.response.text[:1000]}", exc_info=False)
         db_session.close(); exit(1)
    except requests.exceptions.RequestException as e: logging.error(f"RequestException fetching tickers: {e}"); db_session.close(); exit(1)
    except json.JSONDecodeError as e: logging.error(f"JSONDecodeError fetching tickers: {e}. Content-Type was {content_type}. Text(partial): {response.text[:200]}"); db_session.close(); exit(1)
    except Exception as e: logging.error(f"Unexpected error fetching tickers: {e}", exc_info=True); db_session.close(); exit(1)
    finally:
        # Restore default warning behavior
        warnings.resetwarnings()

    # *** IMPORTANT: Remove verify=False warning after testing ***
    if company_tickers_data: # Only log warning if fetch was attempted
        logging.warning("SSL verification was TEMPORARILY disabled for ticker fetch. REMOVE 'verify=False' for production.")

    time.sleep(REQUEST_DELAY) # Pause after fetching tickers list

    if not company_tickers_data:
        logging.error("Failed fetch tickers (data is None after request). Exiting.")
        db_session.close(); exit(1)

    # --- Process Ticker List ---
    if isinstance(company_tickers_data, dict): all_companies = { str(info['cik_str']): {'title': info['title'], 'ticker': info['ticker']} for key, info in company_tickers_data.items() }
    elif isinstance(company_tickers_data, list): all_companies = { str(info['cik_str']): {'title': info['title'], 'ticker': info['ticker']} for info in company_tickers_data }
    else: logging.error(f"Unexpected tickers format: {type(company_tickers_data)}"); db_session.close(); exit(1)
    logging.info(f"Fetched info for {len(all_companies)} companies.")


    # --- Process Each Company ---
    processed_count = 0; error_count = 0; no_facts_count = 0; start_time = time.time()
    companies_to_process = list(all_companies.items()); total_companies_to_process = len(companies_to_process)
    logging.info(f"Starting processing for {total_companies_to_process} selected companies...")

    # Define standard headers for the data API calls (data.sec.gov)
    api_headers = {
        'User-Agent': USER_AGENT,
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'data.sec.gov' # Correct host for data.sec.gov APIs
    }

    for cik_str, company_info in companies_to_process:
        cik = int(cik_str); ticker = company_info['ticker']; title = company_info.get('title', '').strip() or 'N/A'
        processed_count += 1; log_prefix = f"({processed_count}/{total_companies_to_process}) CIK {cik_str} ({ticker or 'No Ticker'}):"

        # ETA calculation
        if processed_count > 1:
            elapsed_time = time.time() - start_time; avg_time_per_company = elapsed_time / (processed_count -1) if processed_count > 1 else 0
            remaining_companies = total_companies_to_process - processed_count; estimated_remaining_time = avg_time_per_company * remaining_companies
            eta_str = time.strftime("%H:%M:%S", time.gmtime(estimated_remaining_time)); logging.info(f"{log_prefix} Processing... (ETA: {eta_str})")
        else: logging.info(f"{log_prefix} Processing...")

        # Fetch facts using the standard API headers and standard get_sec_data (with verify=True by default)
        facts_url = COMPANY_FACTS_URL_TEMPLATE.format(cik=cik_str.zfill(10))
        # Use standard get_sec_data which has verify=True by default
        company_facts_json = get_sec_data(facts_url, api_headers)

        if company_facts_json:
            annual_data_by_year, entity_name = process_company_facts(cik, company_facts_json)
            effective_company_name = entity_name.strip() if entity_name and entity_name.strip() else title
            if annual_data_by_year:
                logging.info(f"{log_prefix} Found {len(annual_data_by_year)} years data for '{effective_company_name}'.")
                years_processed_count = 0
                for year, data in sorted(annual_data_by_year.items()):
                    years_processed_count += 1
                    try:
                        record_data = {'cik': cik, 'year': year, 'ticker': ticker, 'company_name': effective_company_name, 'form': data.get('form'), 'filed_date': data.get('filed_date'), 'period_end_date': data.get('period_end_date'), **{col: data.get(col) for col in DB_COLUMN_MAPPING.values()}}
                        record_obj = AnnualData(**record_data); db_session.merge(record_obj)
                        if years_processed_count % 50 == 0: logging.debug(f"{log_prefix} Committing batch..."); db_session.commit()
                    except Exception as e: logging.error(f"{log_prefix} DB error Year {year}: {e}", exc_info=False); error_count += 1; db_session.rollback()
                try: db_session.commit() # Final commit
                except Exception as e: logging.error(f"{log_prefix} Final commit error: {e}", exc_info=False); error_count += 1; db_session.rollback()
            else: logging.info(f"{log_prefix} No relevant annual data extracted for '{effective_company_name}'."); no_facts_count += 1
        else: logging.info(f"{log_prefix} No facts data retrieved."); no_facts_count += 1
        time.sleep(REQUEST_DELAY) # Rate limit

    # --- Final Summary ---
    end_time = time.time(); total_duration = end_time - start_time
    logging.info("-" * 60); logging.info(f"Finished processing {processed_count} companies in {time.strftime('%H:%M:%S', time.gmtime(total_duration))}.")
    logging.info(f"  {no_facts_count} companies had no facts data/no relevant annual data extracted."); logging.info(f"  Encountered {error_count} database errors."); logging.info("-" * 60)
    db_session.close(); logging.info("Database session closed.")