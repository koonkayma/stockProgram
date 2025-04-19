# <<< fetch_sec_annual_financials.py >>>

import requests
import json
import time
import logging
import os
import warnings # To suppress InsecureRequestWarning
import re # Import regex for camel_to_snake
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
    # Replace with your actual MariaDB connection string if not using environment variable
    'mysql+mysqlconnector://nextcloud:Ks120909090909#@127.0.0.1:3306/nextcloud'
)

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANY_FACTS_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# Financial Tags to Extract (use 'taxonomy:Tag' format) - Keys are CamelCase for easier reading here
DESIRED_TAGS = {
    "Assets": "us-gaap:Assets",
    "Liabilities": "us-gaap:Liabilities",
    "StockholdersEquity": "us-gaap:StockholdersEquity",
    "CashAndEquivalents": "us-gaap:CashAndCashEquivalentsAtCarryingValue",
    "AccountsReceivableNet": "us-gaap:AccountsReceivableNetCurrent",
    "InventoryNet": "us-gaap:InventoryNet",
    "PropertyPlantEquipmentNet": "us-gaap:PropertyPlantAndEquipmentNet",
    "AccumulatedDepreciation": "us-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
    "AccountsPayable": "us-gaap:AccountsPayableCurrent",
    "AccruedLiabilitiesCurrent": "us-gaap:AccruedLiabilitiesCurrent",
    "DebtCurrent": "us-gaap:DebtCurrent",
    "LongTermDebtNoncurrent": "us-gaap:LongTermDebtNoncurrent",
    "Revenues": "us-gaap:Revenues",
    "CostOfRevenue": "us-gaap:CostOfRevenue",
    "GrossProfit": "us-gaap:GrossProfit",
    "OperatingExpenses": "us-gaap:OperatingExpenses",
    "OperatingIncomeLoss": "us-gaap:OperatingIncomeLoss",
    "InterestExpense": "us-gaap:InterestExpense",
    "IncomeBeforeTax": "us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxExtraordinaryItemsNoncontrollingInterest",
    "IncomeTaxExpenseBenefit": "us-gaap:IncomeTaxExpenseBenefit",
    "NetIncomeLoss": "us-gaap:NetIncomeLoss",
    "EPSBasic": "us-gaap:EarningsPerShareBasic",
    "EPSDiluted": "us-gaap:EarningsPerShareDiluted",
    "SharesOutstanding": "dei:EntityCommonStockSharesOutstanding", # Note: dei namespace
    "SharesBasicWeightedAvg": "us-gaap:WeightedAverageNumberOfSharesOutstandingBasic",
    "SharesDilutedWeightedAvg": "us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding",
    "OperatingCashFlow": "us-gaap:NetCashProvidedByUsedInOperatingActivities",
    "CapitalExpenditures": "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
    "DepreciationAndAmortization": "us-gaap:DepreciationAndAmortization",
    "DividendsPaid": "us-gaap:PaymentsOfDividends",
}

REQUEST_DELAY = 0.11 # Minimum 10 requests per second allowed by SEC

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Helper Functions ---

def camel_to_snake(name):
    """Converts CamelCase names to snake_case for database column compatibility."""
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()

# Modified get_sec_data to accept verify flag - BUT WE WILL CALL requests.get directly for ticker
def get_sec_data(url, headers, verify_ssl=True):
    """Fetches data from SEC API, handles basic errors and retries."""
    retries = 3
    last_exception = None
    for attempt in range(retries):
        try:
            # Use the verify_ssl flag passed to the function
            response = requests.get(url, headers=headers, timeout=30, verify=verify_ssl)
            response.raise_for_status() # Raises HTTPError for 4xx/5xx status codes
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' not in content_type:
                logging.warning(f"Unexpected Content-Type '{content_type}' for {url} in get_sec_data.")
                # Attempt to decode anyway if possible, otherwise return None
                try:
                    return response.json()
                except json.JSONDecodeError:
                    logging.error(f"Content was not valid JSON despite warning for {url}.")
                    return None
            return response.json()
        except requests.exceptions.SSLError as e:
            logging.error(f"SSL Error for {url}: {e}. If using verify=False, ensure it's intended.")
            last_exception = e
            return None # Don't retry SSL errors usually
        except requests.exceptions.ConnectionError as e:
            logging.warning(f"Connection error for {url}: {e}. Retrying {attempt + 1}/{retries}...")
            last_exception = e
            time.sleep(2 ** attempt) # Exponential backoff
        except requests.exceptions.Timeout as e:
            logging.warning(f"Timeout error for {url}: {e}. Retrying {attempt + 1}/{retries}...")
            last_exception = e
            time.sleep(2 ** attempt) # Exponential backoff
        except requests.exceptions.HTTPError as e:
            last_exception = e
            if response.status_code == 404:
                logging.warning(f"HTTP 404 Not Found for {url}. Likely no data for CIK.")
                return None # No point retrying 404
            elif response.status_code == 403:
                logging.error(f"HTTP 403 Forbidden for {url}. CRITICAL: CHECK YOUR USER-AGENT! It might be blocked.")
                return None # No point retrying 403 without fixing agent
            elif response.status_code == 429:
                wait_time = 5 * (attempt + 1) # Increase wait time for rate limiting
                logging.warning(f"HTTP 429 Rate limit hit for {url}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logging.error(f"HTTP Error {response.status_code} for {url}: {e}")
                # Optionally retry for 5xx server errors
                if 500 <= response.status_code < 600:
                   logging.warning(f"Retrying server error {response.status_code}...")
                   time.sleep(2 ** attempt)
                else:
                    return None # Don't retry other client errors
        except json.JSONDecodeError as e:
            last_exception = e
            logging.error(f"Failed to decode JSON from {url}: {e}")
            # Log part of the response text if possible
            try: logging.error(f"Response text (partial): {response.text[:200]}")
            except: pass
            return None
        except Exception as e: # Catch any other unexpected errors
            last_exception = e
            logging.error(f"Unexpected error fetching {url}: {type(e).__name__} - {e}", exc_info=False) # exc_info=True for traceback
            # Consider retrying certain unexpected errors if appropriate
            time.sleep(1) # Small pause before potential retry

    logging.error(f"Failed to fetch {url} after {retries} retries. Last error: {last_exception}")
    return None

def parse_date(date_str):
    """Safely parses a YYYY-MM-DD string into a date object."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except (ValueError, TypeError):
        logging.warning(f"Could not parse date string: '{date_str}'")
        return None

def safe_decimal(value):
    """Safely converts a value to a Decimal, handling None, strings, ints, floats."""
    if value is None: return None
    if isinstance(value, Decimal): return value
    try:
        # Convert potential scientific notation strings before Decimal conversion
        if isinstance(value, str): value = value.strip()
        if not value: return None # Handle empty strings after stripping
        # Ensure float conversion first if it's numeric but not Decimal yet
        num_val = float(value)
        return Decimal(str(num_val)) # Convert via string to avoid float precision issues
    except (ValueError, TypeError, InvalidOperation):
        logging.warning(f"Could not convert value '{value}' (type: {type(value)}) to Decimal.")
        return None

def safe_int_or_bigint(value):
    """Safely converts a value to an integer, handling None, strings, decimals."""
    if value is None: return None
    if isinstance(value, int): return value
    try:
        # Use Decimal for intermediate conversion to handle large numbers/scientific notation
        dec_val = Decimal(value)
        # Check if the decimal has a fractional part
        if dec_val % 1 != 0:
            logging.debug(f"Value '{value}' has a fractional part, truncating for integer conversion.")
        # Convert the potentially fractional Decimal to int (truncates)
        return int(dec_val)
    except (InvalidOperation, ValueError, TypeError):
        logging.warning(f"Could not convert '{value}' (type: {type(value)}) to integer.")
        return None


# --- SQLAlchemy Model Definition ---
Base = declarative_base()
class AnnualData(Base):
    __tablename__ = 'sec_annual_data'

    # Core identifying columns
    cik = Column(INTEGER(unsigned=True), primary_key=True, comment='Company Identifier')
    year = Column(Integer, primary_key=True, comment='Fiscal Year')

    # Metadata columns
    ticker = Column(String(20), nullable=True, comment='Trading Symbol at time of fetch')
    company_name = Column(String(255), nullable=True, comment='Company Name')
    form = Column(String(10), nullable=True, comment='Source Form (e.g., 10-K)')
    filed_date = Column(Date, nullable=True, comment='Filing Date of the source form')
    period_end_date = Column(Date, nullable=True, comment='Period End Date for the fiscal year')

    # Dynamically create financial data columns based on DESIRED_TAGS
    # Uses snake_case for attribute/column names to match the database schema
    _column_definitions = {}
    for key, full_tag in DESIRED_TAGS.items():
        # *** Use camel_to_snake to generate DB-compatible column names ***
        col_name = camel_to_snake(key) # e.g., 'net_income_loss'
        col_type = None
        comment_str = f'{key} ({full_tag})' # Comment still shows original key and tag

        # Determine column type based on the snake_case name
        if 'shares' in col_name:
            # Use BigInteger variant for MySQL unsigned bigint compatibility
            col_type = BigInteger().with_variant(INTEGER(unsigned=True), "mysql")
        elif 'eps' in col_name:
            col_type = DECIMAL(18, 6) # Precision for EPS
        else:
            col_type = DECIMAL(28, 4) # Precision for monetary values

        # Add the column definition to the dictionary
        _column_definitions[col_name] = Column(col_type, nullable=True, comment=comment_str)

    # Use locals().update() to add the dynamically generated columns to the class definition
    locals().update(_column_definitions)

    # Audit column
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())

    # Define composite primary key constraint explicitly
    __table_args__ = (PrimaryKeyConstraint('cik', 'year'), {})

    def __repr__(self):
        return f"<AnnualData(cik={self.cik}, year={self.year}, ticker='{self.ticker}')>"

# --- Processing Function ---
def process_company_facts(cik, company_data):
    """
    Processes the raw JSON facts data for a single company.
    Extracts desired annual financial data, preferring 10-K filings and latest filing date.
    Returns a dictionary keyed by fiscal year, containing snake_case keyed financial data.
    """
    if not company_data or 'facts' not in company_data:
        logging.debug(f"CIK {cik}: No 'facts' key found in company data.")
        return {}, None # Return empty dict and None for name

    company_name = company_data.get('entityName', 'N/A')
    facts = company_data['facts']
    # This will store the final processed data for each year for this CIK
    # { year: {'col_name_snake_case': value, 'form': '10-K', 'filed_date': date, 'period_end_date': date}, ... }
    annual_results = {}

    # Iterate through the tags we want to extract
    for key, full_tag in DESIRED_TAGS.items():
        # *** Get the snake_case column name corresponding to the CamelCase key ***
        db_col_snake = camel_to_snake(key) # e.g., 'assets', 'net_income_loss'

        try:
            taxonomy, tag_name = full_tag.split(':')

            # Check if the tag exists in the facts data
            if taxonomy not in facts or tag_name not in facts[taxonomy]:
                # logging.debug(f"CIK {cik}: Tag {full_tag} not found in facts.")
                continue

            tag_data = facts[taxonomy][tag_name]
            units = tag_data.get('units')
            if not units:
                # logging.debug(f"CIK {cik}: Tag {full_tag} has no units.")
                continue

            # Determine the expected unit type and find the relevant unit key (USD, shares, USD/shares)
            unit_key = None
            is_share_metric = 'shares' in db_col_snake
            is_eps_metric = 'eps' in db_col_snake

            if is_share_metric and 'shares' in units: unit_key = 'shares'
            elif is_eps_metric and 'USD/shares' in units: unit_key = 'USD/shares'
            elif not is_share_metric and not is_eps_metric and 'USD' in units: unit_key = 'USD'
            elif units: # Fallback: if primary unit type isn't present, maybe another compatible one is
                 first_unit = list(units.keys())[0]
                 expected_unit_part = 'shares' if is_share_metric else ('USD/shares' if is_eps_metric else 'USD')
                 # Only log if the fallback unit seems unexpected
                 if expected_unit_part not in first_unit:
                     logging.debug(f"CIK {cik}, Tag {full_tag}: Expected unit containing '{expected_unit_part}', using first available unit '{first_unit}'.")
                 unit_key = first_unit # Use the first unit key found

            # If no suitable unit key was found, skip this tag
            if not unit_key or unit_key not in units:
                # logging.debug(f"CIK {cik}: Tag {full_tag} - No valid unit key '{unit_key}' found in units {list(units.keys())}")
                continue

            unit_data = units[unit_key]
            # Temporary dict to find the single best entry for each fiscal year *for this specific tag*
            yearly_data_for_tag = {}

            for entry in unit_data:
                # Filter for annual data (FY), ensure fiscal year and filing date exist
                if entry.get('form') and entry.get('fp') == 'FY' and entry.get('fy') is not None:
                    fy = entry.get('fy') # Fiscal year (integer)
                    filed_date = parse_date(entry.get('filed'))
                    if not filed_date:
                        continue # Skip entries with unparseable filing dates

                    # Logic to select the best entry for a given fiscal year:
                    # Prefer 10-K forms. If forms are the same type, prefer the latest filing date.
                    is_better_candidate = False
                    if fy not in yearly_data_for_tag:
                        is_better_candidate = True # First entry found for this year
                    else:
                        current_best = yearly_data_for_tag[fy]
                        current_form = current_best.get('form')
                        entry_form = entry.get('form')
                        # Rule 1: If new entry is 10-K and current best is not, new one is better.
                        if entry_form == '10-K' and current_form != '10-K':
                            is_better_candidate = True
                        # Rule 2: If forms are same type (both 10-K or both not 10-K), pick later filing date.
                        elif (entry_form == current_form) or (entry_form != '10-K' and current_form != '10-K'):
                           if filed_date > current_best['filed_date']:
                               is_better_candidate = True
                        # Rule 3: If new is not 10-K but current is, current remains better (no change).

                    if is_better_candidate:
                        # Parse the value safely based on metric type
                        value_to_store = safe_int_or_bigint(entry.get('val')) if is_share_metric else safe_decimal(entry.get('val'))
                        # Only store if the value was successfully parsed
                        if value_to_store is not None:
                            yearly_data_for_tag[fy] = {
                                'val': value_to_store,
                                'filed_date': filed_date,
                                'form': entry.get('form'),
                                'end_date': parse_date(entry.get('end')) # Period end date
                            }
                        else:
                            logging.debug(f"CIK {cik}, Tag {full_tag}, FY {fy}: Value '{entry.get('val')}' could not be parsed.")

            # Integrate the best data found *for this tag* into the main annual_results dictionary
            for year, data in yearly_data_for_tag.items():
                if year not in annual_results:
                    # Initialize the dictionary for this fiscal year if it doesn't exist
                    annual_results[year] = {}

                # *** Store the value using the snake_case column name as the key ***
                annual_results[year][db_col_snake] = data['val']

                # Store/update form, filed_date, period_end_date for the year based on this tag's entry.
                # We generally want these metadata fields to come from the 'best' source (latest 10-K if possible).
                # Update if the current tag's entry is 'better' than the existing metadata source for the year.
                update_metadata = False
                if 'form' not in annual_results[year]: # If no metadata stored yet
                    update_metadata = True
                else:
                    current_meta_form = annual_results[year].get('form')
                    current_meta_filed = annual_results[year].get('filed_date')
                    tag_entry_form = data['form']
                    tag_entry_filed = data['filed_date']

                    if tag_entry_form == '10-K' and current_meta_form != '10-K':
                        update_metadata = True
                    elif (tag_entry_form == current_meta_form or (tag_entry_form != '10-K' and current_meta_form != '10-K')):
                        if tag_entry_filed and current_meta_filed and tag_entry_filed > current_meta_filed:
                             update_metadata = True
                        elif tag_entry_filed and not current_meta_filed: # Update if new date exists and old one didn't
                             update_metadata = True

                if update_metadata:
                     annual_results[year]['form'] = data['form']
                     annual_results[year]['filed_date'] = data['filed_date']
                     annual_results[year]['period_end_date'] = data['end_date']

        except Exception as e:
            logging.error(f"Error processing tag {full_tag} (col: {db_col_snake}) for CIK {cik}: {type(e).__name__} - {e}", exc_info=False) # Set exc_info=True for full traceback if needed

    # Return the dictionary keyed by year, containing snake_case financial data and metadata
    # Also return the determined company name
    return annual_results, company_name


# --- Main Execution ---
if __name__ == "__main__":
    logging.info("Starting SEC Annual Data Fetcher.")
    logging.info(f"Using User-Agent: {USER_AGENT}")
    logging.info(f"Database Target: {DB_CONNECTION_STRING.split('@')[-1] if '@' in DB_CONNECTION_STRING else DB_CONNECTION_STRING}")

    # --- Database Connection ---
    try:
        engine = create_engine(DB_CONNECTION_STRING, pool_recycle=3600, echo=False) # echo=True for debugging SQL
        # Create the table if it doesn't exist based on the SQLAlchemy model
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        db_session = Session()
        logging.info("Database connection successful and table schema ensured.")
    except Exception as e:
        logging.error(f"Database connection or table creation failed: {e}", exc_info=True)
        exit(1)

    # --- Fetch Company List ---
    logging.info("Fetching company CIK list from SEC...")

    # Define specific headers JUST for the ticker request (www.sec.gov)
    ticker_fetch_headers = { 'User-Agent': USER_AGENT }
    company_tickers_data = None

    logging.info(f"Attempting request to {COMPANY_TICKERS_URL} with headers: {ticker_fetch_headers}")
    try:
        # Use standard requests.get, SSL verification enabled by default (verify=True)
        response = requests.get(COMPANY_TICKERS_URL, headers=ticker_fetch_headers, timeout=30) # verify=True is default

        logging.info(f"Ticker URL request completed. Status Code: {response.status_code}")
        logging.debug(f"Ticker URL Response Headers: {response.headers}")
        response.raise_for_status() # Check for 4xx/5xx errors AFTER logging status

        content_type = response.headers.get('Content-Type', '')
        if 'application/json' in content_type:
            company_tickers_data = response.json()
            logging.info("Successfully fetched and decoded company tickers JSON.")
        else:
            logging.warning(f"Ticker URL ({COMPANY_TICKERS_URL}) returned unexpected Content-Type: {content_type}. Attempting to decode anyway.")
            try:
                 company_tickers_data = response.json()
                 logging.info("Successfully decoded company tickers JSON despite unexpected Content-Type.")
            except json.JSONDecodeError as json_err:
                 logging.error(f"Failed to decode JSON from ticker URL after warning. Error: {json_err}")
                 logging.debug(f"Ticker URL Response Text (non-JSON): {response.text[:500]}")
                 company_tickers_data = None # Ensure it's None if decoding fails

    except requests.exceptions.HTTPError as e:
         logging.error(f"HTTP Error fetching tickers: Status {e.response.status_code} for {COMPANY_TICKERS_URL}.", exc_info=False)
         try: logging.error(f"Response text (partial): {e.response.text[:1000]}")
         except: pass
    except requests.exceptions.RequestException as e:
        logging.error(f"RequestException fetching tickers: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"JSONDecodeError processing tickers response: {e}. Content-Type was {content_type}.")
        try: logging.error(f"Response text (partial): {response.text[:200]}")
        except: pass
    except Exception as e: # Catch-all for other unexpected errors
        logging.error(f"Unexpected error fetching tickers: {type(e).__name__} - {e}", exc_info=True)

    time.sleep(REQUEST_DELAY) # Pause after fetching tickers list, before hitting data API

    if not company_tickers_data:
        logging.error("Failed to fetch or decode company tickers list. Cannot proceed without it. Exiting.")
        if 'db_session' in locals() and db_session.is_active: db_session.close()
        exit(1)

    # --- Process Ticker List ---
    all_companies = {}
    try:
        # The structure seems to be a dictionary where keys are indices ("0", "1", ...)
        # and values are dictionaries containing 'cik_str', 'ticker', 'title'.
        if isinstance(company_tickers_data, dict):
            all_companies = {
                str(info['cik_str']): {'title': info['title'], 'ticker': info['ticker']}
                for key, info in company_tickers_data.items() if info and 'cik_str' in info and 'title' in info and 'ticker' in info
            }
        # Less common, but handle if it's a list of dictionaries directly
        elif isinstance(company_tickers_data, list):
             all_companies = {
                str(info['cik_str']): {'title': info['title'], 'ticker': info['ticker']}
                for info in company_tickers_data if info and 'cik_str' in info and 'title' in info and 'ticker' in info
            }
        else:
            logging.error(f"Company tickers data is in an unexpected format: {type(company_tickers_data)}. Cannot process.")
            if 'db_session' in locals() and db_session.is_active: db_session.close()
            exit(1)
        logging.info(f"Processed info for {len(all_companies)} companies from the ticker list.")
    except Exception as e:
        logging.error(f"Error processing the fetched company tickers data structure: {e}", exc_info=True)
        if 'db_session' in locals() and db_session.is_active: db_session.close()
        exit(1)

    if not all_companies:
        logging.warning("Ticker list was fetched but resulted in an empty company dictionary. Check processing logic or source data format.")
        if 'db_session' in locals() and db_session.is_active: db_session.close()
        exit(1)


    # --- Process Each Company ---
    processed_count = 0
    error_count = 0
    no_facts_count = 0
    start_time = time.time()

    # --- !!! MODIFICATION FOR TESTING: LIMIT TO 5 COMPANIES !!! ---
    full_companies_list = list(all_companies.items())
    limit_for_testing = 5 # Set the number of companies to test
    if len(full_companies_list) >= limit_for_testing:
        companies_to_process = full_companies_list[:limit_for_testing]
        logging.warning(f"--- TESTING MODE: Processing only the first {limit_for_testing} companies ---")
    else:
        companies_to_process = full_companies_list # Process all if less than the limit are available
        logging.warning(f"--- TESTING MODE: Fewer than {limit_for_testing} companies found ({len(full_companies_list)}), processing all available. ---")

    total_companies_to_process = len(companies_to_process) # Use the length of the *limited* list for logging/ETA
    # --- !!! END OF MODIFICATION !!! ---

    logging.info(f"Starting processing for {total_companies_to_process} selected companies...")

    # Define standard headers for the data API calls (data.sec.gov)
    api_headers = {
        'User-Agent': USER_AGENT,
        'Accept-Encoding': 'gzip, deflate', # Ask for compression
        'Host': 'data.sec.gov' # Be explicit about the host
    }

    # Loop through the potentially limited list of companies
    for cik_str, company_info in companies_to_process:
        try:
            cik = int(cik_str)
        except ValueError:
            logging.warning(f"Invalid CIK string found: '{cik_str}'. Skipping.")
            processed_count += 1 # Count as processed even if skipped
            continue

        ticker = company_info.get('ticker', 'N/A') # Use N/A if ticker missing
        title = company_info.get('title', '').strip() or 'N/A' # Use N/A if title missing/empty
        processed_count += 1
        # Log prefix now reflects the limited total, e.g., (1/5)
        log_prefix = f"({processed_count}/{total_companies_to_process}) CIK {cik_str} ({ticker}):"

        # --- ETA Calculation (Adjusted for testing) ---
        if processed_count > 1 and total_companies_to_process > 1:
            elapsed_time = time.time() - start_time
            avg_time_per_company = elapsed_time / (processed_count -1) if processed_count > 1 else 0
            if avg_time_per_company > 0:
                 remaining_companies = total_companies_to_process - processed_count
                 if remaining_companies > 0:
                    estimated_remaining_time = avg_time_per_company * remaining_companies
                    eta_str = time.strftime("%H:%M:%S", time.gmtime(estimated_remaining_time))
                    logging.info(f"{log_prefix} Processing '{title}'... (Est. Time Remaining for test run: {eta_str})")
                 else:
                    logging.info(f"{log_prefix} Processing '{title}'... (Last company in test run)")
            else:
                 logging.info(f"{log_prefix} Processing '{title}'...")
        else:
            logging.info(f"{log_prefix} Processing '{title}'...") # First or only company

        # --- Fetch Company Facts ---
        facts_url = COMPANY_FACTS_URL_TEMPLATE.format(cik=cik_str.zfill(10))
        company_facts_json = get_sec_data(facts_url, api_headers) # verify=True is default

        # --- Process Facts and Merge to DB ---
        if company_facts_json:
            annual_data_by_year, entity_name = process_company_facts(cik, company_facts_json)
            effective_company_name = entity_name.strip() if entity_name and entity_name.strip() and entity_name != 'N/A' else title

            if annual_data_by_year:
                logging.info(f"{log_prefix} Found {len(annual_data_by_year)} years of potential data for '{effective_company_name}'. Merging into DB...")
                years_processed_for_cik = 0
                years_merged_count = 0
                for year, data in sorted(annual_data_by_year.items()):
                    years_processed_for_cik += 1
                    try:
                        # Prepare data dict; keys are snake_case from process_company_facts
                        record_data_for_year = {
                            'ticker': ticker,
                            'company_name': effective_company_name,
                            'form': data.get('form'),
                            'filed_date': data.get('filed_date'),
                            'period_end_date': data.get('period_end_date'),
                            **data # Unpack financial metrics and potentially metadata
                        }
                        # Clean up keys not part of the model or handled separately
                        record_data_for_year.pop('cik', None)
                        record_data_for_year.pop('year', None)
                        record_data_for_year.pop('val', None)

                        # Create SQLAlchemy object and merge
                        record_obj = AnnualData(cik=cik, year=year, **record_data_for_year)
                        db_session.merge(record_obj)
                        years_merged_count +=1

                        # Commit periodically within a CIK
                        if years_merged_count % 20 == 0:
                            # logging.debug(f"{log_prefix} Committing batch...")
                            db_session.commit()

                    except Exception as e:
                        logging.error(f"{log_prefix} DB merge error Year {year} for '{effective_company_name}': {type(e).__name__} - {e}", exc_info=False)
                        error_count += 1
                        db_session.rollback()
                        # Consider breaking the inner loop for this CIK if one year fails,
                        # depending on desired behavior. For now, it continues to the next year.
                try:
                    db_session.commit() # Final commit for the current CIK
                    logging.info(f"{log_prefix} Finished merge for '{effective_company_name}'. Merged {years_merged_count} year(s) data.")
                except Exception as e:
                    logging.error(f"{log_prefix} Final commit error for CIK '{effective_company_name}': {type(e).__name__} - {e}", exc_info=False)
                    error_count += 1
                    db_session.rollback()
            else:
                logging.info(f"{log_prefix} No relevant annual data points extracted for '{effective_company_name}' after processing facts.")
                no_facts_count += 1
        else:
            logging.info(f"{log_prefix} No facts data retrieved from API for '{title}'.")
            no_facts_count += 1

        # --- Rate Limiting ---
        time.sleep(REQUEST_DELAY) # Pause between CIKs

    # --- Final Summary ---
    end_time = time.time()
    total_duration = end_time - start_time
    logging.info("-" * 60)
    # Modify summary message to reflect testing mode
    logging.info(f"Finished TEST processing {processed_count} CIKs from list (limited to {total_companies_to_process}).")
    logging.info(f"Total time: {time.strftime('%H:%M:%S', time.gmtime(total_duration))}.")
    logging.info(f"  {no_facts_count} CIKs had no facts data retrieved or no relevant annual data extracted.")
    logging.info(f"  Encountered {error_count} database errors during merge/commit.")
    logging.info("-" * 60)

    # --- Cleanup ---
    if 'db_session' in locals() and db_session.is_active:
        db_session.close()
        logging.info("Database session closed.")
    else:
        logging.info("No active database session to close.")