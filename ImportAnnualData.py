import requests
import pandas as pd
import time
import logging
import mysql.connector
from mysql.connector import Error
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, timedelta # Import timedelta
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed # For potential parallelization

# --- Configuration ---
YEARS_TO_FETCH = 5 # Fetch 5 years of data
SOURCE_API_NAME = "FMP_Direct_Annual"

# --- FMP Configuration ---
FMP_BASE_URL = "https://financialmodelingprep.com/api/v3"
FMP_API_KEY = os.environ.get("FMP_API_KEY", "IQ7xQeQoWApWqfkuxZl88l1A22p4qLw5") # Use environment variable!
if FMP_API_KEY == "IQ7xQeQoWApWqfkuxZl88l1A22p4qLw5": logging.warning("Using hardcoded FMP API key. Use environment variables.")
elif not FMP_API_KEY: logging.critical("FMP_API_KEY not found. Exiting."); exit()

# TradingView API config (for ticker list)
TRADINGVIEW_API_URL = "https://scanner.tradingview.com/america/scan"
TV_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Content-Type": "application/json"
}
TV_DELAY_BETWEEN_BATCHES = 1.1
TV_REQUEST_TIMEOUT = 25

# API Delays & Retries
FMP_REQUEST_DELAY = 0.15 # Delay between *any* FMP request to avoid rapid fire
FMP_RETRY_COUNT = 3
FMP_RETRY_DELAY_START = 1 # Seconds for first retry delay
FMP_REQUEST_TIMEOUT = 20 # Timeout for individual FMP requests
TICKER_PROCESSING_DELAY = 0.05 # Can be lower now as many tickers will be skipped

# --- Data Freshness Configuration ---
DATA_REFRESH_INTERVAL_HOURS = 24 # Refresh data older than 24 hours

# --- Database Configuration ---
DB_HOST = os.environ.get("DB_HOST", "192.168.1.142")
DB_PORT = os.environ.get("DB_PORT", 3306)
DB_NAME = os.environ.get("DB_NAME", "nextcloud")
DB_USER = os.environ.get("DB_USER", "nextcloud") # <-- REPLACE or set env var
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Ks120909090909#") # <-- REPLACE or set env var
DB_TABLE_ANNUAL = "stock_annual_financials" # New table name

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-8s - %(name)s - %(message)s', # Added logger name
    handlers=[
        logging.FileHandler("importAnnualData.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__) # Create logger instance
logging.getLogger("urllib3").setLevel(logging.WARNING)

# --- Database Connection ---
def create_db_connection() -> Optional[mysql.connector.MySQLConnection]:
    """Creates and returns a database connection."""
    connection = None
    logger.debug(f"Attempting DB connection to {DB_HOST}:{DB_PORT}, DB: {DB_NAME}, User: {DB_USER}")
    try:
        connection = mysql.connector.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, connection_timeout=10
        )
        if connection.is_connected(): logger.info("MariaDB connection successful")
        else: logger.error("MariaDB connection failed."); connection = None
    except Error as e: logger.error(f"Error connecting to MariaDB: {e}")
    except Exception as e: logger.error(f"Unexpected error during DB connection: {e}")
    return connection

# --- Ticker Fetching (from TradingView) ---
def get_all_us_stocks() -> List[str]:
    """Fetches a list of US stock tickers from TradingView and cleans them."""
    # (This function remains identical to the previous correct version)
    logger.debug("Entering get_all_us_stocks function.")
    all_tickers_with_exchange = []; current_range = 0; batch_size = 1500; max_tickers_to_fetch = 30000
    logger.info("Attempting to fetch US stock list from TradingView...")
    while current_range < max_tickers_to_fetch:
        payload = { "filter": [{"left": "exchange", "operation": "in_range", "right": ["NYSE", "NASDAQ", "AMEX"]}, {"left": "is_primary", "operation": "equal", "right": True}, {"left": "type", "operation": "in_range", "right": ["stock", "dr"]}, {"left": "subtype", "operation": "in_range", "right": ["common", "", "preferred", "foreign-issuer", "american_depository_receipt", "reit", "trust"]}, {"left": "market_cap_basic", "operation": "greater", "right": 10000000}], "options": {"lang": "en"}, "markets": ["america"], "symbols": {"query": {"types": []}, "tickers": []}, "columns": ["name"], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}, "range": [current_range, current_range + batch_size] }
        logger.debug(f"Requesting TV batch range: [{current_range}, {current_range + batch_size}]")
        try:
            response = requests.post(TRADINGVIEW_API_URL, json=payload, headers=TV_HEADERS, timeout=TV_REQUEST_TIMEOUT)
            logger.debug(f"TV API Response Status Code: {response.status_code} for range {current_range}")
            if response.status_code != 200: logger.warning(f"TV API Response Text (non-200): {response.text[:500]}...")
            response.raise_for_status(); data = response.json()
            if not isinstance(data, dict) or 'data' not in data or not isinstance(data['data'], list): logger.warning(f"Unexpected TV API response structure for range {current_range}."); time.sleep(TV_DELAY_BETWEEN_BATCHES * 2); continue
            if not data['data']: logger.info(f"No more data from TradingView at range {current_range}. Total fetched: {len(all_tickers_with_exchange)}"); break
            batch_tickers = [item['d'][0] for item in data['data'] if item and 'd' in item and item['d']]
            all_tickers_with_exchange.extend(batch_tickers)
            logger.info(f"Fetched TV batch {current_range // batch_size + 1} ({len(batch_tickers)} tickers). Total: {len(all_tickers_with_exchange)}")
            if len(batch_tickers) < batch_size: logger.info(f"Last TV batch smaller than requested, assuming end of list."); break
            current_range += batch_size; logger.debug(f"Sleeping for {TV_DELAY_BETWEEN_BATCHES} sec..."); time.sleep(TV_DELAY_BETWEEN_BATCHES)
        except requests.exceptions.Timeout: logger.warning(f"Timeout fetching TV stocks batch {current_range}. Retrying..."); time.sleep(10); continue
        except requests.exceptions.RequestException as e: logger.error(f"HTTP Error fetching TV stocks batch {current_range}: {e}"); current_range += batch_size; time.sleep(5)
        except Exception as e: logger.error(f"Unexpected error processing TV stock batch {current_range}: {e}", exc_info=True); break
    if not all_tickers_with_exchange: logger.error("Could not fetch any stock tickers from TradingView."); return []
    logger.info(f"Fetched {len(all_tickers_with_exchange)} raw tickers. Starting cleaning...")
    cleaned_tickers = set(); skipped_count = 0
    for ticker_raw in all_tickers_with_exchange: # Simplified Cleaning Logic
        symbol = None
        if ":" in ticker_raw:
            try: prefix, potential_symbol = ticker_raw.split(":", 1); symbol = potential_symbol if prefix in ["NASDAQ", "NYSE", "AMEX", "OTC", "ARCA", "BATS", "OTCBB", "PINX"] else None
            except ValueError: pass
        else: symbol = ticker_raw
        if symbol:
            original_symbol = symbol; symbol = symbol.replace('.', '-').split('/')[0]
            if symbol and all(c.isalnum() or c in ['-', '.'] for c in symbol): cleaned_tickers.add(symbol)
            else: skipped_count += 1; logger.debug(f"Skipping symbol '{symbol}' from '{original_symbol}'")
        else: skipped_count += 1
    if skipped_count > 0: logger.info(f"Skipped {skipped_count} tickers during cleaning.")
    final_list = sorted(list(cleaned_tickers)); logger.info(f"Cleaning complete. Found {len(final_list)} unique, potentially valid symbols.")
    if not final_list: logger.warning("Cleaned ticker list is empty!")
    logger.debug("Exiting get_all_us_stocks function."); return final_list

# --- FMP Data Fetching Helper ---
def fetch_fmp_data(endpoint_path: str) -> Optional[List[Dict]]:
    """ Fetches data from FMP endpoint with rate limit handling and retries. """
    # (This function remains identical)
    url = f"{FMP_BASE_URL}/{endpoint_path}&apikey={FMP_API_KEY}"; base_url_log = f"{FMP_BASE_URL}/{endpoint_path}"
    retries = FMP_RETRY_COUNT; delay = FMP_RETRY_DELAY_START
    while retries > 0:
        try:
            logger.debug(f"Requesting FMP: {base_url_log}..."); response = requests.get(url, timeout=FMP_REQUEST_TIMEOUT)
            logger.debug(f"Response Status: {response.status_code}")
            if response.status_code == 200:
                try:
                    data = response.json()
                    if isinstance(data, list): return data
                    elif isinstance(data, dict) and data.get("Error Message"): logger.warning(f"FMP API Error for {base_url_log}: {data['Error Message']}"); return None
                    else: logger.warning(f"Unexpected JSON structure from FMP for {base_url_log}: {str(data)[:200]}"); return None
                except json.JSONDecodeError as e: logger.error(f"Failed to decode FMP JSON for {base_url_log}: {e}. Resp: {response.text[:200]}"); return None
            elif response.status_code == 429: logger.warning(f"Rate limit (429) hit for {base_url_log}. Retrying in {delay}s... ({FMP_RETRY_COUNT-retries+1}/{FMP_RETRY_COUNT})"); time.sleep(delay); delay *= 2; retries -= 1
            elif 400 <= response.status_code < 500: logger.error(f"FMP Client Error {response.status_code} for {base_url_log}: {response.text[:200]}"); return None
            else: logger.warning(f"FMP Server Error {response.status_code} for {base_url_log}. Retrying in {delay}s... ({FMP_RETRY_COUNT-retries+1}/{FMP_RETRY_COUNT})"); time.sleep(delay); delay *= 2; retries -= 1
        except requests.exceptions.Timeout: logger.warning(f"Timeout requesting {base_url_log}. Retrying in {delay}s... ({FMP_RETRY_COUNT-retries+1}/{FMP_RETRY_COUNT})"); time.sleep(delay); delay *= 2; retries -= 1
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error requesting {base_url_log}: {e}")
            if retries == FMP_RETRY_COUNT: logger.warning(f"Retrying network error in {delay}s..."); time.sleep(delay); delay *=2; retries -=1;
            else: logger.error("Network error persisted. Giving up."); return None
        except Exception as e: logger.error(f"Unexpected error fetching {base_url_log}: {e}", exc_info=True); return None
    logger.error(f"Failed to fetch data from {base_url_log} after {FMP_RETRY_COUNT} retries."); return None

# --- Process Ticker Data ---
def process_ticker(ticker: str) -> Optional[pd.DataFrame]:
    """Fetches IS, BS, CF statements for a ticker and merges them."""
    # (This function remains identical except for the extract_year definition)
    logger.info(f"[{ticker}] Fetching {YEARS_TO_FETCH} years of FMP statements...")
    income_statements = fetch_fmp_data(f"income-statement/{ticker}?period=annual&limit={YEARS_TO_FETCH}")
    time.sleep(FMP_REQUEST_DELAY)
    balance_sheets = fetch_fmp_data(f"balance-sheet-statement/{ticker}?period=annual&limit={YEARS_TO_FETCH}")
    time.sleep(FMP_REQUEST_DELAY)
    cash_flow_statements = fetch_fmp_data(f"cash-flow-statement/{ticker}?period=annual&limit={YEARS_TO_FETCH}")
    if income_statements is None or balance_sheets is None or cash_flow_statements is None: logger.warning(f"[{ticker}] Failed to fetch one or more required statements."); return None
    if not income_statements or not balance_sheets or not cash_flow_statements: logger.warning(f"[{ticker}] One or more statements returned empty lists."); return None

    # ***** CORRECTED FUNCTION DEFINITION IS EMBEDDED HERE *****
    def extract_year(stmt_dict):
        """Helper to get year from FMP dict, preferring calendarYear."""
        yr_str = stmt_dict.get('calendarYear')
        if yr_str:
            try:
                # FMP sometimes returns year as float string "2023.0"
                return int(float(yr_str))
            except (ValueError, TypeError):
                # Added logging for parsing failure
                logger.debug(f"[{ticker}] Couldn't parse calendarYear '{yr_str}' as int/float, falling back to date.")
                # Fall through if not integer/float
                pass # Explicitly pass after logging

        # Fallback to date field if calendarYear wasn't valid or present
        date_str = stmt_dict.get('date', '0') # Default to '0' if date missing
        try:
            # Extract year part from YYYY-MM-DD format
            year_part = date_str.split('-')[0]
            return int(year_part)
        except (ValueError, TypeError, IndexError, AttributeError):
            # Handle various errors: non-numeric year, bad split, date not string
            # Moved logging inside the except block
            logger.warning(f"[{ticker}] Could not parse year from date: '{date_str}' in stmt: {stmt_dict}")
            return 0 # Return 0 to indicate failure, will be filtered later
    # ***** END OF EMBEDDED FUNCTION DEFINITION *****

    try:
        is_data = [{'year': extract_year(s), 'revenue': s.get('revenue'), 'gross_profit': s.get('grossProfit'), 'operating_income': s.get('operatingIncome'), 'net_income': s.get('netIncome'), 'interest_expense': s.get('interestExpense'), 'income_before_tax': s.get('incomeBeforeTax'), 'ebitda': s.get('ebitda'), 'eps': s.get('eps'), 'report_date': s.get('fillingDate', s.get('date')), 'currency': s.get('reportedCurrency'), 'da_is': s.get('depreciationAndAmortization')} for s in income_statements if extract_year(s) > 0]
        if not is_data: raise ValueError("No valid IS records")
        is_df = pd.DataFrame(is_data).set_index('year'); is_df = is_df[~is_df.index.duplicated(keep='last')]
        bs_data = [{'year': extract_year(s), 'total_assets': s.get('totalAssets'), 'total_liabilities': s.get('totalLiabilities'), 'total_debt': s.get('totalDebt'), 'total_equity': s.get('totalEquity'), 'cash_and_equivalents': s.get('cashAndCashEquivalents')} for s in balance_sheets if extract_year(s) > 0]
        if not bs_data: raise ValueError("No valid BS records")
        bs_df = pd.DataFrame(bs_data).set_index('year'); bs_df = bs_df[~bs_df.index.duplicated(keep='last')]
        cf_data = [{'year': extract_year(s), 'operating_cash_flow': s.get('operatingCashFlow'), 'capital_expenditure': s.get('capitalExpenditure'), 'free_cash_flow': s.get('freeCashFlow'), 'dividends_paid': s.get('dividendsPaid'), 'depreciation_amortization': s.get('depreciationAndAmortization')} for s in cash_flow_statements if extract_year(s) > 0]
        if not cf_data: raise ValueError("No valid CF records")
        cf_df = pd.DataFrame(cf_data).set_index('year'); cf_df = cf_df[~cf_df.index.duplicated(keep='last')]

        combined_df = is_df.join(cf_df, how='outer', lsuffix='_is', rsuffix='_cf'); combined_df = combined_df.join(bs_df, how='outer')
        combined_df['depreciation_amortization'] = combined_df['depreciation_amortization'].fillna(combined_df['da_is'])
        combined_df.drop(columns=['da_is'], inplace=True, errors='ignore')
        numeric_cols = ['revenue', 'gross_profit', 'operating_income', 'net_income', 'interest_expense', 'income_before_tax', 'ebitda', 'eps', 'total_assets', 'total_liabilities', 'total_debt', 'total_equity', 'cash_and_equivalents', 'operating_cash_flow', 'capital_expenditure', 'free_cash_flow', 'dividends_paid', 'depreciation_amortization']
        for col in numeric_cols:
            if col in combined_df.columns: combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
        combined_df.sort_index(inplace=True)
        logger.debug(f"[{ticker}] Combined DataFrame shape {combined_df.shape}"); return combined_df
    except Exception as e: logger.error(f"[{ticker}] Error processing data into DataFrame: {e}", exc_info=True); return None

# --- Database Upsert for Annual Data ---
def upsert_annual_data(connection, ticker: str, year_data: pd.Series):
    """ Inserts or updates a single year's data for a ticker. """
    # (This function remains identical)
    year = year_data.name; logger.debug(f"[{ticker}-{year}] Attempting DB upsert...")
    if not connection or not connection.is_connected(): logger.error(f"[{ticker}-{year}] DB connection unavailable."); return False
    cursor = None; success = False
    try:
        cursor = connection.cursor()
        data_dict = {'ticker': ticker, 'year': int(year), 'revenue': year_data.get('revenue'), 'cost_of_revenue': None, 'gross_profit': year_data.get('gross_profit'), 'operating_income': year_data.get('operating_income'), 'interest_expense': year_data.get('interest_expense'), 'income_before_tax': year_data.get('income_before_tax'), 'net_income': year_data.get('net_income'), 'ebitda': year_data.get('ebitda'), 'eps': year_data.get('eps'), 'total_assets': year_data.get('total_assets'), 'total_liabilities': year_data.get('total_liabilities'), 'total_debt': year_data.get('total_debt'), 'total_equity': year_data.get('total_equity'), 'cash_and_equivalents': year_data.get('cash_and_equivalents'), 'operating_cash_flow': year_data.get('operating_cash_flow'), 'capital_expenditure': year_data.get('capital_expenditure'), 'free_cash_flow': year_data.get('free_cash_flow'), 'dividends_paid': year_data.get('dividends_paid'), 'depreciation_amortization': year_data.get('depreciation_amortization'), 'report_date': pd.to_datetime(year_data.get('report_date'), errors='coerce').date() if pd.notna(year_data.get('report_date')) else None, 'currency': year_data.get('currency'), 'source_api': SOURCE_API_NAME}
        for k, v in data_dict.items():
            if pd.isna(v): data_dict[k] = None
        cols = list(data_dict.keys()); placeholders = ', '.join([f'%({k})s' for k in cols])
        updates = ', '.join([f'`{k}` = VALUES(`{k}`)' for k in cols if k not in ['ticker', 'year']]) + ', updated_at = NOW()'
        sql = f"INSERT INTO {DB_TABLE_ANNUAL} (`{'`, `'.join(cols)}`) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates};"
        cursor.execute(sql, data_dict); connection.commit(); logger.debug(f"[{ticker}-{year}] DB commit ok. Rows: {cursor.rowcount}"); success = True
    except Error as e:
        logger.error(f"[{ticker}-{year}] DB error upsert: {e}")
        if connection: connection.rollback(); logger.info(f"[{ticker}-{year}] DB rolled back.")
    except Exception as e:
         logger.error(f"[{ticker}-{year}] Unexpected DB upsert error: {e}", exc_info=True)
         if connection: connection.rollback(); logger.info(f"[{ticker}-{year}] DB rolled back.")
    finally:
        if cursor: cursor.close()
        return success

# --- Check Ticker Last Update Time ---
def check_ticker_last_update_time(connection, ticker: str) -> Optional[datetime]:
    """ Checks the database for the most recent update timestamp for any year of a ticker. """
    # (This function remains identical)
    if not connection or not connection.is_connected(): logger.error(f"[{ticker}] Cannot check last update, DB invalid."); return None
    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        sql = f"SELECT MAX(updated_at) as last_update FROM {DB_TABLE_ANNUAL} WHERE ticker = %s"
        cursor.execute(sql, (ticker,)); result = cursor.fetchone()
        if result and result.get('last_update'): last_update_time = result['last_update']; logger.debug(f"[{ticker}] Found last update: {last_update_time}"); return last_update_time
        else: logger.debug(f"[{ticker}] No existing records found."); return None
    except Error as e: logger.error(f"[{ticker}] DB error checking last update: {e}"); return None
    except Exception as e: logger.error(f"[{ticker}] Unexpected error checking last update: {e}", exc_info=True); return None
    finally:
        if cursor: cursor.close()

# --- Main Execution ---
def main():
    start_time = time.time()
    logger.info("==================================================")
    logger.info(f"=== Starting Annual Stock Data Import (Source: {SOURCE_API_NAME}) ===")
    logger.info(f"Years to fetch per ticker: {YEARS_TO_FETCH}")
    logger.info(f"Data Refresh Interval: {DATA_REFRESH_INTERVAL_HOURS} hours")
    logger.info("==================================================")

    db_connection = create_db_connection()
    if not db_connection: logger.critical("Exiting: Database connection failed."); return

    processed_tickers = 0; total_years_upserted = 0; fetch_errors = 0
    db_errors = 0; connection_lost = False; skipped_fresh_count = 0
    refresh_threshold = datetime.now() - timedelta(hours=DATA_REFRESH_INTERVAL_HOURS)
    logger.info(f"Will refresh data older than: {refresh_threshold.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        stock_tickers = get_all_us_stocks()
        if not stock_tickers: logger.error("No tickers obtained. Exiting."); return

        total_tickers_to_process = len(stock_tickers)
        logger.info(f"Starting processing for {total_tickers_to_process} tickers...")

        for i, ticker in enumerate(stock_tickers):
            if (i + 1) % 50 == 0: logger.info(f"--- Progress: Processed {i + 1}/{total_tickers_to_process} tickers ---")
            logger.debug(f"--- Starting Ticker: {ticker} [{i + 1}/{total_tickers_to_process}] ---")
            processed_tickers += 1

            if connection_lost or not db_connection or not db_connection.is_connected():
                 logger.warning(f"[{ticker}] DB connection lost. Reconnecting..."); db_connection = create_db_connection();
                 if not db_connection: logger.critical("Reconnection failed. Stopping."); break
                 else: logger.info("Reconnected."); connection_lost = False

            # Check Freshness
            last_update_time = check_ticker_last_update_time(db_connection, ticker)
            skip_api_call = False
            if last_update_time is not None:
                if last_update_time > refresh_threshold:
                    skip_api_call = True; skipped_fresh_count += 1
                    logger.info(f"[{ticker}] Skipping API calls: Data fresh (Updated: {last_update_time.strftime('%Y-%m-%d %H:%M')})")
                else: logger.debug(f"[{ticker}] Data stale. Fetching update.")
            else: logger.debug(f"[{ticker}] No previous update time found. Fetching data.")

            if skip_api_call:
                time.sleep(TICKER_PROCESSING_DELAY * 0.1) # Optional small delay even when skipping
                continue # Next ticker

            # Fetch and process data
            combined_df = process_ticker(ticker)

            if combined_df is not None and not combined_df.empty:
                years_updated_count = 0
                for year_index, year_data in combined_df.iterrows():
                    if upsert_annual_data(db_connection, ticker, year_data): years_updated_count += 1
                    else: db_errors += 1; logger.error(f"[{ticker}-{year_data.name}] Failed DB upsert.")
                total_years_upserted += years_updated_count
                if years_updated_count > 0: logger.info(f"[{ticker}] Successfully processed. Upserted/Updated {years_updated_count} years.")
                else: logger.warning(f"[{ticker}] Processed but no years were updated/upserted.")

            else:
                logger.warning(f"[{ticker}] Failed to fetch or process data. Skipping DB update for this ticker.")
                fetch_errors += 1
                # Optionally upsert a minimal error record for this ticker?

            # Delay between tickers (only if API was called)
            logger.debug(f"Sleeping for {TICKER_PROCESSING_DELAY} sec...")
            time.sleep(TICKER_PROCESSING_DELAY)

    except KeyboardInterrupt: logger.warning("Keyboard interrupt received. Shutting down...")
    except Exception as e: logger.critical(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
    finally:
        if db_connection and db_connection.is_connected():
            try: db_connection.close(); logger.info("Database connection closed.")
            except Error as e: logger.error(f"Error closing database connection: {e}")

    end_time = time.time()
    logger.info("\n==================================================")
    logger.info(f"=== Annual Data Import Process Complete ===")
    logger.info(f"Total time taken: {end_time - start_time:.2f} seconds")
    logger.info(f"Tickers processed (attempted fetch or checked freshness): {processed_tickers}")
    logger.info(f"Tickers skipped (data fresh): {skipped_fresh_count}")
    logger.info(f"Tickers with fetch/processing errors: {fetch_errors}")
    logger.info(f"Total annual records upserted/updated: {total_years_upserted}")
    logger.info(f"Individual year DB upsert errors: {db_errors}")
    logger.info("==================================================")

if __name__ == "__main__":
    main()