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

# --- Configuration ---
# Analysis Period
YEARS_HISTORY = 5
# Data Source API Identifier
SOURCE_API_NAME = "FMP_Direct" # Indicate direct API usage

# --- FMP Configuration ---
FMP_BASE_URL = "https://financialmodelingprep.com/api/v3"
# !!! IMPORTANT: Use environment variables for API keys !!!
FMP_API_KEY = os.environ.get("FMP_API_KEY", "IQ7xQeQoWApWqfkuxZl88l1A22p4qLw5")
if FMP_API_KEY == "IQ7xQeQoWApWqfkuxZl88l1A22p4qLw5": logging.warning("Using hardcoded FMP API key. Use environment variables.")
elif not FMP_API_KEY: logging.critical("FMP_API_KEY not found. Exiting."); exit()

# TradingView API config
TRADINGVIEW_API_URL = "https://scanner.tradingview.com/america/scan"
TV_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Content-Type": "application/json"
}
TV_DELAY_BETWEEN_BATCHES = 1.1
TV_REQUEST_TIMEOUT = 25

# API Delays
FMP_STATEMENT_DELAY = 0.1 # Delay between IS and CF calls for one ticker
TICKER_PROCESSING_DELAY = 0.15 # Delay between different tickers

# --- Data Freshness Configuration ---
DATA_REFRESH_INTERVAL_HOURS = 24 # Refresh data older than 24 hours

# --- Database Configuration ---
DB_HOST = os.environ.get("DB_HOST", "192.168.1.142")
DB_PORT = os.environ.get("DB_PORT", 3306)
DB_NAME = os.environ.get("DB_NAME", "nextcloud")
DB_USER = os.environ.get("DB_USER", "nextcloud") # <-- REPLACE or set env var
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Ks120909090909#") # <-- REPLACE or set env var
DB_TABLE = "stock_financial_summary"

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-8s - %(message)s',
    handlers=[
        logging.FileHandler("importData_FMP_Direct.log"),
        logging.StreamHandler()
    ]
)
logging.getLogger("urllib3").setLevel(logging.WARNING) # Quieten requests library logs

# --- Database Connection ---
def create_db_connection() -> Optional[mysql.connector.MySQLConnection]:
    """Creates and returns a database connection."""
    connection = None
    logging.debug(f"Attempting DB connection to {DB_HOST}:{DB_PORT}, DB: {DB_NAME}, User: {DB_USER}")
    try:
        connection = mysql.connector.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, connection_timeout=10
        )
        if connection.is_connected(): logging.info("MariaDB connection successful")
        else: logging.error("MariaDB connection failed."); connection = None
    except Error as e: logging.error(f"Error connecting to MariaDB: {e}")
    except Exception as e: logging.error(f"Unexpected error during DB connection: {e}")
    return connection


# --- Ticker Fetching (from TradingView) ---
def get_all_us_stocks() -> List[str]:
    """Fetches a list of US stock tickers from TradingView and cleans them."""
    # (This function remains identical to the previous correct version)
    logging.debug("Entering get_all_us_stocks function.")
    all_tickers_with_exchange = []
    current_range = 0; batch_size = 1500; max_tickers_to_fetch = 30000
    logging.info("Attempting to fetch US stock list from TradingView...")
    while current_range < max_tickers_to_fetch:
        payload = {
            "filter": [{"left": "exchange", "operation": "in_range", "right": ["NYSE", "NASDAQ", "AMEX"]}, {"left": "is_primary", "operation": "equal", "right": True}, {"left": "type", "operation": "in_range", "right": ["stock", "dr"]}, {"left": "subtype", "operation": "in_range", "right": ["common", "", "preferred", "foreign-issuer", "american_depository_receipt", "reit", "trust"]}, {"left": "market_cap_basic", "operation": "greater", "right": 10000000}],
            "options": {"lang": "en"}, "markets": ["america"], "symbols": {"query": {"types": []}, "tickers": []}, "columns": ["name"], "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"}, "range": [current_range, current_range + batch_size]
        }
        logging.debug(f"Requesting TV batch range: [{current_range}, {current_range + batch_size}]")
        try:
            response = requests.post(TRADINGVIEW_API_URL, json=payload, headers=TV_HEADERS, timeout=TV_REQUEST_TIMEOUT)
            logging.debug(f"TV API Response Status Code: {response.status_code} for range {current_range}")
            if response.status_code != 200: logging.warning(f"TV API Response Text (non-200): {response.text[:500]}...")
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict) or 'data' not in data or not isinstance(data['data'], list): logging.warning(f"Unexpected TV API response structure for range {current_range}."); time.sleep(TV_DELAY_BETWEEN_BATCHES * 2); continue
            if not data['data']: logging.info(f"No more data from TradingView at range {current_range}. Total fetched: {len(all_tickers_with_exchange)}"); break
            batch_tickers = [item['d'][0] for item in data['data'] if item and 'd' in item and item['d']]
            all_tickers_with_exchange.extend(batch_tickers)
            logging.info(f"Fetched TV batch {current_range // batch_size + 1} ({len(batch_tickers)} tickers). Total: {len(all_tickers_with_exchange)}")
            if len(batch_tickers) < batch_size: logging.info(f"Last TV batch smaller than requested, assuming end of list."); break
            current_range += batch_size; logging.debug(f"Sleeping for {TV_DELAY_BETWEEN_BATCHES} sec..."); time.sleep(TV_DELAY_BETWEEN_BATCHES)
        except requests.exceptions.Timeout: logging.warning(f"Timeout fetching TV stocks batch {current_range}. Retrying..."); time.sleep(10); continue
        except requests.exceptions.RequestException as e: logging.error(f"HTTP Error fetching TV stocks batch {current_range}: {e}"); current_range += batch_size; time.sleep(5)
        except Exception as e: logging.error(f"Unexpected error processing TV stock batch {current_range}: {e}", exc_info=True); break

    if not all_tickers_with_exchange: logging.error("Could not fetch any stock tickers from TradingView."); return []
    logging.info(f"Fetched {len(all_tickers_with_exchange)} raw tickers. Starting cleaning...")
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
            else: skipped_count += 1; logging.debug(f"Skipping symbol '{symbol}' from '{original_symbol}'")
        else: skipped_count += 1
    if skipped_count > 0: logging.info(f"Skipped {skipped_count} tickers during cleaning.")
    final_list = sorted(list(cleaned_tickers))
    logging.info(f"Cleaning complete. Found {len(final_list)} unique, potentially valid symbols.")
    if not final_list: logging.warning("Cleaned ticker list is empty!")
    logging.debug("Exiting get_all_us_stocks function."); return final_list


# --- Check Ticker Freshness ---
def check_ticker_freshness(connection, ticker: str) -> Tuple[Optional[datetime], Optional[bool]]:
    """ Checks the database for a ticker's last update time and error status. """
    # (This function remains identical to the previous correct version)
    if not connection or not connection.is_connected():
        logging.error(f"[{ticker}] Cannot check freshness, DB connection invalid.")
        return None, None
    cursor = None
    try:
        cursor = connection.cursor(dictionary=True)
        sql = f"SELECT updated_at, data_fetch_error FROM {DB_TABLE} WHERE ticker = %s"
        cursor.execute(sql, (ticker,))
        result = cursor.fetchone()
        if result:
            last_updated = result.get('updated_at'); had_error = bool(result.get('data_fetch_error'))
            logging.debug(f"[{ticker}] Found DB record. Updated: {last_updated}, Error: {had_error}")
            return last_updated, had_error
        else:
            logging.debug(f"[{ticker}] No existing record found in DB.")
            return None, None
    except Error as e: logging.error(f"[{ticker}] DB error checking freshness: {e}"); return None, None
    except Exception as e: logging.error(f"[{ticker}] Unexpected error checking freshness: {e}", exc_info=True); return None, None
    finally:
        if cursor: cursor.close()


# --- Financial Data Fetching & Summary Calculation (Using requests) ---
def calculate_financial_summary(ticker: str) -> Dict[str, Any]:
    """ Fetches FMP data via direct requests, calculates summary metrics. """
    logging.debug(f"[{ticker}] --> Starting financial summary calculation using FMP Direct API...")
    summary = { # Initialize with failure state
        "ticker": ticker, "data_period_years": YEARS_HISTORY, "latest_data_year": None, "earliest_data_year": None, "source_api": SOURCE_API_NAME, "positive_ebitda_years_count": None, "positive_fcf_years_count": None, "ebitda_cagr_percent": None, "is_ebitda_turnaround": None, "ebitda_latest": None, "ebitda_earliest": None, "fcf_latest": None, "data_fetch_error": True, "last_error_message": "Process started"
    }
    fmp_limit = YEARS_HISTORY + 2; request_timeout = 20
    income_statements = None; cash_flow_statements = None
    try:
        # --- Fetch Income Statement ---
        is_url = f"{FMP_BASE_URL}/income-statement/{ticker}?period=annual&limit={fmp_limit}&apikey={FMP_API_KEY}"
        logging.debug(f"[{ticker}] Requesting FMP IS...")
        response_is = requests.get(is_url, timeout=request_timeout)
        logging.debug(f"[{ticker}] FMP IS Response Status: {response_is.status_code}")
        if response_is.status_code != 200: msg = f"FMP IS API failed: {response_is.status_code}"; logging.warning(f"[{ticker}] {msg}"); summary["last_error_message"] = msg; return summary
        try: income_statements = response_is.json()
        except json.JSONDecodeError as e: msg = f"Decode IS JSON failed: {e}"; logging.error(f"[{ticker}] {msg}"); summary["last_error_message"] = msg; return summary
        time.sleep(FMP_STATEMENT_DELAY)
        # --- Fetch Cash Flow Statement ---
        cf_url = f"{FMP_BASE_URL}/cash-flow-statement/{ticker}?period=annual&limit={fmp_limit}&apikey={FMP_API_KEY}"
        logging.debug(f"[{ticker}] Requesting FMP CF...")
        response_cf = requests.get(cf_url, timeout=request_timeout)
        logging.debug(f"[{ticker}] FMP CF Response Status: {response_cf.status_code}")
        if response_cf.status_code != 200: msg = f"FMP CF API failed: {response_cf.status_code}"; logging.warning(f"[{ticker}] {msg}"); summary["last_error_message"] = msg; return summary
        try: cash_flow_statements = response_cf.json()
        except json.JSONDecodeError as e: msg = f"Decode CF JSON failed: {e}"; logging.error(f"[{ticker}] {msg}"); summary["last_error_message"] = msg; return summary

        # --- Check fetched data structure ---
        if not isinstance(income_statements, list) or not isinstance(cash_flow_statements, list):
             fmp_error = None;
             if isinstance(income_statements, dict) and income_statements.get("Error Message"): fmp_error = income_statements["Error Message"]
             elif isinstance(cash_flow_statements, dict) and cash_flow_statements.get("Error Message"): fmp_error = cash_flow_statements["Error Message"]
             msg = f"FMP API did not return lists." + (f" Error: {fmp_error}" if fmp_error else ""); logging.warning(f"[{ticker}] {msg}"); summary["last_error_message"] = msg[:65530]; return summary
        if not income_statements or not cash_flow_statements: msg = "FMP returned empty list(s)."; logging.warning(f"[{ticker}] {msg}"); summary["last_error_message"] = msg; return summary
        logging.debug(f"[{ticker}] FMP returned {len(income_statements)} IS records and {len(cash_flow_statements)} CF records.")

        # --- Convert to DataFrames ---
        def extract_year(stmt_dict): # Corrected function definition is here
            """Helper to get year from FMP dict, preferring calendarYear."""
            yr_str = stmt_dict.get('calendarYear')
            if yr_str:
                try: return int(float(yr_str)) # Handle "YYYY.0" format
                except (ValueError, TypeError): logging.debug(f"[{ticker}] Couldn't parse calendarYear '{yr_str}', falling back to date.") ; pass
            date_str = stmt_dict.get('date', '0');
            try: year_part = date_str.split('-')[0]; return int(year_part)
            except (ValueError, TypeError, IndexError, AttributeError): logging.warning(f"Could not parse year from date: '{date_str}' in stmt: {stmt_dict}"); return 0

        try: # DataFrame creation
            is_data = [{'year': extract_year(stmt), 'ebitda': stmt.get('ebitda'), 'op_income': stmt.get('operatingIncome'), 'da': stmt.get('depreciationAndAmortization')} for stmt in income_statements if extract_year(stmt) > 0];
            if not is_data: logging.warning(f"[{ticker}] No valid IS data after parsing years."); raise ValueError("No valid IS data")
            is_df = pd.DataFrame(is_data).set_index('year').sort_index(); is_df = is_df[~is_df.index.duplicated(keep='last')]; logging.debug(f"[{ticker}] Income Statement DataFrame (Tail):\n{is_df.tail().to_string()}")
            cf_data = [{'year': extract_year(stmt), 'cfo': stmt.get('operatingCashFlow'), 'capex': stmt.get('capitalExpenditure'), 'da_cf': stmt.get('depreciationAndAmortization')} for stmt in cash_flow_statements if extract_year(stmt) > 0];
            if not cf_data: logging.warning(f"[{ticker}] No valid CF data after parsing years."); raise ValueError("No valid CF data")
            cf_df = pd.DataFrame(cf_data).set_index('year').sort_index(); cf_df = cf_df[~cf_df.index.duplicated(keep='last')]; logging.debug(f"[{ticker}] Cash Flow DataFrame (Tail):\n{cf_df.tail().to_string()}")
        except Exception as e: msg = f"Error processing/converting FMP statement data: {e}"; logging.error(f"[{ticker}] {msg}", exc_info=True); summary["last_error_message"] = msg; return summary

        # --- Combine and Calculate Metrics ---
        # ***** START OF CORRECTED BLOCK *****
        logging.debug(f"[{ticker}] Aligning data and calculating metrics...")
        ebitda_s = pd.Series(dtype=float); # Initialize empty series

        # Get EBITDA Series
        if 'ebitda' in is_df.columns:
            ebitda_direct = pd.to_numeric(is_df['ebitda'], errors='coerce')
            # Check if the direct ebitda series contains *any* valid numbers
            if not ebitda_direct.isnull().all():
                ebitda_s = ebitda_direct
                logging.debug(f"[{ticker}] Using direct EBITDA from FMP Income Statement.")
        # ***** END OF CORRECTED BLOCK *****

        # Try calculation if direct EBITDA failed or wasn't present
        if ebitda_s.empty: # Check if ebitda_s is still the initial empty series
            if 'op_income' in is_df.columns:
                op_income = pd.to_numeric(is_df['op_income'], errors='coerce')
                # Prefer D&A from IS if available, else from CF
                da_is = pd.to_numeric(is_df.get('da'), errors='coerce')
                da_cf = pd.to_numeric(cf_df.get('da_cf'), errors='coerce')
                dep_amort = da_is if da_is is not None and not da_is.isnull().all() else da_cf

                if dep_amort is not None and not dep_amort.isnull().all():
                    # Align indices before adding
                    common_idx = op_income.index.intersection(dep_amort.index)
                    # Ensure indices actually overlap
                    if not common_idx.empty:
                         ebitda_s = op_income.loc[common_idx] + dep_amort.loc[common_idx]
                         logging.debug(f"[{ticker}] Calculated EBITDA = OpIncome + D&A.")
                    else:
                         logging.warning(f"[{ticker}] Cannot calculate EBITDA: OpIncome/D&A indices do not overlap.")
                else:
                     logging.warning(f"[{ticker}] Cannot calculate EBITDA: D&A missing or invalid from both IS/CF.")
            else:
                 logging.warning(f"[{ticker}] Cannot calculate EBITDA: OpIncome missing.")

        # Get FCF Series
        fcf_s = pd.Series(dtype=float) # Initialize empty series
        if 'cfo' in cf_df.columns and 'capex' in cf_df.columns:
             cfo = pd.to_numeric(cf_df['cfo'], errors='coerce')
             capex = pd.to_numeric(cf_df['capex'], errors='coerce') # Assume negative
             if not cfo.isnull().all() and not capex.isnull().all():
                 common_idx = cfo.index.intersection(capex.index)
                 if not common_idx.empty:
                     fcf_s = cfo.loc[common_idx] + capex.loc[common_idx].fillna(0) # Add because capex negative
                     logging.debug(f"[{ticker}] Calculated FCF = CFO + CapEx.")
                 else: logging.warning(f"[{ticker}] FCF calc failed: CFO/CapEx indices mismatch.")
             else: logging.warning(f"[{ticker}] FCF calc failed: CFO or CapEx data invalid/missing.")
        else: logging.warning(f"[{ticker}] Cannot calculate FCF: CFO or CapEx columns missing.")

        # --- Final check if series are still empty ---
        if ebitda_s.empty or fcf_s.empty:
            msg = "EBITDA or FCF series is empty after extraction/calculation attempts."
            logging.warning(f"[{ticker}] {msg}")
            summary["last_error_message"] = msg
            return summary

        # --- Clean, Align, Select ---
        combined_df = pd.DataFrame({'EBITDA': ebitda_s, 'FCF': fcf_s}).sort_index()
        logging.debug(f"[{ticker}] Combined DF before dropna tail:\n{combined_df.tail(YEARS_HISTORY + 2).to_string()}")
        combined_df.dropna(inplace=True) # Drop years missing EITHER calculated/found metric
        logging.debug(f"[{ticker}] Combined DF after dropna tail:\n{combined_df.tail(YEARS_HISTORY + 2).to_string()}")

        if len(combined_df) < YEARS_HISTORY:
            msg = f"Insufficient valid data points after FMP processing & cleaning ({len(combined_df)} < {YEARS_HISTORY})."; logging.warning(f"[{ticker}] {msg}"); summary["last_error_message"] = msg; return summary
        final_df = combined_df.iloc[-YEARS_HISTORY:].copy(); logging.debug(f"[{ticker}] Final DF ({len(final_df)} years):\n{final_df.to_string()}")

        # --- Perform Calculations ---
        summary['positive_ebitda_years_count'] = int((final_df['EBITDA'] > 0).sum()); summary['positive_fcf_years_count'] = int((final_df['FCF'] > 0).sum())
        summary['latest_data_year'] = int(final_df.index[-1]); summary['earliest_data_year'] = int(final_df.index[0])
        try: # Handle potential NaN/Inf/-Inf from calculations before float conversion
            summary['ebitda_latest'] = float(final_df['EBITDA'].iloc[-1]) if pd.notna(final_df['EBITDA'].iloc[-1]) else None
            summary['ebitda_earliest'] = float(final_df['EBITDA'].iloc[0]) if pd.notna(final_df['EBITDA'].iloc[0]) else None
            summary['fcf_latest'] = float(final_df['FCF'].iloc[-1]) if pd.notna(final_df['FCF'].iloc[-1]) else None
        except IndexError:
             msg = "IndexError accessing final_df elements for latest/earliest values."
             logging.error(f"[{ticker}] {msg}", exc_info=True); summary["last_error_message"] = msg; return summary

        # Calculate CAGR
        latest_ebitda = summary['ebitda_latest']; earliest_ebitda = summary['ebitda_earliest']; num_periods = YEARS_HISTORY - 1
        if num_periods > 0:
            if earliest_ebitda is not None and latest_ebitda is not None:
                 if earliest_ebitda <= 0:
                     if latest_ebitda > 0: summary['is_ebitda_turnaround'] = True; summary['ebitda_cagr_percent'] = None; logging.debug(f"[{ticker}] EBITDA Turnaround.")
                     else: summary['is_ebitda_turnaround'] = False; summary['ebitda_cagr_percent'] = None; logging.debug(f"[{ticker}] EBITDA non-positive start/end.")
                 elif latest_ebitda <= 0: summary['is_ebitda_turnaround'] = False; summary['ebitda_cagr_percent'] = None; logging.debug(f"[{ticker}] EBITDA positive start, non-positive end.")
                 else:
                     summary['is_ebitda_turnaround'] = False
                     try:
                         if earliest_ebitda > 0:
                             base = latest_ebitda / earliest_ebitda
                             growth_rate = (base ** (1 / num_periods)) - 1 if base > 0 else None
                             summary['ebitda_cagr_percent'] = growth_rate * 100 if growth_rate is not None else None
                             if summary['ebitda_cagr_percent'] is not None: logging.debug(f"[{ticker}] Calculated EBITDA CAGR: {summary['ebitda_cagr_percent']:.2f}%")
                             else: logging.warning(f"[{ticker}] CAGR base was non-positive in positive->positive calc.")
                         else: summary['ebitda_cagr_percent'] = None; logging.warning(f"[{ticker}] Zero/Neg earliest EBITDA in pos->pos CAGR.")
                     except (ValueError, ZeroDivisionError, OverflowError) as calc_e: logging.warning(f"[{ticker}] Error calculating pos EBITDA CAGR: {calc_e}"); summary['ebitda_cagr_percent'] = None
            else: logging.debug(f"[{ticker}] Cannot calc CAGR, endpoints None."); summary['is_ebitda_turnaround'] = False; summary['ebitda_cagr_percent'] = None
        else: logging.warning(f"[{ticker}] Cannot calc CAGR, num_periods={num_periods}"); summary['is_ebitda_turnaround'] = False; summary['ebitda_cagr_percent'] = None

        summary['data_fetch_error'] = False; summary['last_error_message'] = None; logging.info(f"[{ticker}] <== Successfully calculated financial summary using FMP Direct.")
    except requests.exceptions.RequestException as req_e: msg = f"Network error: {req_e}"; logging.error(f"[{ticker}] {msg}"); summary["last_error_message"] = msg; summary["data_fetch_error"] = True
    except Exception as e: msg = f"Unexpected error: {type(e).__name__}: {e}"; logging.error(f"[{ticker}] {msg}", exc_info=True); summary["last_error_message"] = msg[:65530]; summary["data_fetch_error"] = True
    logging.debug(f"[{ticker}] <-- Exiting financial summary calculation."); return summary


# --- Database Update ---
def update_stock_summary_in_db(connection, summary_data: Dict[str, Any]):
    """Inserts or updates a ticker's summary data in the database."""
    # (This function is now correct)
    ticker = summary_data.get('ticker', 'UNKNOWN'); logging.debug(f"[{ticker}] Attempting DB update...")
    if not connection or not connection.is_connected(): logging.error(f"[{ticker}] DB connection unavailable."); return False
    cursor = None; success = False
    try:
        cursor = connection.cursor()
        sql = f"""INSERT INTO {DB_TABLE} (ticker, data_period_years, latest_data_year, earliest_data_year, source_api, positive_ebitda_years_count, positive_fcf_years_count, ebitda_cagr_percent, is_ebitda_turnaround, ebitda_latest, ebitda_earliest, fcf_latest, data_fetch_error, last_error_message, updated_at) VALUES (%(ticker)s, %(data_period_years)s, %(latest_data_year)s, %(earliest_data_year)s, %(source_api)s, %(positive_ebitda_years_count)s, %(positive_fcf_years_count)s, %(ebitda_cagr_percent)s, %(is_ebitda_turnaround)s, %(ebitda_latest)s, %(ebitda_earliest)s, %(fcf_latest)s, %(data_fetch_error)s, %(last_error_message)s, NOW()) ON DUPLICATE KEY UPDATE data_period_years = VALUES(data_period_years), latest_data_year = VALUES(latest_data_year), earliest_data_year = VALUES(earliest_data_year), source_api = VALUES(source_api), positive_ebitda_years_count = VALUES(positive_ebitda_years_count), positive_fcf_years_count = VALUES(positive_fcf_years_count), ebitda_cagr_percent = VALUES(ebitda_cagr_percent), is_ebitda_turnaround = VALUES(is_ebitda_turnaround), ebitda_latest = VALUES(ebitda_latest), ebitda_earliest = VALUES(ebitda_earliest), fcf_latest = VALUES(fcf_latest), data_fetch_error = VALUES(data_fetch_error), last_error_message = VALUES(last_error_message), updated_at = NOW();"""
        if summary_data.get("last_error_message") and len(summary_data["last_error_message"]) > 65530: summary_data["last_error_message"] = summary_data["last_error_message"][:65530] + "..."
        logging.debug(f"[{ticker}] Executing SQL for DB update...")
        cursor.execute(sql, summary_data); connection.commit()
        logging.debug(f"[{ticker}] DB commit ok. Rows: {cursor.rowcount}"); success = True
    except Error as e:
        logging.error(f"[{ticker}] DB error update/insert: {e}")
        if connection:
            connection.rollback()
            logging.info(f"[{ticker}] DB transaction rolled back due to DB Error.")
    except Exception as e:
        logging.error(f"[{ticker}] Unexpected DB update error: {e}", exc_info=True)
        if connection:
            connection.rollback()
            logging.info(f"[{ticker}] DB transaction rolled back due to unexpected error.")
    finally:
        if cursor: cursor.close()
        logging.debug(f"[{ticker}] Finished DB update attempt. Success: {success}"); return success


# --- Main Execution ---
def main():
    start_time = time.time()
    logging.info("==================================================")
    logging.info(f"=== Starting Stock Data Import Process (Source: {SOURCE_API_NAME}) ===")
    logging.info(f"Analysis Period: {YEARS_HISTORY} years")
    logging.info(f"Data Refresh Interval: {DATA_REFRESH_INTERVAL_HOURS} hours")
    logging.info("==================================================")

    db_connection = create_db_connection()
    if not db_connection: logging.critical("Exiting: Database connection failed."); return

    processed_count = 0; db_update_success_count = 0; fetch_calc_error_count = 0
    connection_lost = False; skipped_fresh_count = 0
    refresh_threshold = datetime.now() - timedelta(hours=DATA_REFRESH_INTERVAL_HOURS)
    logging.info(f"Will refresh data older than: {refresh_threshold.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        stock_tickers = get_all_us_stocks()
        if not stock_tickers: logging.error("No tickers obtained. Exiting."); return

        total_tickers = len(stock_tickers)
        logging.info(f"Starting processing for {total_tickers} tickers using {SOURCE_API_NAME} API...")

        for i, ticker in enumerate(stock_tickers):
            if connection_lost or not db_connection or not db_connection.is_connected():
                 logging.warning(f"[{ticker}] DB connection lost/invalid. Reconnecting..."); db_connection = create_db_connection();
                 if not db_connection: logging.critical("Reconnection failed. Stopping."); break
                 else: logging.info("Reconnected."); connection_lost = False

            logging.info(f"--- [{i + 1}/{total_tickers}] Processing: {ticker} ---")
            processed_count += 1

            # Check Freshness
            last_updated, had_error = check_ticker_freshness(db_connection, ticker)
            skip_api_call = False
            if last_updated is not None and not had_error:
                if last_updated > refresh_threshold:
                    skip_api_call = True; skipped_fresh_count += 1
                    logging.info(f"[{ticker}] Skipping API call: Data fresh (Updated: {last_updated.strftime('%Y-%m-%d %H:%M')})")
                else: logging.debug(f"[{ticker}] Data stale. Fetching update.")
            elif had_error: logging.debug(f"[{ticker}] Previous fetch had error. Retrying.")
            else: logging.debug(f"[{ticker}] No recent successful record. Fetching data.")

            if skip_api_call:
                time.sleep(TICKER_PROCESSING_DELAY * 0.1) # Optional small delay even when skipping
                continue # Next ticker

            # Fetch and Calculate Data
            summary = calculate_financial_summary(ticker)

            # Update Database
            if not summary.get('data_fetch_error', True):
                update_success = update_stock_summary_in_db(db_connection, summary)
                if update_success: db_update_success_count += 1
                else: connection_lost = True # Assume connection issue on failure
            else:
                fetch_calc_error_count += 1
                logging.warning(f"[{ticker}] Skipping primary DB update due to fetch/calculation error.")
                logging.debug(f"[{ticker}] Attempting to update DB with error status...")
                update_stock_summary_in_db(db_connection, summary) # Log error state

            # Rate limiting between tickers
            logging.debug(f"Sleeping for {TICKER_PROCESSING_DELAY} sec...")
            time.sleep(TICKER_PROCESSING_DELAY)

    except KeyboardInterrupt: logging.warning("Keyboard interrupt received. Shutting down...")
    except Exception as e: logging.critical(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
    finally:
        if db_connection and db_connection.is_connected():
            try: db_connection.close(); logging.info("Database connection closed.")
            except Error as e: logging.error(f"Error closing database connection: {e}")

    end_time = time.time()
    logging.info("\n==================================================")
    logging.info(f"=== Data Import Process Complete (Source: {SOURCE_API_NAME}) ===")
    logging.info(f"Total time taken: {end_time - start_time:.2f} seconds")
    logging.info(f"Tickers processed (attempted fetch or checked freshness): {processed_count}")
    logging.info(f"Tickers skipped (data fresh): {skipped_fresh_count}")
    logging.info(f"Tickers with NEW fetch/calculation errors: {fetch_calc_error_count}")
    logging.info(f"Successful NEW/Updated DB records: {db_update_success_count}")
    logging.info("==================================================")

if __name__ == "__main__":
    main()