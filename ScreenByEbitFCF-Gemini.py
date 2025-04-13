import requests
import pandas as pd
import time
import logging
import yfinance as yf
import mysql.connector
from mysql.connector import Error
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime
import os
import json # For logging potentially complex structures

# --- Configuration ---
# Analysis Period
YEARS_HISTORY = 5 # How many years back to analyze for summaries

# Data Source API Identifier
SOURCE_API_NAME = "yfinance" # Identifier for the data source

# TradingView API for ticker list
TRADINGVIEW_API_URL = "https://scanner.tradingview.com/america/scan"
TV_HEADERS = { # Use a realistic User-Agent
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Content-Type": "application/json"
}
TV_DELAY_BETWEEN_BATCHES = 1.1 # Delay between TV API calls (seconds)
TV_REQUEST_TIMEOUT = 25 # Timeout for TV requests (seconds)

# yfinance Configuration
YF_DELAY_BETWEEN_CALLS = 0.25 # Delay between yfinance calls (seconds) to avoid rate limits

# --- Database Configuration ---
# **IMPORTANT**: Use environment variables or a secure config system in production!
DB_HOST = os.environ.get("DB_HOST", "192.168.1.142")
DB_PORT = os.environ.get("DB_PORT", 3306)
DB_NAME = os.environ.get("DB_NAME", "nextcloud")
DB_USER = os.environ.get("DB_USER", "your_db_user") # <-- REPLACE or set env var
DB_PASSWORD = os.environ.get("DB_PASSWORD", "your_db_password") # <-- REPLACE or set env var
DB_TABLE = "stock_financial_summary"

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO, # Change to DEBUG for very detailed logs
    format='%(asctime)s - %(levelname)-8s - %(message)s',
    handlers=[
        logging.FileHandler("importData.log"), # Log to a file
        logging.StreamHandler() # Also log to console
    ]
)
logging.getLogger("yfinance").setLevel(logging.WARNING) # Quieten yfinance's INFO logs if desired
logging.getLogger("urllib3").setLevel(logging.WARNING) # Quieten underlying requests library logs

# --- Database Connection ---
def create_db_connection() -> Optional[mysql.connector.MySQLConnection]:
    """Creates and returns a database connection."""
    connection = None
    logging.debug(f"Attempting DB connection to {DB_HOST}:{DB_PORT}, DB: {DB_NAME}, User: {DB_USER}")
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connection_timeout=10 # Add a connection timeout
        )
        if connection.is_connected():
            logging.info("MariaDB connection successful")
        else:
            logging.error("MariaDB connection attempt failed (connector reported not connected).")
            connection = None
    except Error as e:
        logging.error(f"Error connecting to MariaDB: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during DB connection: {e}")
    return connection

# --- Ticker Fetching (from TradingView) ---
def get_all_us_stocks() -> List[str]:
    """Fetches a list of US stock tickers from TradingView and cleans them."""
    logging.debug("Entering get_all_us_stocks function.")
    all_tickers_with_exchange = []
    current_range = 0
    batch_size = 1500 # Can try slightly larger batches
    max_tickers_to_fetch = 30000 # Safety limit

    logging.info("Attempting to fetch US stock list from TradingView...")
    while current_range < max_tickers_to_fetch:
        payload = {
            "filter": [
                {"left": "exchange", "operation": "in_range", "right": ["NYSE", "NASDAQ", "AMEX"]},
                {"left": "is_primary", "operation": "equal", "right": True},
                {"left": "type", "operation": "in_range", "right": ["stock", "dr"]},
                {"left": "subtype", "operation": "in_range", "right": ["common", "", "preferred", "foreign-issuer", "american_depository_receipt", "reit", "trust"]}, # Expanded subtypes
                 # Consider adding a volume or market cap filter here if the list is too noisy
                 {"left": "market_cap_basic", "operation": "greater", "right": 10000000} # Optional: e.g., > $10M Market Cap
            ],
            "options": {"lang": "en"}, "markets": ["america"],
            "symbols": {"query": {"types": []}, "tickers": []},
            "columns": ["name"], # 'name' includes exchange, e.g., NASDAQ:AAPL
            "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
            "range": [current_range, current_range + batch_size]
        }
        logging.debug(f"Requesting TV batch range: [{current_range}, {current_range + batch_size}]")
        try:
            response = requests.post(TRADINGVIEW_API_URL, json=payload, headers=TV_HEADERS, timeout=TV_REQUEST_TIMEOUT)
            logging.debug(f"TV API Response Status Code: {response.status_code} for range {current_range}")
            if response.status_code != 200:
                 logging.warning(f"TV API Response Text (non-200): {response.text[:500]}...")
            response.raise_for_status()
            data = response.json()

            if not isinstance(data, dict) or 'data' not in data or not isinstance(data['data'], list):
                 logging.warning(f"Unexpected API response structure for range {current_range}. Data: {str(data)[:500]}...")
                 time.sleep(TV_DELAY_BETWEEN_BATCHES * 2) # Wait longer on weird response
                 continue # Try next batch maybe? Or break? Let's try continue for now.

            if not data['data']:
                logging.info(f"No more data from TradingView at range {current_range}. Total fetched: {len(all_tickers_with_exchange)}")
                break

            batch_tickers = [item['d'][0] for item in data['data'] if item and 'd' in item and item['d']]
            all_tickers_with_exchange.extend(batch_tickers)
            logging.info(f"Fetched batch {current_range // batch_size + 1} ({len(batch_tickers)} tickers). Total: {len(all_tickers_with_exchange)}")

            if len(batch_tickers) < batch_size:
                 logging.info(f"Last TV batch smaller than requested, assuming end of list.")
                 break

            current_range += batch_size
            logging.debug(f"Sleeping for {TV_DELAY_BETWEEN_BATCHES} sec before next TV batch.")
            time.sleep(TV_DELAY_BETWEEN_BATCHES)

        except requests.exceptions.Timeout:
             logging.warning(f"Timeout fetching TV stocks batch {current_range}. Retrying after delay...")
             time.sleep(10)
             continue
        except requests.exceptions.RequestException as e:
            logging.error(f"HTTP Error fetching TV stocks batch {current_range}: {e}")
            current_range += batch_size # Skip range
            time.sleep(5)
        except Exception as e:
             logging.error(f"Unexpected error processing TV stock batch {current_range}: {e}", exc_info=True)
             break # Stop on unexpected errors

    # --- Cleaning Tickers ---
    if not all_tickers_with_exchange:
        logging.error("Could not fetch any stock tickers from TradingView.")
        return []

    logging.info(f"Fetched {len(all_tickers_with_exchange)} raw tickers. Starting cleaning...")
    cleaned_tickers = set()
    skipped_count = 0
    for ticker_raw in all_tickers_with_exchange:
        logging.debug(f"Cleaning raw ticker: '{ticker_raw}'")
        symbol = None
        if ":" in ticker_raw:
            try:
                prefix, potential_symbol = ticker_raw.split(":", 1)
                # Allow known exchange prefixes - adjust as needed
                if prefix in ["NASDAQ", "NYSE", "AMEX", "OTC", "ARCA", "BATS", "OTCBB", "PINX"]:
                     symbol = potential_symbol
                else:
                    logging.debug(f"  Unrecognized prefix '{prefix}', skipping.")
                    skipped_count += 1; continue
            except ValueError: # More than one ':'?
                 logging.debug(f"  Could not split ticker '{ticker_raw}' cleanly by ':', skipping.")
                 skipped_count += 1; continue
        else:
             symbol = ticker_raw # Assume it might be valid if no prefix

        if symbol:
             original_symbol = symbol
             symbol = symbol.replace('.', '-') # yfinance compatibility (e.g., BRK.B -> BRK-B)
             symbol = symbol.split('/')[0] # Remove suffixes like /CL, /PK
             # Basic validation: Letters, numbers, hyphen, period (yfinance sometimes uses .)
             # Make stricter if needed, but allow flexibility. Ensure not empty.
             if symbol and all(c.isalnum() or c in ['-', '.'] for c in symbol):
                 cleaned_tickers.add(symbol)
             else:
                 logging.debug(f"  Skipping symbol '{symbol}' (from '{original_symbol}') due to invalid characters or empty after cleaning.")
                 skipped_count += 1
        else:
            skipped_count += 1

    if skipped_count > 0: logging.info(f"Skipped {skipped_count} tickers during cleaning.")
    final_list = sorted(list(cleaned_tickers))
    logging.info(f"Cleaning complete. Found {len(final_list)} unique, potentially valid symbols.")
    if not final_list: logging.warning("Cleaned ticker list is empty!")
    logging.debug("Exiting get_all_us_stocks function.")
    return final_list


# --- Financial Data Fetching & Summary Calculation ---
def calculate_financial_summary(ticker: str) -> Dict[str, Any]:
    """ Fetches yfinance data, calculates summary metrics for the database table. """
    logging.debug(f"[{ticker}] --> Starting financial summary calculation...")
    # Initialize result dict matching DB structure, assuming failure initially
    summary = {
        "ticker": ticker,
        "data_period_years": YEARS_HISTORY,
        "latest_data_year": None, "earliest_data_year": None,
        "source_api": SOURCE_API_NAME,
        "positive_ebitda_years_count": None, "positive_fcf_years_count": None,
        "ebitda_cagr_percent": None, "is_ebitda_turnaround": None,
        "ebitda_latest": None, "ebitda_earliest": None, "fcf_latest": None,
        "data_fetch_error": True, "last_error_message": "Process started"
    }

    try:
        logging.debug(f"[{ticker}] Fetching yfinance data...")
        stock = yf.Ticker(ticker)
        financials = stock.financials # Annual financials
        cashflow = stock.cashflow   # Annual cash flow

        if financials.empty or cashflow.empty:
            msg = "yfinance returned empty DataFrame for financials or cashflow."
            logging.warning(f"[{ticker}] {msg}")
            summary["last_error_message"] = msg
            return summary

        logging.debug(f"[{ticker}] Raw data received. Financials years: {financials.columns.year.tolist()}, Cashflow years: {cashflow.columns.year.tolist()}")

        # --- Robust EBITDA Extraction ---
        ebitda_data = None
        ebitda_keys = [idx for idx in financials.index if 'ebitda' in idx.lower()]
        if ebitda_keys:
            ebitda_data = financials.loc[ebitda_keys[0]]
            logging.debug(f"[{ticker}] Found direct EBITDA key: '{ebitda_keys[0]}'")
        else: # Attempt calculation
            op_income_keys = [idx for idx in financials.index if 'operating income' in idx.lower()]
            da_keys = [idx for idx in financials.index + cashflow.index if 'depreciation' in idx.lower() and ('amortization' in idx.lower() or idx.lower().endswith('depreciation'))] # Check both statements
            da_key = da_keys[0] if da_keys else None
            op_income_key = op_income_keys[0] if op_income_keys else None
            if op_income_key and da_key:
                op_income = financials.loc[op_income_key].fillna(0)
                da_source = financials if da_key in financials.index else cashflow
                dep_amort = da_source.loc[da_key].fillna(0)
                common_index = op_income.index.intersection(dep_amort.index)
                if not common_index.empty:
                    ebitda_data = op_income[common_index] + dep_amort[common_index]
                    logging.debug(f"[{ticker}] Calculated EBITDA from '{op_income_key}' and '{da_key}'.")
                else: logging.warning(f"[{ticker}] EBITDA calc failed: OpIncome/DA indices mismatch.")
            else: logging.warning(f"[{ticker}] EBITDA calc failed: Missing OpIncome ('{op_income_key}') or D&A ('{da_key}').")

        # --- Robust FCF Extraction ---
        fcf_data = None
        fcf_keys = [idx for idx in cashflow.index if 'free cash flow' in idx.lower()]
        if fcf_keys:
            fcf_data = cashflow.loc[fcf_keys[0]]
            logging.debug(f"[{ticker}] Found direct FCF key: '{fcf_keys[0]}'")
        else: # Attempt calculation: CFO + CapEx (CapEx usually negative in yfinance)
            cfo_keys = [idx for idx in cashflow.index if ('operating cash flow' in idx.lower() or 'total cash from operating activities' in idx.lower())]
            capex_keys = [idx for idx in cashflow.index if 'capital expenditure' in idx.lower()]
            cfo_key = cfo_keys[0] if cfo_keys else None
            capex_key = capex_keys[0] if capex_keys else None
            if cfo_key and capex_key:
                cfo = cashflow.loc[cfo_key].fillna(0)
                capex = cashflow.loc[capex_key].fillna(0)
                common_index = cfo.index.intersection(capex.index)
                if not common_index.empty:
                    fcf_data = cfo[common_index] + capex[common_index]
                    logging.debug(f"[{ticker}] Calculated FCF from '{cfo_key}' and '{capex_key}'.")
                else: logging.warning(f"[{ticker}] FCF calc failed: CFO/CapEx indices mismatch.")
            else: logging.warning(f"[{ticker}] FCF calc failed: Missing CFO ('{cfo_key}') or CapEx ('{capex_key}').")


        if ebitda_data is None or fcf_data is None:
            msg = "Could not obtain valid data series for both EBITDA and FCF after extraction/calculation."
            logging.warning(f"[{ticker}] {msg}")
            summary["last_error_message"] = msg
            return summary

        # --- Clean, Align, Select Data ---
        logging.debug(f"[{ticker}] Cleaning and aligning EBITDA/FCF data...")
        ebitda_data = pd.to_numeric(ebitda_data, errors='coerce')
        fcf_data = pd.to_numeric(fcf_data, errors='coerce')
        combined_df = pd.DataFrame({'EBITDA': ebitda_data, 'FCF': fcf_data}).sort_index()
        logging.debug(f"[{ticker}] Combined DF before dropna:\n{combined_df.tail(YEARS_HISTORY + 2).to_string()}") # Log last few years before dropna

        combined_df.dropna(inplace=True) # Drop years missing *either* metric
        logging.debug(f"[{ticker}] Combined DF after dropna:\n{combined_df.tail(YEARS_HISTORY + 2).to_string()}")


        if len(combined_df) < YEARS_HISTORY:
            msg = f"Insufficient valid data points after cleaning ({len(combined_df)} years < {YEARS_HISTORY} required)."
            logging.warning(f"[{ticker}] {msg}")
            summary["last_error_message"] = msg
            return summary

        # Select the most recent YEARS_HISTORY data points with valid data for BOTH metrics
        final_df = combined_df.iloc[-YEARS_HISTORY:].copy()
        logging.debug(f"[{ticker}] Final DF for calculations ({len(final_df)} years):\n{final_df.to_string()}")

        # --- Perform Calculations ---
        logging.debug(f"[{ticker}] Calculating summary metrics from final DF...")
        summary['positive_ebitda_years_count'] = int((final_df['EBITDA'] > 0).sum())
        summary['positive_fcf_years_count'] = int((final_df['FCF'] > 0).sum())

        summary['latest_data_year'] = int(final_df.index[-1].year) # Last row is latest year after sort
        summary['earliest_data_year'] = int(final_df.index[0].year) # First row is earliest
        summary['ebitda_latest'] = float(final_df['EBITDA'].iloc[-1])
        summary['ebitda_earliest'] = float(final_df['EBITDA'].iloc[0])
        summary['fcf_latest'] = float(final_df['FCF'].iloc[-1])

        # Calculate CAGR for EBITDA
        latest_ebitda = summary['ebitda_latest']
        earliest_ebitda = summary['ebitda_earliest']
        num_periods = YEARS_HISTORY - 1 # Number of growth periods

        if num_periods > 0:
            if earliest_ebitda is not None and latest_ebitda is not None:
                 if earliest_ebitda <= 0:
                     if latest_ebitda > 0:
                         summary['is_ebitda_turnaround'] = True
                         summary['ebitda_cagr_percent'] = None # Infinite growth not stored as number
                         logging.debug(f"[{ticker}] EBITDA Turnaround detected.")
                     else: # Non-positive to non-positive
                         summary['is_ebitda_turnaround'] = False
                         summary['ebitda_cagr_percent'] = None
                         logging.debug(f"[{ticker}] EBITDA non-positive start and end.")
                 elif latest_ebitda <= 0: # Positive start, non-positive end
                      summary['is_ebitda_turnaround'] = False
                      # Calculate negative growth if needed, otherwise None
                      try:
                          base = latest_ebitda / earliest_ebitda # Will be <= 0
                          # CAGR formula doesn't work well here directly for negative end. Set to None.
                          summary['ebitda_cagr_percent'] = None
                          logging.debug(f"[{ticker}] EBITDA positive start, non-positive end. CAGR set to None.")
                      except ZeroDivisionError:
                           summary['ebitda_cagr_percent'] = None
                 else: # Positive to positive growth
                     summary['is_ebitda_turnaround'] = False
                     try:
                         # Ensure base is positive for fractional exponent
                         if earliest_ebitda > 0 and latest_ebitda > 0:
                              base = latest_ebitda / earliest_ebitda
                              growth_rate = (base ** (1 / num_periods)) - 1
                              summary['ebitda_cagr_percent'] = growth_rate * 100
                              logging.debug(f"[{ticker}] Calculated EBITDA CAGR: {summary['ebitda_cagr_percent']:.2f}%")
                         else: # Should not happen if logic above is correct, but safety check
                              summary['ebitda_cagr_percent'] = None
                              logging.warning(f"[{ticker}] Unexpected state in CAGR positive->positive calc.")
                     except (ValueError, ZeroDivisionError, OverflowError) as calc_e:
                          logging.warning(f"[{ticker}] Error calculating positive EBITDA CAGR: {calc_e}")
                          summary['ebitda_cagr_percent'] = None
            else:
                 logging.debug(f"[{ticker}] Cannot calculate CAGR, earliest or latest EBITDA is None.")
                 summary['is_ebitda_turnaround'] = False
                 summary['ebitda_cagr_percent'] = None
        else:
            logging.warning(f"[{ticker}] Cannot calculate CAGR, num_periods={num_periods} (YEARS_HISTORY={YEARS_HISTORY})")
            summary['is_ebitda_turnaround'] = False
            summary['ebitda_cagr_percent'] = None

        # If we reached here, processing was successful for this ticker
        summary['data_fetch_error'] = False
        summary['last_error_message'] = None
        logging.info(f"[{ticker}] <== Successfully calculated financial summary.")

    except yf.exceptions.YFinanceException as yfe: # Catch specific yfinance errors
        msg = f"yfinance exception: {yfe}"
        logging.error(f"[{ticker}] {msg}")
        summary["last_error_message"] = msg
        summary["data_fetch_error"] = True
    except Exception as e:
        msg = f"Unexpected error during financial calculation: {type(e).__name__}: {e}"
        logging.error(f"[{ticker}] {msg}", exc_info=True) # Log traceback for unexpected
        summary["last_error_message"] = msg
        summary["data_fetch_error"] = True

    logging.debug(f"[{ticker}] <-- Exiting financial summary calculation.")
    return summary


# --- Database Update ---
def update_stock_summary_in_db(connection, summary_data: Dict[str, Any]):
    """Inserts or updates a ticker's summary data in the database."""
    ticker = summary_data.get('ticker', 'UNKNOWN')
    logging.debug(f"[{ticker}] Attempting DB update...")

    if not connection or not connection.is_connected():
        logging.error(f"[{ticker}] DB connection unavailable for update.")
        # Maybe raise an exception or return a failure status? For now, just log.
        return False # Indicate failure

    cursor = None
    success = False
    try:
        cursor = connection.cursor()
        # Use INSERT ... ON DUPLICATE KEY UPDATE for atomic upsert
        sql = f"""
            INSERT INTO {DB_TABLE} (
                ticker, data_period_years, latest_data_year, earliest_data_year, source_api,
                positive_ebitda_years_count, positive_fcf_years_count,
                ebitda_cagr_percent, is_ebitda_turnaround,
                ebitda_latest, ebitda_earliest, fcf_latest,
                data_fetch_error, last_error_message, updated_at
            ) VALUES (
                %(ticker)s, %(data_period_years)s, %(latest_data_year)s, %(earliest_data_year)s, %(source_api)s,
                %(positive_ebitda_years_count)s, %(positive_fcf_years_count)s,
                %(ebitda_cagr_percent)s, %(is_ebitda_turnaround)s,
                %(ebitda_latest)s, %(ebitda_earliest)s, %(fcf_latest)s,
                %(data_fetch_error)s, %(last_error_message)s, NOW()
            )
            ON DUPLICATE KEY UPDATE
                data_period_years = VALUES(data_period_years),
                latest_data_year = VALUES(latest_data_year),
                earliest_data_year = VALUES(earliest_data_year),
                source_api = VALUES(source_api),
                positive_ebitda_years_count = VALUES(positive_ebitda_years_count),
                positive_fcf_years_count = VALUES(positive_fcf_years_count),
                ebitda_cagr_percent = VALUES(ebitda_cagr_percent),
                is_ebitda_turnaround = VALUES(is_ebitda_turnaround),
                ebitda_latest = VALUES(ebitda_latest),
                ebitda_earliest = VALUES(ebitda_earliest),
                fcf_latest = VALUES(fcf_latest),
                data_fetch_error = VALUES(data_fetch_error),
                last_error_message = VALUES(last_error_message),
                updated_at = NOW();
        """
        # Ensure boolean values are correctly formatted if needed (connector usually handles bool->0/1)
        # summary_data['is_ebitda_turnaround'] = int(summary_data['is_ebitda_turnaround']) if summary_data['is_ebitda_turnaround'] is not None else None
        # summary_data['data_fetch_error'] = int(summary_data['data_fetch_error'])

        # Truncate long error messages if necessary
        if summary_data.get("last_error_message") and len(summary_data["last_error_message"]) > 65530: # TEXT limit approx
             summary_data["last_error_message"] = summary_data["last_error_message"][:65530] + "..."

        logging.debug(f"[{ticker}] Executing SQL with data: { {k: v for k, v in summary_data.items() if k != 'last_error_message'} }") # Log data minus potentially long msg
        cursor.execute(sql, summary_data)
        connection.commit()
        logging.debug(f"[{ticker}] DB commit successful. Rows affected: {cursor.rowcount}")
        success = True

    except Error as e:
        logging.error(f"[{ticker}] Database error during update/insert: {e}")
        if connection: connection.rollback()
    except Exception as e:
         logging.error(f"[{ticker}] Unexpected error during DB update: {e}", exc_info=True)
         if connection: connection.rollback()
    finally:
        if cursor: cursor.close()
        logging.debug(f"[{ticker}] Finished DB update attempt. Success: {success}")
        return success


# --- Main Execution ---
def main():
    start_time = time.time()
    logging.info("==================================================")
    logging.info("=== Starting Stock Data Import Process ===")
    logging.info(f"Analysis Period: {YEARS_HISTORY} years")
    logging.info("==================================================")

    db_connection = create_db_connection()
    if not db_connection:
        logging.critical("Exiting: Database connection failed.")
        return

    processed_count = 0
    db_update_success_count = 0
    fetch_calc_error_count = 0
    connection_lost = False

    try:
        # 1. Get Tickers
        stock_tickers = get_all_us_stocks()
        if not stock_tickers:
            logging.error("No tickers obtained. Exiting.")
            return

        # Optional: Limit tickers for testing
        # test_tickers = ['AAPL', 'MSFT', 'GOOGL', 'NVDA', 'NONEXISTENT', 'BRK-B', 'JNJ', 'V', 'PG', 'AMD', 'TSLA', 'META']
        # logging.warning(f"--- RUNNING WITH TEST TICKER LIST: {test_tickers} ---")
        # stock_tickers = test_tickers
        # stock_tickers = stock_tickers[1000:1100] # Process a slice for testing

        total_tickers = len(stock_tickers)
        logging.info(f"Starting financial data processing for {total_tickers} tickers...")

        # 2. Process Each Ticker
        for i, ticker in enumerate(stock_tickers):
            # Check connection before each ticker (paranoid mode)
            if connection_lost or not db_connection or not db_connection.is_connected():
                 logging.warning(f"[{ticker}] DB connection lost or invalid. Attempting reconnect...")
                 db_connection = create_db_connection()
                 if not db_connection:
                      logging.critical("Reconnection failed. Stopping processing.")
                      break # Stop if reconnect fails
                 else:
                      logging.info("Successfully reconnected.")
                      connection_lost = False

            logging.info(f"--- [{i + 1}/{total_tickers}] Processing: {ticker} ---")
            processed_count += 1

            # Calculate summary metrics
            summary = calculate_financial_summary(ticker)

            # Update database ONLY if calculation didn't fail
            if not summary.get('data_fetch_error', True):
                update_success = update_stock_summary_in_db(db_connection, summary)
                if update_success:
                     db_update_success_count += 1
                else:
                     # Mark connection as potentially lost if update failed, to trigger reconnect check
                     connection_lost = True
            else:
                fetch_calc_error_count += 1
                logging.warning(f"[{ticker}] Skipping DB update due to fetch/calculation error.")
                # Still attempt to record the error state in DB if possible
                logging.debug(f"[{ticker}] Attempting to update DB with error status...")
                update_stock_summary_in_db(db_connection, summary) # Update with error flag = True


            # Rate limiting for data source API (yfinance)
            logging.debug(f"Sleeping for {YF_DELAY_BETWEEN_CALLS} sec...")
            time.sleep(YF_DELAY_BETWEEN_CALLS)

    except KeyboardInterrupt:
         logging.warning("Keyboard interrupt received. Shutting down...")
    except Exception as e:
         logging.critical(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
    finally:
        if db_connection and db_connection.is_connected():
            try: db_connection.close(); logging.info("Database connection closed.")
            except Error as e: logging.error(f"Error closing database connection: {e}")

    end_time = time.time()
    logging.info("\n==================================================")
    logging.info("=== Data Import Process Complete ===")
    logging.info(f"Total time taken: {end_time - start_time:.2f} seconds")
    logging.info(f"Tickers attempted: {processed_count}")
    logging.info(f"Tickers with fetch/calculation errors: {fetch_calc_error_count}")
    logging.info(f"Successful DB updates: {db_update_success_count}")
    logging.info("==================================================")


if __name__ == "__main__":
    main()