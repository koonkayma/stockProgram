# Filename: import_simfin_financials.py

import simfin as sf
import pandas as pd
import mysql.connector
import os
import logging
import time
from dotenv import load_dotenv
from decimal import Decimal, InvalidOperation
import requests # Needed for SEC fetch and ConnectionError
import json     # Needed for SEC fetch

# --- Configuration ---
load_dotenv()
SIMFIN_API_KEY = os.getenv('SIMFIN_API_KEY')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_PORT = os.getenv('DB_PORT', 3306)

# --- Constants for SEC Fetch ---
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
# Define a User-Agent for requests to SEC
SEC_REQUEST_HEADERS = {
    'User-Agent': 'YourAppName/1.0 (YourEmail@example.com)' # Modify with your info
}

# Data variant and Market for SimFin (Same as before)
DATA_VARIANT = 'annual'
MARKET = 'us'
SIMFIN_DATA_DIR = os.path.expanduser('~/simfin_data/')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- DB Column Mapping --- (Same as before)
DB_COLUMN_MAP = {
    # ... (Keep the full map from the previous working version) ...
    'ticker': 'Ticker', 'simfin_id': 'SimFinId', 'fiscal_year': 'Fiscal Year', 'fiscal_period': 'Fiscal Period', 'report_date': 'Report Date', 'publish_date': 'Publish Date', 'currency': 'Currency',
    'revenue': 'Revenue', 'cost_of_revenue': 'Cost of Revenue', 'gross_profit': 'Gross Profit', 'research_development': 'Research & Development', 'selling_general_administrative': 'Selling, General & Administrative', 'other_operating_expenses': 'Other Operating Expenses', 'operating_expenses': 'Operating Expenses', 'operating_income_loss': 'Operating Income (Loss)', 'non_operating_income_loss': 'Non-Operating Income (Loss)', 'interest_expense_net': 'Interest Expense, Net', 'pretax_income_loss': 'Pretax Income (Loss), Adj.', 'income_tax_expense_benefit': 'Income Tax Expense (Benefit)', 'net_income_loss': 'Net Income', 'net_income_common': 'Net Income Available to Common Shareholders', 'eps_basic': 'Earnings Per Share, Basic', 'eps_diluted': 'Earnings Per Share, Diluted', 'shares_basic': 'Weighted Average Basic Shares Outstanding', 'shares_diluted': 'Weighted Average Diluted Shares Outstanding',
    'cash_and_equivalents': 'Cash & Cash Equivalents', 'short_term_investments': 'Short Term Investments', 'accounts_receivable': 'Accounts & Notes Receivable', 'inventories': 'Inventories', 'total_current_assets': 'Total Current Assets', 'property_plant_equipment_net': 'Property, Plant & Equipment, Net', 'long_term_investments': 'Long Term Investments', 'goodwill_intangible_assets': 'Goodwill and Intangible Assets', 'total_non_current_assets': 'Total Non-Current Assets', 'total_assets': 'Total Assets',
    'accounts_payable': 'Accounts Payable', 'short_term_debt': 'Short Term Debt', 'accrued_liabilities': 'Accrued Liabilities', 'deferred_revenue_current': 'Deferred Revenue', 'total_current_liabilities': 'Total Current Liabilities', 'long_term_debt': 'Long Term Debt', 'deferred_revenue_non_current': 'Deferred Revenue', 'other_non_current_liabilities': 'Other Non-Current Liabilities', 'total_non_current_liabilities': 'Total Non-Current Liabilities', 'total_liabilities': 'Total Liabilities',
    'common_stock': 'Common Stock', 'retained_earnings': 'Retained Earnings', 'accumulated_other_comprehensive_income': 'Accumulated Other Comprehensive Income (Loss)', 'total_equity': 'Total Equity', 'total_liabilities_equity': 'Total Liabilities & Equity',
    'cf_net_income': 'Net Income/Starting Line', 'depreciation_amortization': 'Depreciation & Amortization', 'stock_based_compensation': 'Stock-Based Compensation', 'cash_from_operations': 'Net Cash from Operating Activities', 'capital_expenditures': 'Change in Fixed Assets & Intangibles', 'net_change_investments': 'Net Change in Investments', 'cash_acquisitions_divestitures': 'Cash from Acquisitions & Divestitures', 'cash_from_investing': 'Net Cash from Investing Activities', 'net_change_debt': 'Net Change in Debt', 'repurchase_common_stock': 'Repurchase of Common Stock', 'issuance_common_stock': 'Issuance of Common Stock', 'dividend_payments': 'Dividend Payments', 'cash_from_financing': 'Net Cash from Financing Activities', 'effect_exchange_rate_cash': 'Effect of Foreign Exchange Rates on Cash', 'net_change_cash': 'Net Change in Cash', 'cash_begin_period': 'Cash at Beginning of Period', 'cash_end_period': 'Cash at End of Period'
}


# --- Helper Functions ---

def get_sec_tickers(url, headers):
    """Fetches company tickers from the SEC JSON file."""
    logging.info(f"Attempting to fetch ticker list from SEC: {url}")
    tickers = []
    try:
        response = requests.get(url, headers=headers, timeout=60) # Increased timeout
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        data = response.json()
        # The data is a dictionary where values contain ticker info
        for company_info in data.values():
            ticker = company_info.get('ticker')
            if ticker: # Ensure ticker exists
                tickers.append(ticker.upper()) # Standardize to uppercase

        logging.info(f"Successfully fetched {len(tickers)} tickers from SEC.")
        # Remove potential duplicates just in case
        unique_tickers = sorted(list(set(tickers)))
        logging.info(f"Found {len(unique_tickers)} unique tickers.")
        return unique_tickers

    except requests.exceptions.Timeout:
        logging.error("Request timed out while fetching SEC tickers.")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching SEC tickers: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON from SEC response: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during SEC ticker fetch: {e}", exc_info=True)
        return None

# --- (safe_decimal, calculate_fcf, insert_update_db remain the same as the previous working version) ---
def safe_decimal(value):
    if value is None or pd.isna(value): return None
    if isinstance(value, (str)) and not value.strip(): return None
    try:
        if isinstance(value, float) and (value == float('inf') or value == float('-inf')): return None
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError): return None

def calculate_fcf(cfo_val, capex_val):
    if cfo_val is None or not isinstance(cfo_val, Decimal): return None
    capex = capex_val if isinstance(capex_val, Decimal) else Decimal(0)
    try: return cfo_val + capex
    except Exception as e: logging.warning(f"Error calculating FCF (CFO: {cfo_val}, CapEx: {capex_val}): {e}"); return None

def insert_update_db(db_cursor, data_dict):
    columns = list(data_dict.keys())
    if not columns: logging.warning("Attempted insert/update with empty data."); return False
    unique_key_cols = ['simfin_id', 'fiscal_year', 'fiscal_period']
    if not all(key in data_dict and data_dict[key] is not None for key in unique_key_cols):
        logging.error(f"Missing/NULL unique keys: {data_dict.get('ticker')}, {data_dict.get('fiscal_year')}, {data_dict.get('fiscal_period')}. Skipping.")
        return False
    placeholders = ', '.join([f'%({col})s' for col in columns])
    update_clause = ', '.join([f'`{col}` = VALUES(`{col}`)' for col in columns if col not in unique_key_cols])
    escaped_columns = ', '.join([f'`{col}`' for col in columns])
    if not update_clause: update_clause = "`last_updated` = CURRENT_TIMESTAMP"
    sql = f"""INSERT INTO simfin_financial_data ({escaped_columns}) VALUES ({placeholders})
              ON DUPLICATE KEY UPDATE {update_clause}, `last_updated` = CURRENT_TIMESTAMP;"""
    try: db_cursor.execute(sql, data_dict); return True
    except mysql.connector.Error as err: logging.error(f"DB error: {data_dict.get('ticker')} {data_dict.get('fiscal_year')} {data_dict.get('fiscal_period')}: {err}"); return False
    except Exception as e: logging.error(f"Unexpected DB error: {data_dict.get('ticker')}: {e}", exc_info=True); return False


# --- Main Execution Logic ---
def main():
    logging.info("Starting SimFin data import process using 'simfin' package...")
    # --- Get Target Tickers ---
    TARGET_TICKERS = get_sec_tickers(SEC_TICKERS_URL, SEC_REQUEST_HEADERS)
    if not TARGET_TICKERS: # Check if list is None or empty
        logging.error("CRITICAL: Failed to retrieve target tickers from SEC. Exiting.")
        return
    logging.warning(f"Processing a large list of {len(TARGET_TICKERS)} tickers.")
    logging.warning("!!! This WILL likely exceed SimFin Free Tier API limits !!!")
    logging.warning("Consider filtering TARGET_TICKERS to a smaller subset for testing/free tier use.")
    # Example: Filter for testing (uncomment to use)
    # TARGET_TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'NVDA']
    # logging.info(f"Filtered to process: {TARGET_TICKERS}")


    # --- Initialization & DB Connection ---
    if not SIMFIN_API_KEY: logging.error("CRITICAL: SimFin API Key missing. Exiting."); return
    if not all([DB_HOST, DB_USER, DB_PASSWORD, DB_NAME]): logging.error("CRITICAL: DB credentials missing. Exiting."); return
    try:
        logging.info(f"Initializing SimFin package..."); sf.set_api_key(SIMFIN_API_KEY)
        if SIMFIN_DATA_DIR: os.makedirs(SIMFIN_DATA_DIR, exist_ok=True); sf.set_data_dir(SIMFIN_DATA_DIR); logging.info(f"SimFin data directory set to: {SIMFIN_DATA_DIR}")
    except Exception as e: logging.error(f"CRITICAL: Failed to initialize SimFin: {e}", exc_info=True); return
    db_connection = None; total_inserted_updated = 0; total_failed_records = 0
    try:
        logging.info(f"Connecting to database '{DB_NAME}' on {DB_HOST}:{DB_PORT}...");
        db_connection = mysql.connector.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, port=int(DB_PORT))
        cursor = db_connection.cursor(dictionary=True); logging.info("Database connection successful.")

        # --- Fetch Data (Load Full Market, then Filter) ---
        logging.info(f"Fetching ALL {DATA_VARIANT} financial data for market '{MARKET}' (will filter later)...")
        try:
            df_income_full = sf.load_income(variant=DATA_VARIANT, market=MARKET)
            df_balance_full = sf.load_balance(variant=DATA_VARIANT, market=MARKET)
            df_cashflow_full = sf.load_cashflow(variant=DATA_VARIANT, market=MARKET)
            # Load company info - filter this source directly if possible
            logging.info("Loading company info...")
            try:
                 # Try loading only target tickers - might be more efficient if supported
                 df_companies = sf.load_companies(market=MARKET, index='Ticker').loc[TARGET_TICKERS]
                 logging.info(f"Loaded company info for {len(df_companies)} target tickers.")
            except (KeyError, AttributeError, TypeError):
                 # Fallback: load all and filter
                 logging.debug("Fallback: Loading all company info and filtering.")
                 df_companies_full = sf.load_companies(market=MARKET, index='Ticker')
                 df_companies = df_companies_full.loc[df_companies_full.index.isin(TARGET_TICKERS)]
                 logging.info(f"Loaded all company info and filtered to {len(df_companies)} target tickers.")


            logging.info(f"Filtering financial data for {len(TARGET_TICKERS)} target tickers...")
            ticker_index_level = 'Ticker'
            df_income = df_income_full.loc[df_income_full.index.get_level_values(ticker_index_level).isin(TARGET_TICKERS)]
            df_balance = df_balance_full.loc[df_balance_full.index.get_level_values(ticker_index_level).isin(TARGET_TICKERS)]
            df_cashflow = df_cashflow_full.loc[df_cashflow_full.index.get_level_values(ticker_index_level).isin(TARGET_TICKERS)]
            logging.info(f"Filtering complete. Income rows: {len(df_income)}, Balance rows: {len(df_balance)}, Cashflow rows: {len(df_cashflow)}")

        except requests.exceptions.ConnectionError as e: logging.error(f"CRITICAL: Network error: {e}. Exiting."); return
        except Exception as e: logging.error(f"CRITICAL: Error fetching/filtering: {e}", exc_info=True); return

        # --- Process DataFrames ---
        if df_income.empty and df_balance.empty and df_cashflow.empty:
            logging.warning(f"All filtered financial dataframes are empty for target tickers. No data to process.")
        else:
            # --- Join using suffixes (Same logic as before) ---
            logging.info("Joining dataframes using suffixes...")
            if not df_income.empty:
                df_merged = df_income
                if not df_balance.empty: df_merged = df_merged.join(df_balance, how='outer', lsuffix='_inc', rsuffix='_bal')
                if not df_cashflow.empty:
                    if 'df_merged' in locals(): df_merged = df_merged.join(df_cashflow, how='outer', rsuffix='_cf')
                    else: df_merged = df_balance.join(df_cashflow, how='outer', lsuffix='_bal', rsuffix='_cf')
            elif not df_balance.empty:
                 df_merged = df_balance
                 if not df_cashflow.empty: df_merged = df_merged.join(df_cashflow, how='outer', lsuffix='_bal', rsuffix='_cf')
            elif not df_cashflow.empty: df_merged = df_cashflow
            else: df_merged = pd.DataFrame()

            if df_merged.empty:
                logging.warning("Merged dataframe is empty after joins.")
            else:
                logging.debug(f"Initial join complete. Shape: {df_merged.shape}. Columns: {df_merged.columns.tolist()}")

                # --- Clean up suffixes and select preferred columns (Same logic as before) ---
                final_cols = {}; processed_cols = set(); col_priority = {'_cf', '_bal', '_inc', ''}
                df_merged = df_merged.reset_index() # Reset index early to access Ticker/Date as columns

                # --- Map Metadata Columns ---
                metadata_map = {k: v for k, v in DB_COLUMN_MAP.items() if k in ['ticker', 'simfin_id', 'fiscal_year', 'fiscal_period', 'report_date', 'publish_date', 'currency', 'company_name']}
                for db_col, df_col_base in metadata_map.items():
                    found = False
                    # Handle known index/column names directly first
                    if db_col == 'ticker' and 'Ticker' in df_merged.columns: final_cols[db_col] = df_merged['Ticker']; processed_cols.add('Ticker'); found=True
                    elif db_col == 'report_date' and 'Report Date' in df_merged.columns: final_cols[db_col] = df_merged['Report Date']; processed_cols.add('Report Date'); found=True
                    # Add Fiscal Year / Period from index if they were index levels
                    elif db_col == 'fiscal_year' and 'Fiscal Year' in df_merged.columns: final_cols[db_col] = df_merged['Fiscal Year']; processed_cols.add('Fiscal Year'); found=True
                    elif db_col == 'fiscal_period' and 'Fiscal Period' in df_merged.columns: final_cols[db_col] = df_merged['Fiscal Period']; processed_cols.add('Fiscal Period'); found=True

                    if not found: # Check suffixed versions if not found directly
                        for suffix in [''] + list(col_priority):
                            potential_col_name = f"{df_col_base}{suffix}"
                            if potential_col_name in df_merged.columns and potential_col_name not in processed_cols:
                                final_cols[db_col] = df_merged[potential_col_name]; processed_cols.add(potential_col_name); found = True; break
                    # If still not found, it will be missing from final_cols dict initially

                # --- Add Company Info (if missing and available) ---
                if 'company_name' not in final_cols or final_cols.get('company_name') is None:
                    if not df_companies.empty:
                        comp_idx_name = df_companies.index.name
                        if comp_idx_name != 'Ticker': df_companies_reset = df_companies.reset_index().set_index('Ticker')
                        else: df_companies_reset = df_companies
                        if 'ticker' in final_cols: # Ensure we have ticker column to map from
                             final_cols['company_name'] = final_cols['ticker'].map(df_companies_reset.get('Company Name', pd.Series(dtype=str)))
                             # Try to add SimFinId from companies if missing
                             if 'simfin_id' not in final_cols or final_cols.get('simfin_id') is None:
                                 if 'SimFinId' in df_companies_reset.columns:
                                      final_cols['simfin_id'] = final_cols['ticker'].map(df_companies_reset.get('SimFinId', pd.Series(dtype=float))) # SimFinId might be float

                # --- Map Financial Columns ---
                financial_map = {k: v for k, v in DB_COLUMN_MAP.items() if k not in metadata_map}
                for db_col, df_col_base in financial_map.items():
                    if db_col not in final_cols: # Only process if not already mapped as metadata
                        found = False
                        for suffix in col_priority:
                            potential_col_name = f"{df_col_base}{suffix}"
                            if potential_col_name in df_merged.columns and potential_col_name not in processed_cols:
                                final_cols[db_col] = df_merged[potential_col_name]; processed_cols.add(potential_col_name); found = True; break
                        if not found and df_col_base in df_merged.columns and df_col_base not in processed_cols: # Check unsuffixed base name
                             final_cols[db_col] = df_merged[df_col_base]; processed_cols.add(df_col_base); found = True
                        if not found: final_cols[db_col] = None # Assign None if not found

                # Create final DataFrame
                df_final = pd.DataFrame(final_cols)
                # Reorder columns to match DB table approximately (optional)
                ordered_db_cols = [col for col in DB_COLUMN_MAP.keys() if col in df_final.columns] + \
                                  [col for col in df_final.columns if col not in DB_COLUMN_MAP.keys()] # Keep extra cols at end
                df_final = df_final[ordered_db_cols]

                logging.info(f"Created final DataFrame for DB insertion. Shape: {df_final.shape}")
                logging.debug(f"Final DF Columns: {df_final.columns.tolist()}")


                # --- Iterate and Insert/Update Database (Same logic as before) ---
                logging.info("Processing final data and updating database...")
                processed_tickers_count = 0 # Count tickers actually processed in loop

                for index, row in df_final.iterrows():
                    db_data = {}
                    # Convert row to dict, handle types
                    for col in df_final.columns:
                        db_col_name = col
                        if db_col_name in ['simfin_id', 'fiscal_year']:
                            try: db_data[db_col_name] = int(row[col]) if pd.notna(row[col]) else None
                            except (ValueError, TypeError): db_data[db_col_name] = None
                        elif db_col_name in ['report_date', 'publish_date']:
                            try: db_data[db_col_name] = pd.to_datetime(row[col], errors='coerce').date() if pd.notna(row[col]) else None
                            except: db_data[db_col_name] = None
                        elif db_col_name not in ['ticker', 'company_name', 'fiscal_period', 'currency']:
                            db_data[db_col_name] = safe_decimal(row[col])
                        else:
                            db_data[db_col_name] = row[col] if pd.notna(row[col]) else None

                    if db_data.get('ticker') is None: continue # Should not happen if logic is correct

                    processed_tickers_count += 1 # Count attempt

                    if db_data.get('simfin_id') is None or db_data.get('fiscal_year') is None or db_data.get('fiscal_period') is None:
                         logging.warning(f"Skipping DB insert for {db_data.get('ticker')} row {index} due to missing SimFinId/Year/Period.")
                         total_failed_records += 1
                         continue

                    # Calculate FCF
                    db_data['free_cash_flow'] = calculate_fcf(db_data.get('cash_from_operations'), db_data.get('capital_expenditures'))

                    # Insert / Update DB
                    if insert_update_db(cursor, db_data): total_inserted_updated += 1
                    else: total_failed_records += 1

                # --- Commit and Final Logs ---
                logging.info(f"Committing database changes...")
                db_connection.commit()
                logging.info(f"Database commit successful.")

        logging.info(f"--- Data Import Complete ---")
        logging.info(f"Tickers targeted: {len(TARGET_TICKERS)}")
        # Note: Processed count might be higher than unique tickers if multiple years per ticker
        logging.info(f"Total records successfully inserted/updated in DB: {total_inserted_updated}")
        logging.info(f"Total records failed during processing/DB operation: {total_failed_records}")

    except mysql.connector.Error as err:
        logging.error(f"CRITICAL: Database connection or operational error: {err}. Process halted.")
        if db_connection and db_connection.is_connected():
            try: db_connection.rollback(); logging.info("Attempted database rollback.")
            except: logging.error("Rollback attempt failed.")
    except Exception as e:
        logging.error(f"CRITICAL: An unexpected error occurred in main function: {e}", exc_info=True)
    finally:
        # Close the database connection
        if db_connection and db_connection.is_connected():
            try: cursor.close(); db_connection.close(); logging.info("Database connection closed.")
            except: logging.error("Error closing database connection.")
        else: logging.info("Database connection was not established or already closed.")

if __name__ == "__main__":
    main()