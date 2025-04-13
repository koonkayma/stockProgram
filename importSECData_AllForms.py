import requests # Not used in this version but kept from source
import pandas as pd
import time
import logging
import mysql.connector
from mysql.connector import Error
from typing import List, Optional, Dict, Any, Tuple, Set
import os
import json # Not used in this version but kept from source
from decimal import Decimal, InvalidOperation # Use Decimal for precision
import argparse # Import argparse
from datetime import datetime, timedelta # Import timedelta

# --- Configuration ---
SOURCE_API_NAME = "SEC_XBRL_Dataset"
# *** DEFINE DEFAULT LOG FILENAME ***
DEFAULT_LOG_FILENAME = "importSECData_AllForms.log"

# --- Database Configuration ---
#DB_HOST = os.environ.get("DB_HOST", "192.168.1.142")
DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_PORT = os.environ.get("DB_PORT", 3306)
DB_NAME = os.environ.get("DB_NAME", "nextcloud")
DB_USER = os.environ.get("DB_USER", "nextcloud") # <-- REPLACE or set env var
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Ks120909090909#") # <-- REPLACE or set env var
DB_TABLE = "sec_numeric_data" # Target table

# --- Data Processing Configuration ---
CHUNK_SIZE = 50000
INSERT_BATCH_SIZE = 5000
TARGET_UOM = 'USD'
MAX_DECIMAL_PLACES = 4

# --- FCF Calculation Configuration ---
CFO_TAGS: Set[str] = {
    'NetCashProvidedByUsedInOperatingActivities',
    'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations',
}
CAPEX_TAGS: Set[str] = {
    'PaymentsToAcquirePropertyPlantAndEquipment',
    'PurchaseOfPropertyPlantAndEquipment',
    'PaymentsToAcquireProductiveAssets',
}
FCF_CALCULATION_METHOD = lambda cfo, capex: cfo + capex # Assumes CapEx is negative

# --- Function to Setup Logging ---
def setup_logging(log_file_name):
    """Configures logging with the specified file name."""
    # Close existing handlers if reconfiguring
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
        handler.close()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)-8s - %(name)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file_name, mode='a'), # Use the provided name
            logging.StreamHandler()
        ]
    )
    # Optionally quiet noisy libraries
    logging.getLogger("mysql.connector").setLevel(logging.WARNING)

# Get logger instance - will be configured in main
logger = logging.getLogger(__name__)

# --- Database Connection ---
def create_db_connection() -> Optional[mysql.connector.MySQLConnection]:
    """Creates and returns a database connection."""
    connection = None; logger.debug(f"Attempting DB connection...")
    try:
        connection = mysql.connector.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, connection_timeout=10)
        if connection.is_connected(): logger.info("MariaDB connection successful")
        else: logger.error("MariaDB connection failed."); connection = None
    except Error as e: logger.error(f"Error connecting to MariaDB: {e}")
    except Exception as e: logger.error(f"Unexpected error during DB connection: {e}")
    return connection

# --- Helper Functions ---
def parse_decimal(value_str: Any) -> Optional[Decimal]:
    """Safely parse string to Decimal, handling potential errors."""
    if value_str is None: return None
    try: d = Decimal(str(value_str)); return d.quantize(Decimal('1e-' + str(MAX_DECIMAL_PLACES)))
    except (InvalidOperation, TypeError, ValueError): return None

def format_date(date_val: Any) -> Optional[str]:
    """Safely format date values to YYYY-MM-DD string."""
    if pd.isna(date_val): return None
    try:
        if isinstance(date_val, (int, float)): date_str = str(int(date_val)); return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}" if len(date_str) == 8 else None
        elif isinstance(date_val, str): return pd.to_datetime(date_val).strftime('%Y-%m-%d')
        elif isinstance(date_val, (datetime, pd.Timestamp)): return date_val.strftime('%Y-%m-%d')
    except Exception: pass
    return None

# --- Data Loading and Processing ---
def load_submissions(filepath: str) -> Optional[pd.DataFrame]:
    """Loads the sub.txt file into a pandas DataFrame."""
    logger.info(f"Loading submission data from: {filepath}")
    if not os.path.exists(filepath): logger.critical(f"Submission file not found: {filepath}"); return None
    try:
        sub_df = pd.read_csv(filepath, sep='\t', dtype={'cik': 'Int64', 'fy': 'Int64', 'adsh': str, 'form': str, 'fp': str}, parse_dates=['period', 'filed'], encoding='utf-8', low_memory=False)
        logger.info(f"Loaded {len(sub_df)} total submissions.")
        sub_df = sub_df[['adsh', 'cik', 'name', 'form', 'period', 'fy', 'fp']].copy()
        sub_df.dropna(subset=['cik'], inplace=True)
        sub_df['cik'] = sub_df['cik'].astype(int)
        logger.info(f"Keeping {len(sub_df)} submissions with valid CIKs (importing ALL form types).")
        return sub_df
    except Exception as e: logger.critical(f"Error loading submission file {filepath}: {e}", exc_info=True); return None

def process_numeric_chunk(chunk_df: pd.DataFrame, sub_map: Dict[str, Dict], connection) -> Tuple[int, int]:
    """Processes a chunk of num.txt, calculates FCF, and inserts into DB."""
    logger = logging.getLogger(__name__)
    logger.debug(f"Processing chunk of {len(chunk_df)} numeric rows...")
    insert_count = 0; error_count = 0; rows_to_insert = []

    relevant_chunk = chunk_df[chunk_df['adsh'].isin(sub_map.keys())].copy()
    relevant_chunk = relevant_chunk[
        (relevant_chunk['qtrs'].isin([0, 1, 4])) &
        (relevant_chunk['uom'] == TARGET_UOM)
    ].copy()
    logger.debug(f"Chunk filtered to {len(relevant_chunk)} relevant rows.")
    if relevant_chunk.empty: return 0, 0

    fcf_candidates = {}
    annual_rows = relevant_chunk[relevant_chunk['qtrs'] == 4]
    for row in annual_rows.itertuples(index=False):
        adsh = row.adsh; tag = row.tag; ddate_str = format_date(row.ddate)
        if not ddate_str: continue
        try:
            year = int(ddate_str[:4]); val = parse_decimal(row.value);
            if val is None: continue
            key = (adsh, year)
            if tag in CFO_TAGS: fcf_candidates.setdefault(key, {})['cfo'] = val
            elif tag in CAPEX_TAGS: fcf_candidates.setdefault(key, {})['capex'] = val
        except Exception as e: logger.warning(f"Error processing row during FCF collect: {row}. Error: {e}"); continue

    for row in relevant_chunk.itertuples(index=False):
        adsh = row.adsh; sub_info = sub_map.get(adsh);
        if not sub_info: continue
        ddate_str = format_date(row.ddate);
        if not ddate_str: continue
        try:
            value = parse_decimal(row.value);
            if value is None: continue
            row_data = { "adsh": adsh, "tag": row.tag, "version": row.version, "ddate": ddate_str, "qtrs": row.qtrs, "uom": row.uom, "value": value, "coreg": getattr(row, 'coreg', None), "footnote": getattr(row, 'footnote', None), "cik": sub_info['cik'], "form": sub_info['form'], "period": format_date(sub_info['period']), "fy": sub_info['fy'], "fp": sub_info['fp'] }
            cols_order = [ "adsh", "tag", "version", "ddate", "qtrs", "uom", "value", "coreg", "footnote", "cik", "form", "period", "fy", "fp" ]
            row_tuple = tuple(row_data.get(col) if not pd.isna(row_data.get(col)) else None for col in cols_order)
            rows_to_insert.append(row_tuple)
        except Exception as e: logger.warning(f"Error preparing row for insert: {row}. Error: {e}"); continue

    fcf_rows_to_insert = []
    for (adsh, year), values in fcf_candidates.items():
        if 'cfo' in values and 'capex' in values:
            try:
                fcf_value = FCF_CALCULATION_METHOD(values['cfo'], values['capex'])
                fcf_value_sql = fcf_value.quantize(Decimal('1e-' + str(MAX_DECIMAL_PLACES)))
                logger.info(f"[{adsh}-{year}] Calculated FCF: {values['cfo']} + {values['capex']} = {fcf_value_sql}")
                sub_info = sub_map.get(adsh);
                if sub_info:
                    accurate_ddate = f"{year}-12-31"
                    fcf_row_data = { "adsh": adsh, "tag": "CalculatedFreeCashFlow", "version": "custom/internal", "ddate": accurate_ddate, "qtrs": 4, "uom": TARGET_UOM, "value": fcf_value_sql, "coreg": None, "footnote": "Calculated as CFO + CapEx", "cik": sub_info['cik'], "form": sub_info['form'], "period": format_date(sub_info['period']), "fy": sub_info['fy'], "fp": sub_info['fp'] }
                    cols_order = [ "adsh", "tag", "version", "ddate", "qtrs", "uom", "value", "coreg", "footnote", "cik", "form", "period", "fy", "fp" ]
                    fcf_tuple = tuple(fcf_row_data.get(col) if not pd.isna(fcf_row_data.get(col)) else None for col in cols_order)
                    fcf_rows_to_insert.append(fcf_tuple)
            except Exception as e: logger.error(f"Error calculating/preparing FCF for {adsh}-{year}: {e}")

    all_rows_to_insert = rows_to_insert + fcf_rows_to_insert
    if not all_rows_to_insert: logger.debug("No rows to insert."); return 0, 0

    cursor = None; i = 0; batch_for_insert = []
    processed_rows_count = 0 # Track rows successfully submitted in batches
    try:
        cursor = connection.cursor()
        cols = [ "adsh", "tag", "version", "ddate", "qtrs", "uom", "value", "coreg", "footnote", "cik", "form", "period", "fy", "fp" ]
        sql = f"INSERT INTO {DB_TABLE} (`{'`, `'.join(cols)}`, `imported_at`) VALUES ({', '.join(['%s'] * len(cols))}, NOW()) ON DUPLICATE KEY UPDATE value = VALUES(value), footnote = VALUES(footnote), cik = VALUES(cik), form = VALUES(form), period = VALUES(period), fy = VALUES(fy), fp = VALUES(fp), updated_at = NOW();"

        for i in range(0, len(all_rows_to_insert), INSERT_BATCH_SIZE):
            batch_for_insert = all_rows_to_insert[i : i + INSERT_BATCH_SIZE]
            if batch_for_insert:
                cursor.executemany(sql, batch_for_insert)
                connection.commit()
                rows_affected = cursor.rowcount
                logger.debug(f"Committed batch starting index {i}, size {len(batch_for_insert)} (DB reported {rows_affected} affected).")
                processed_rows_count += len(batch_for_insert)
        insert_count = processed_rows_count
        logger.debug(f"Chunk processed. Submitted {insert_count} rows in batches.")
    except Error as e:
        logger.error(f"DB error during batch insert (around index {i}): {e}")
        try: failed_batch_sample = batch_for_insert[:5]; logger.error(f"Sample failing BATCH data: {failed_batch_sample}")
        except: logger.error("Sample failing batch data: (batch not available)")
        error_count = len(all_rows_to_insert) - processed_rows_count
        if connection: connection.rollback(); logger.info("DB rolled back.")
    except Exception as e:
        logger.error(f"Unexpected error during DB insert (around index {i}): {e}", exc_info=True)
        error_count = len(all_rows_to_insert) - processed_rows_count
        if connection: connection.rollback(); logger.info("DB rolled back.")
    finally:
        if cursor: cursor.close()
    # Return count of rows successfully processed in batches, and estimate of errors
    return insert_count, error_count

# --- Main Execution ---
def main():
    # 1. Setup Argument Parser
    parser = argparse.ArgumentParser(description="Import SEC Financial Statement Data from local files into MariaDB.")
    parser.add_argument(
        "-d", "--data-dir",
        required=True,
        help="Path to the directory containing sub.txt and num.txt"
        )
    # *** ADDED LOG FILE ARGUMENT ***
    parser.add_argument(
        "--log-file",
        type=str,
        default=DEFAULT_LOG_FILENAME, # Use the defined default
        help=f"Specify the path for the output log file. Default: {DEFAULT_LOG_FILENAME}"
    )
    # *** END OF ADDITION ***
    args = parser.parse_args()

    # 2. Setup Logging using the argument
    setup_logging(args.log_file)
    logger = logging.getLogger(__name__) # Re-get logger after setup

    # 3. Log startup info
    data_directory = args.data_dir
    sub_file = os.path.join(data_directory, "sub.txt")
    num_file = os.path.join(data_directory, "num.txt")

    start_time = time.time()
    logger.info("==================================================")
    logger.info(f"=== Starting SEC Data Import (ALL FORM TYPES) ===")
    logger.info(f"Source Directory: {data_directory}")
    logger.info(f"Target Table: {DB_TABLE}")
    logger.info(f"Log File: {args.log_file}") # Log the actual log file
    logger.info(f"Chunk Size: {CHUNK_SIZE}, Insert Batch Size: {INSERT_BATCH_SIZE}")
    logger.info("==================================================")

    # 4. Validate input files
    if not os.path.isdir(data_directory): logger.critical(f"Error: Data directory not found: {data_directory}"); return
    if not os.path.isfile(sub_file): logger.critical(f"Error: Submission file not found: {sub_file}"); return
    if not os.path.isfile(num_file): logger.critical(f"Error: Numeric data file not found: {num_file}"); return
    logger.info(f"Found required files in {data_directory}")

    # 5. Connect to DB
    db_connection = create_db_connection()
    if not db_connection: logger.critical("Exiting: Database connection failed."); return

    total_processed_rows = 0
    total_errors = 0 # Track estimated errors based on failed batches

    try:
        # 6. Load Submissions
        sub_df = load_submissions(sub_file)
        if sub_df is None: logger.critical("Failed to load submission data. Exiting."); return
        sub_map = sub_df.set_index('adsh').to_dict('index')
        logger.info(f"Created submission map for {len(sub_map)} filings.")
        del sub_df # Free memory

        # 7. Process Numeric Data in Chunks
        logger.info(f"Processing numeric data from {num_file} in chunks...")
        chunk_num = 0
        for chunk in pd.read_csv(
            num_file, sep='\t', chunksize=CHUNK_SIZE,
            dtype={'adsh': str, 'tag': str, 'version': str, 'coreg': str, 'footnote':str, 'uom':str},
            parse_dates=['ddate'], encoding='utf-8', on_bad_lines='warn', low_memory=False):
            chunk_num += 1
            logger.info(f"--- Processing Chunk {chunk_num} ---")
            processed_in_chunk, errors_in_chunk = process_numeric_chunk(chunk, sub_map, db_connection)
            total_processed_rows += processed_in_chunk
            total_errors += errors_in_chunk
            # Check DB connection status periodically
            if chunk_num % 10 == 0: # Check every 10 chunks
                 if not db_connection or not db_connection.is_connected():
                     logger.warning("DB connection lost during chunk processing. Reconnecting...")
                     db_connection = create_db_connection()
                     if not db_connection:
                         logger.critical("Reconnection failed. Stopping processing.")
                         break # Stop if cannot reconnect
                 else:
                     logger.debug("DB connection check passed.")

    except FileNotFoundError as fnf_e: logger.critical(f"File not found during processing: {fnf_e}")
    except pd.errors.EmptyDataError as ede: logger.error(f"Empty data error reading file: {ede}")
    except Exception as e: logger.critical(f"An unexpected error occurred during file processing: {e}", exc_info=True)
    finally:
        # 8. Close DB Connection
        if db_connection and db_connection.is_connected():
            try: db_connection.close(); logger.info("Database connection closed.")
            except Error as e: logger.error(f"Error closing database connection: {e}")

    # 9. Log Summary
    end_time = time.time()
    logger.info("\n==================================================")
    logger.info(f"=== SEC Data Import Process Complete ===")
    logger.info(f"Total time taken: {end_time - start_time:.2f} seconds")
    logger.info(f"Total rows submitted in successful batches: {total_processed_rows}")
    logger.info(f"Total rows potentially skipped due to errors: {total_errors}")
    logger.info("==================================================")

# --- Entry Point ---
if __name__ == "__main__":
    main() # Logging is configured inside main()