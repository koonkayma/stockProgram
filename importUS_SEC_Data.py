import requests
import pandas as pd
import time
import logging
import mysql.connector
from mysql.connector import Error
from typing import List, Optional, Dict, Any, Tuple, Set
import os
import json
from decimal import Decimal, InvalidOperation # Use Decimal for precision
import argparse # Import argparse
from datetime import datetime, timedelta # Import timedelta

# --- Configuration ---
SOURCE_API_NAME = "SEC_XBRL_Dataset"

# --- Database Configuration ---
DB_HOST = os.environ.get("DB_HOST", "192.168.1.142")
DB_PORT = os.environ.get("DB_PORT", 3306)
DB_NAME = os.environ.get("DB_NAME", "nextcloud")
DB_USER = os.environ.get("DB_USER", "nextcloud") # <-- REPLACE or set env var
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Ks120909090909#") # <-- REPLACE or set env var
DB_TABLE = "sec_numeric_data" # Target table

# --- Data Processing Configuration ---
# *** CHANGED CHUNK_SIZE and INSERT_BATCH_SIZE ***
CHUNK_SIZE = 50000  # Process num.txt in chunks of this many rows
INSERT_BATCH_SIZE = 5000 # Insert rows into DB in batches of this size
# TARGET_FORMS removed - processing all forms
TARGET_UOM = 'USD' # Focus on USD values primarily
MAX_DECIMAL_PLACES = 4 # For storing values

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

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-8s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler("importSECData_AllForms.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logging.getLogger("mysql.connector").setLevel(logging.WARNING)

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
    logger.debug(f"Processing chunk of {len(chunk_df)} numeric rows...")
    insert_count = 0; error_count = 0; rows_to_insert = []

    relevant_chunk = chunk_df[chunk_df['adsh'].isin(sub_map.keys())].copy()
    relevant_chunk = relevant_chunk[
        (relevant_chunk['qtrs'].isin([0, 1, 4])) &
        (relevant_chunk['uom'] == TARGET_UOM)
    ].copy()

    logger.debug(f"Chunk filtered to {len(relevant_chunk)} potentially relevant rows (adsh in sub_map, qtrs 0/1/4, USD).")
    if relevant_chunk.empty: return 0, 0

    fcf_candidates = {}
    for row in relevant_chunk.itertuples(index=False):
        val: Optional[Decimal] = None; adsh = row.adsh; tag = row.tag; qtrs = row.qtrs; ddate_str = format_date(row.ddate);
        if not ddate_str: continue
        try:
            year = int(ddate_str[:4]); val = parse_decimal(row.value);
            if val is None: continue
            key = (adsh, year);
            if qtrs == 4:
                if tag in CFO_TAGS:
                    if key not in fcf_candidates: fcf_candidates[key] = {}
                    if 'cfo' not in fcf_candidates[key]: fcf_candidates[key]['cfo'] = val; logger.debug(f"Found potential Annual CFO for {key}: {val}")
                elif tag in CAPEX_TAGS:
                    if key not in fcf_candidates: fcf_candidates[key] = {}
                    if 'capex' not in fcf_candidates[key]: fcf_candidates[key]['capex'] = val; logger.debug(f"Found potential Annual CapEx for {key}: {val}")
        except Exception as e: logger.error(f"Error processing row during FCF candidate collection: {row}. Error: {e}", exc_info=False); continue

    for row in relevant_chunk.itertuples(index=False):
        value: Optional[Decimal] = None; adsh = row.adsh; sub_info = sub_map.get(adsh);
        if not sub_info: continue
        ddate_str = format_date(row.ddate);
        if not ddate_str: continue
        try:
            year = int(ddate_str[:4]); tag = row.tag; version = row.version; qtrs = row.qtrs; uom = row.uom;
            coreg = getattr(row, 'coreg', None); footnote = getattr(row, 'footnote', None); value = parse_decimal(row.value);
            if value is None: continue
            row_data = { "adsh": adsh, "tag": tag, "version": version, "ddate": ddate_str, "qtrs": qtrs, "uom": uom, "value": value, "coreg": coreg, "footnote": footnote, "cik": sub_info['cik'], "form": sub_info['form'], "period": format_date(sub_info['period']), "fy": sub_info['fy'], "fp": sub_info['fp'] }
            cols_order = [ "adsh", "tag", "version", "ddate", "qtrs", "uom", "value", "coreg", "footnote", "cik", "form", "period", "fy", "fp" ]
            row_tuple = tuple(row_data.get(col) if not pd.isna(row_data.get(col)) else None for col in cols_order)
            rows_to_insert.append(row_tuple)
        except Exception as e: logger.error(f"Error preparing row for insertion: {row}. Error: {e}", exc_info=False); continue

    fcf_rows_to_insert = []
    for (adsh, year), values in fcf_candidates.items():
        if 'cfo' in values and 'capex' in values:
            try:
                fcf_value = FCF_CALCULATION_METHOD(values['cfo'], values['capex'])
                fcf_value_sql = fcf_value
                logger.info(f"[{adsh}-{year}] Calculated FCF: {values['cfo']} + {values['capex']} = {fcf_value_sql}")
                sub_info = sub_map.get(adsh);
                if sub_info:
                    period_date_str = format_date(sub_info['period']); period_year = int(period_date_str[:4]) if period_date_str else 0
                    accurate_ddate = period_date_str if period_year == year else f"{year}-12-31"
                    fcf_tag = "CalculatedFreeCashFlow"; fcf_version = "custom/internal";
                    fcf_row_data = { "adsh": adsh, "tag": fcf_tag, "version": fcf_version, "ddate": accurate_ddate, "qtrs": 4, "uom": TARGET_UOM, "value": fcf_value_sql, "coreg": None, "footnote": "Calculated as CFO + CapEx", "cik": sub_info['cik'], "form": sub_info['form'], "period": format_date(sub_info['period']), "fy": sub_info['fy'], "fp": sub_info['fp'] }
                    cols_order = [ "adsh", "tag", "version", "ddate", "qtrs", "uom", "value", "coreg", "footnote", "cik", "form", "period", "fy", "fp" ]
                    fcf_tuple = tuple(fcf_row_data.get(col) if not pd.isna(fcf_row_data.get(col)) else None for col in cols_order)
                    fcf_rows_to_insert.append(fcf_tuple)
                else: logger.warning(f"No sub_info for {adsh} to create FCF record for {year}")
            except Exception as e: logger.error(f"Error calculating or preparing FCF for {adsh}-{year}: {e}")

    all_rows_to_insert = rows_to_insert + fcf_rows_to_insert
    if not all_rows_to_insert: logger.debug("No rows to insert."); return 0, 0

    cursor = None; i = 0; batch_cleaned = []
    try:
        cursor = connection.cursor(prepared=False)
        cols = [ "adsh", "tag", "version", "ddate", "qtrs", "uom", "value", "coreg", "footnote", "cik", "form", "period", "fy", "fp" ]
        sql = f"INSERT INTO {DB_TABLE} (`{'`, `'.join(cols)}`) VALUES ({', '.join(['%s'] * len(cols))}) ON DUPLICATE KEY UPDATE value = VALUES(value), footnote = VALUES(footnote), cik = VALUES(cik), form = VALUES(form), period = VALUES(period), fy = VALUES(fy), fp = VALUES(fp), imported_at = NOW(), updated_at = NOW();"
        processed_rows_count = 0
        for i in range(0, len(all_rows_to_insert), INSERT_BATCH_SIZE):
            batch_raw = all_rows_to_insert[i : i + INSERT_BATCH_SIZE]
            if batch_raw:
                batch_cleaned = [tuple(None if pd.isna(v) else v for v in row) for row in batch_raw]
                cursor.executemany(sql, batch_cleaned)
                connection.commit()
                rows_affected = cursor.rowcount
                logger.debug(f"Committed batch starting index {i}, size {len(batch_cleaned)} (rows_affected={rows_affected}).")
                processed_rows_count += len(batch_cleaned)
        insert_count = processed_rows_count
        logger.debug(f"Chunk processed. Submitted {insert_count} rows in batches.")
    except Error as e:
        logger.error(f"DB error during batch insert (around index {i}): {e}")
        try: failed_batch_sample = batch_cleaned[:5]; logger.error(f"Sample failing BATCH data: {failed_batch_sample}")
        except: logger.error("Sample failing batch data: (batch not available)")
        error_count = len(all_rows_to_insert)
        if connection: connection.rollback(); logger.info("DB rolled back.")
    except Exception as e:
        logger.error(f"Unexpected error during DB insert (around index {i}): {e}", exc_info=True)
        error_count = len(all_rows_to_insert)
        if connection: connection.rollback(); logger.info("DB rolled back.")
    finally:
        if cursor: cursor.close()
    return insert_count, error_count


# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="Import SEC Financial Statement Data into MariaDB.")
    parser.add_argument("-d", "--data-dir", required=True, help="Path to the directory containing sub.txt and num.txt")
    args = parser.parse_args()
    data_directory = args.data_dir

    sub_file = os.path.join(data_directory, "sub.txt")
    num_file = os.path.join(data_directory, "num.txt")

    start_time = time.time()
    logger.info("==================================================")
    logger.info(f"=== Starting SEC Data Import (ALL FORM TYPES) ===")
    logger.info(f"Source Directory: {data_directory}")
    logger.info(f"Target Table: {DB_TABLE}")
    logger.info(f"Chunk Size: {CHUNK_SIZE}, Insert Batch Size: {INSERT_BATCH_SIZE}") # Log sizes
    logger.info("==================================================")

    db_connection = create_db_connection()
    if not db_connection: logger.critical("Exiting: Database connection failed."); return

    total_processed_rows = 0
    total_errors = 0

    try:
        sub_df = load_submissions(sub_file)
        if sub_df is None: logger.critical("Failed to load submission data. Exiting."); return
        sub_map = sub_df.set_index('adsh').to_dict('index')
        logger.info(f"Created submission map for {len(sub_map)} filings (all forms).")
        del sub_df # Free memory

        logger.info(f"Processing numeric data from {num_file} in chunks of {CHUNK_SIZE}...")
        if not os.path.exists(num_file): logger.critical(f"Numeric data file not found: {num_file}. Exiting."); return

        num_iterator = pd.read_csv(
            num_file, sep='\t', chunksize=CHUNK_SIZE,
            dtype={'adsh': str, 'tag': str, 'version': str, 'coreg': str, 'footnote':str, 'uom':str},
            parse_dates=['ddate'], encoding='utf-8', on_bad_lines='warn', low_memory=False
        )

        chunk_num = 0
        for chunk in num_iterator:
            chunk_num += 1
            logger.info(f"--- Processing Chunk {chunk_num} ---")
            processed_in_chunk, errors_in_chunk = process_numeric_chunk(chunk, sub_map, db_connection)
            total_processed_rows += processed_in_chunk
            total_errors += errors_in_chunk
            # time.sleep(0.05) # Optional delay

    except Exception as e: logger.critical(f"An unexpected error occurred during file processing: {e}", exc_info=True)
    finally:
        if db_connection and db_connection.is_connected():
            try: db_connection.close(); logger.info("Database connection closed.")
            except Error as e: logger.error(f"Error closing database connection: {e}")

    end_time = time.time()
    logger.info("\n==================================================")
    logger.info(f"=== SEC Data Import Process Complete ===")
    logger.info(f"Total time taken: {end_time - start_time:.2f} seconds")
    logger.info(f"Total rows processed in successful batches: {total_processed_rows}")
    logger.info(f"Total rows potentially skipped due to errors (processing or DB): {total_errors}")
    logger.info("==================================================")

if __name__ == "__main__":
    main()