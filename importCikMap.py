import requests
import pandas as pd
import time
import logging
# import mysql.connector # REMOVE
# from mysql.connector import Error # REMOVE
# from mysql.connector.constants import ClientFlag # REMOVE
import pymysql # ***** ADD *****
from pymysql import Error as PyMySQLError # ***** ADD ***** Alias Error
from typing import List, Optional, Dict, Any, Tuple
import os
import json
from datetime import datetime

# --- Configuration ---
SOURCE_API_NAME = "SEC_CIK_Ticker_Map"
SEC_CIK_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
REQUEST_TIMEOUT = 45
INSERT_BATCH_SIZE = 5000

# --- Database Configuration ---
DB_HOST = os.environ.get("DB_HOST", "192.168.1.142")
DB_PORT = os.environ.get("DB_PORT", 3306)
DB_NAME = os.environ.get("DB_NAME", "nextcloud")
DB_USER = os.environ.get("DB_USER", "nextcloud")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Ks120909090909#")
DB_TABLE = "company_cik_map"

# Define maximum lengths based on table schema
MAX_TICKER_LEN = 20
MAX_COMPANY_NAME_LEN = 255
MAX_SOURCE_URL_LEN = 512

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-8s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler("importCikMap.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logging.getLogger("urllib3").setLevel(logging.WARNING)
# logging.getLogger("pymysql").setLevel(logging.WARNING) # Optional: Quieten pymysql logs

# --- Database Connection ---
def create_db_connection() -> Optional[pymysql.connections.Connection]: # Type hint updated
    """Creates and returns a database connection using PyMySQL."""
    connection = None; logger.debug(f"Attempting DB connection using PyMySQL...")
    try:
        # ***** USE pymysql.connect *****
        connection = pymysql.connect(
            host=DB_HOST,
            port=int(DB_PORT), # PyMySQL usually expects int for port
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=10,
            charset='utf8mb4', # Good practice for character sets
            cursorclass=pymysql.cursors.Cursor # Use standard cursor for inserts
            # cursorclass=pymysql.cursors.DictCursor # Use DictCursor if fetching data later
        )
        # PyMySQL doesn't have is_connected(), connection object is None on failure
        logger.info("PyMySQL connection successful")
    # ***** USE PyMySQLError *****
    except PyMySQLError as e:
        logger.error(f"Error connecting to MariaDB via PyMySQL: {e}")
        connection = None # Ensure connection is None on error
    except Exception as e:
        logger.error(f"Unexpected error during DB connection: {e}")
        connection = None
    return connection

# --- Download and Process CIK Map ---
def download_and_process_cik_map(url: str) -> Optional[List[Dict]]:
    """Downloads SEC ticker map JSON and processes into a list of dicts."""
    # (Function remains identical to previous version with length checks)
    logger.info(f"Attempting to download CIK-Ticker map from {url}...")
    try:
        headers = {'User-Agent': 'YourCompanyName/AppContact YourEmail@example.com'}
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status(); logger.info("Download successful. Processing JSON...")
        data = response.json(); processed_data = []; skipped_malformed = 0; processed_count = 0
        for key, value in data.items():
            if isinstance(value, dict) and 'cik_str' in value and 'ticker' in value and 'title' in value:
                 try:
                     cik_int = int(value['cik_str']); ticker = str(value['ticker']).strip().upper() if value['ticker'] else None
                     if ticker and len(ticker) > MAX_TICKER_LEN: logger.warning(f"Truncating ticker '{ticker}' for CIK {cik_int}"); ticker = ticker[:MAX_TICKER_LEN]
                     company_name = str(value['title']).strip() if value['title'] else None
                     if company_name and len(company_name) > MAX_COMPANY_NAME_LEN: logger.warning(f"Truncating company_name '{company_name[:50]}...' for CIK {cik_int}"); company_name = company_name[:MAX_COMPANY_NAME_LEN]
                     source_url = url;
                     if len(source_url) > MAX_SOURCE_URL_LEN: source_url = source_url[:MAX_SOURCE_URL_LEN]
                     if not ticker or not company_name: logger.debug(f"Skipping entry after clean/truncate: CIK {cik_int}"); skipped_malformed += 1; continue
                     processed_data.append({'cik': cik_int, 'ticker': ticker, 'company_name': company_name, 'source_url': source_url, 'downloaded_at': datetime.now()})
                     processed_count += 1
                 except (ValueError, TypeError) as e: logger.warning(f"Skipping entry data conversion error: Key {key}, Value {value}, Error: {e}"); skipped_malformed += 1
                 except Exception as e: logger.error(f"Unexpected error processing entry: Key {key}, Value {value}, Error: {e}", exc_info=False); skipped_malformed += 1
            else: logger.debug(f"Skipping malformed entry structure: Key {key}, Value {value}"); skipped_malformed += 1
        if not processed_data: logger.error("No valid data processed from JSON."); return None
        logger.info(f"Processed {processed_count} valid CIK-Ticker mappings.");
        if skipped_malformed > 0: logger.warning(f"Skipped {skipped_malformed} malformed/incomplete entries.")
        return processed_data
    except requests.exceptions.Timeout: logger.error(f"Timeout occurred while downloading CIK map from {url}"); return None
    except requests.exceptions.RequestException as e: logger.error(f"Failed to download CIK map: {e}"); return None
    except json.JSONDecodeError as e: logger.error(f"Failed to decode JSON from CIK map URL: {e}. Response text: {response.text[:200]}"); return None
    except Exception as e: logger.error(f"An unexpected error occurred downloading/processing CIK map: {e}", exc_info=True); return None

# --- Database Upsert for CIK Map ---
def upsert_cik_data(connection, cik_data_list: List[Dict]):
    """ Inserts or updates CIK mapping data in the database using batches. """
    # ***** Uses PyMySQL connection and Error *****
    if not connection: # PyMySQL connection object itself can be None
        logger.error("DB connection unavailable for CIK map upsert.")
        return 0, len(cik_data_list)

    cursor = None
    total_processed = 0
    total_errors = 0
    current_batch_start_index = 0

    try:
        # ***** Get cursor from PyMySQL connection *****
        cursor = connection.cursor()

        # SQL remains the same, PyMySQL uses %s placeholders too
        cols = ['cik', 'ticker', 'company_name', 'source_url', 'downloaded_at']
        sql = f"""
            INSERT INTO {DB_TABLE} (`{'`, `'.join(cols)}`)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                ticker = VALUES(ticker),
                company_name = VALUES(company_name),
                source_url = VALUES(source_url),
                downloaded_at = VALUES(downloaded_at),
                imported_at = NOW();
        """

        data_tuples = []
        for item in cik_data_list:
             # Ensure datetime is formatted correctly if needed, though PyMySQL often handles it
             download_time = item.get('downloaded_at')
             # formatted_time = download_time.strftime('%Y-%m-%d %H:%M:%S') if isinstance(download_time, datetime) else None
             formatted_time = download_time # Pass datetime object directly

             data_tuples.append((
                 item.get('cik'), item.get('ticker'), item.get('company_name'),
                 item.get('source_url'), formatted_time
             ))

        for i in range(0, len(data_tuples), INSERT_BATCH_SIZE):
            current_batch_start_index = i
            batch = data_tuples[i : i + INSERT_BATCH_SIZE]
            if batch:
                try:
                    # executemany returns the number of affected rows in PyMySQL
                    rows_affected = cursor.executemany(sql, batch)
                    connection.commit()
                    logger.debug(f"Committed batch of {len(batch)} CIK records (cursor affected rows={rows_affected}).")
                    total_processed += len(batch) # Count rows attempted in successful batch
                # ***** Use PyMySQLError *****
                except PyMySQLError as batch_e:
                    logger.error(f"DB error during CIK batch insert (around index {i}): {batch_e}")
                    logger.error(f"Sample failing CIK batch data (first 5): {batch[:5]}")
                    try: connection.rollback()
                    except Exception as rb_e: logger.error(f"Rollback failed: {rb_e}")
                    total_errors += len(batch)
                    logger.warning(f"Skipping rest of CIK import due to batch error.")
                    break
                except Exception as batch_ex:
                    logger.error(f"Unexpected error during CIK batch insert (around index {i}): {batch_ex}", exc_info=True)
                    try: connection.rollback()
                    except Exception as rb_e: logger.error(f"Rollback failed: {rb_e}")
                    total_errors += len(batch)
                    logger.warning(f"Skipping rest of CIK import due to unexpected batch error.")
                    break

    # ***** Use PyMySQLError *****
    except PyMySQLError as e:
        logger.error(f"DB setup error before batch insert for CIKs: {e}")
        total_errors = len(cik_data_list)
        # No rollback needed here as transaction likely didn't start
    except Exception as e:
         logger.error(f"Unexpected error during CIK DB upsert preparation: {e}", exc_info=True)
         total_errors = len(cik_data_list)
    finally:
        if cursor: cursor.close()
        logger.info(f"CIK map upsert finished. Processed batches: {total_processed}, Errors: {total_errors}")
        return total_processed, total_errors


# --- Main Execution ---
def main():
    start_time = time.time()
    logger.info("==================================================")
    logger.info(f"=== Starting SEC CIK-Ticker Map Import ===")
    logger.info(f"Target Table: {DB_TABLE}")
    logger.info("==================================================")

    db_connection = create_db_connection()
    if not db_connection: logger.critical("Exiting: Database connection failed."); return

    total_processed_count = 0; total_error_count = 0; cik_list = None

    try:
        cik_list = download_and_process_cik_map(SEC_CIK_MAP_URL)
        if cik_list:
            logger.info(f"Attempting to upsert {len(cik_list)} CIK records into the database...")
            processed, errors = upsert_cik_data(db_connection, cik_list)
            total_processed_count = processed; total_error_count = errors
        else: logger.error("No CIK data processed from download.")
    except KeyboardInterrupt: logger.warning("Keyboard interrupt received.")
    except Exception as e: logger.critical(f"An unexpected error occurred in main process: {e}", exc_info=True)
    finally:
        # ***** Use PyMySQL connection close *****
        if db_connection:
            try: db_connection.close(); logger.info("Database connection closed.")
            # PyMySQLError might be raised on close if connection broken
            except PyMySQLError as e: logger.error(f"Error closing database connection: {e}")
            except Exception as e: logger.error(f"Unexpected error closing connection: {e}")

    end_time = time.time()
    logger.info("\n==================================================")
    logger.info(f"=== CIK Map Import Process Complete ===")
    logger.info(f"Total time taken: {end_time - start_time:.2f} seconds")
    data_processed_from_source = len(cik_list) if cik_list else 0
    logger.info(f"Total CIK records processed from source file: {data_processed_from_source}")
    logger.info(f"CIK records successfully processed in DB batches: {total_processed_count}")
    logger.info(f"CIK records failed during DB batch processing: {total_error_count}")
    logger.info(f"CIK records skipped/malformed before DB: {data_processed_from_source - total_processed_count - total_error_count}")
    logger.info("==================================================")

if __name__ == "__main__":
    main()