import pandas as pd
import time
import logging
import mysql.connector
from mysql.connector import Error
from typing import List, Optional, Dict, Any, Tuple, Set
import os
import json
from decimal import Decimal, InvalidOperation
import argparse
from datetime import datetime

# --- Configuration ---
SOURCE_TABLE = "sec_numeric_data"
TARGET_TABLE = "company_financial_periods"
SOURCE_API_NAME_META = "SEC_XBRL_Processed" # Identifier for this derived data

# How many CIKs to process in one go from source table
CIK_BATCH_SIZE = 100
# How many rows (company-periods) to insert/update in DB at once
INSERT_BATCH_SIZE = 1000

# --- Tag Mapping Configuration ---
# !!! CRITICAL: This mapping needs careful refinement based on tag.txt and data exploration !!!
# Map target table column names to potential SEC XBRL tags
# Prioritize common/standard tags first in the list for each concept
TAG_MAP = {
    # Income Statement
    'revenue': ['Revenues', 'RevenueFromContractWithCustomerExcludingAssessedTax', 'SalesRevenueNet'],
    'cost_of_revenue': ['CostOfRevenue', 'CostOfGoodsAndServicesSold'],
    'gross_profit': ['GrossProfit'],
    'research_and_development_expense': ['ResearchAndDevelopmentExpense'],
    'selling_general_and_administrative_expense': ['SellingGeneralAndAdministrativeExpense'],
    'operating_income_loss': ['OperatingIncomeLoss'],
    'interest_expense': ['InterestExpense'],
    'income_tax_expense_benefit': ['IncomeTaxExpenseBenefit'],
    'net_income_loss': ['NetIncomeLoss', 'ProfitLoss'],
    'eps_basic': ['EarningsPerShareBasic'],
    'eps_diluted': ['EarningsPerShareDiluted'],
    'ebitda': ['EBITDA'], # Use direct tag if available

    # Balance Sheet
    'cash_and_cash_equivalents': ['CashAndCashEquivalentsAtCarryingValue'],
    'accounts_receivable_net_current': ['AccountsReceivableNetCurrent'],
    'inventory_net': ['InventoryNet'],
    'total_current_assets': ['AssetsCurrent'],
    'property_plant_and_equipment_net': ['PropertyPlantAndEquipmentNet'],
    'total_assets': ['Assets'],
    'accounts_payable_current': ['AccountsPayableCurrent'],
    'short_term_debt': ['ShortTermDebt', 'DebtCurrent', 'CurrentPortionOfLongTermDebt'], # Example: Needs logic to combine if split
    'total_current_liabilities': ['LiabilitiesCurrent'],
    'long_term_debt': ['LongTermDebt', 'LongTermDebtNoncurrent'],
    'total_liabilities': ['Liabilities'],
    'total_stockholders_equity': ['StockholdersEquity', 'EquityAttributableToParent'],

    # Cash Flow
    'net_cash_provided_by_used_in_operating_activities': ['NetCashProvidedByUsedInOperatingActivities', 'NetCashProvidedByUsedInOperatingActivitiesContinuingOperations'], # CFO
    'depreciation_and_amortization': ['DepreciationAndAmortization', 'DepreciationDepletionAndAmortization'], # Check IS/CF source? Use CF priority?
    'capital_expenditure': ['PaymentsToAcquirePropertyPlantAndEquipment', 'PurchaseOfPropertyPlantAndEquipment', 'PaymentsToAcquireProductiveAssets'], # CapEx
    'net_cash_provided_by_used_in_investing_activities': ['NetCashProvidedByUsedInInvestingActivities'],
    'net_cash_provided_by_used_in_financing_activities': ['NetCashProvidedByUsedInFinancingActivities'],
    'dividends_paid': ['PaymentsOfDividends', 'DividendsPaid'], # Usually negative
    'free_cash_flow': ['FreeCashFlow'], # FMP/SEC direct reported FCF
}

# --- Database Configuration --- (Same as before)
DB_HOST = os.environ.get("DB_HOST", "192.168.1.142")
DB_PORT = os.environ.get("DB_PORT", 3306)
DB_NAME = os.environ.get("DB_NAME", "nextcloud")
DB_USER = os.environ.get("DB_USER", "nextcloud")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Ks120909090909#")

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-8s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler("transformSECToPeriods.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logging.getLogger("mysql.connector").setLevel(logging.WARNING)

# --- Database Connection --- (Identical)
def create_db_connection() -> Optional[mysql.connector.MySQLConnection]:
    connection = None; logger.debug(f"Attempting DB connection...")
    try:
        connection = mysql.connector.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, connection_timeout=10)
        if connection.is_connected(): logger.info("MariaDB connection successful")
        else: logger.error("MariaDB connection failed."); connection = None
    except Error as e: logger.error(f"Error connecting to MariaDB: {e}")
    except Exception as e: logger.error(f"Unexpected error during DB connection: {e}")
    return connection

# --- Helper: Get Unique CIKs to Process ---
def get_ciks_to_process(connection) -> List[int]:
    """ Gets a list of unique CIKs from the source table. """
    logger.info(f"Fetching unique CIKs from {SOURCE_TABLE}...")
    ciks = []
    cursor = None
    try:
        cursor = connection.cursor()
        # Query distinct CIKs, maybe filter for recently updated ones?
        # For full transform, select all distinct CIKs.
        sql = f"SELECT DISTINCT cik FROM {SOURCE_TABLE} ORDER BY cik"
        cursor.execute(sql)
        results = cursor.fetchall()
        ciks = [row[0] for row in results if row[0] is not None]
        logger.info(f"Found {len(ciks)} unique CIKs to process.")
    except Error as e:
        logger.error(f"DB error fetching CIK list: {e}")
    except Exception as e:
        logger.error(f"Unexpected error fetching CIK list: {e}", exc_info=True)
    finally:
        if cursor: cursor.close()
    return ciks

# --- Helper: Fetch Data for a Batch of CIKs ---
def fetch_data_for_ciks(connection, cik_batch: List[int]) -> Optional[pd.DataFrame]:
    """ Fetches all necessary data from sec_numeric_data for a batch of CIKs. """
    if not cik_batch: return pd.DataFrame() # Return empty DataFrame if batch is empty
    logger.debug(f"Fetching data for CIK batch (size {len(cik_batch)})...")
    cursor = None
    # Flatten the list of all tags we might need
    all_tags_needed = set(tag for tag_list in TAG_MAP.values() for tag in tag_list)
    # Add calculated FCF tag if it exists from previous script runs
    all_tags_needed.add('CalculatedFreeCashFlow')

    try:
        cursor = connection.cursor(dictionary=True)
        # Using IN clause for CIKs and tags
        tag_placeholders = ', '.join(['%s'] * len(all_tags_needed))
        cik_placeholders = ', '.join(['%s'] * len(cik_batch))
        sql = f"""
            SELECT
                cik, adsh, form, period, fy, fp, -- Submission context
                ddate, qtrs, tag, version, uom, value, -- Numeric fact context
                updated_at -- To get the latest version if multiple exist for same key? No, PK handles this.
            FROM {SOURCE_TABLE}
            WHERE cik IN ({cik_placeholders})
              AND tag IN ({tag_placeholders})
              AND uom = 'USD' -- Filter UOM here
            ORDER BY cik, ddate, qtrs -- Order for predictable processing
        """
        params = tuple(cik_batch) + tuple(all_tags_needed)
        cursor.execute(sql, params)
        data = cursor.fetchall()
        logger.debug(f"Fetched {len(data)} rows for {len(cik_batch)} CIKs.")
        return pd.DataFrame(data)
    except Error as e:
        logger.error(f"DB error fetching data for CIK batch: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching data for CIK batch: {e}", exc_info=True)
        return None
    finally:
        if cursor: cursor.close()


# --- Helper: Transform Raw Data to Pivoted Format ---
def transform_data(raw_df: pd.DataFrame) -> pd.DataFrame:
    """ Pivots raw data and maps tags to structured columns. """
    if raw_df.empty: return pd.DataFrame()
    logger.info(f"Pivoting and transforming {len(raw_df)} raw data rows...")

    # Ensure 'value' is numeric, handle errors
    raw_df['value'] = pd.to_numeric(raw_df['value'], errors='coerce')
    raw_df.dropna(subset=['value'], inplace=True) # Remove rows where value became NaN

    # Create a unique period identifier (using ddate and qtrs)
    # ddate is the point-in-time date.
    # We group by cik, ddate, qtrs to get unique facts for a period end + duration
    # Pivot using these as index, tags as columns
    try:
        pivoted = raw_df.pivot_table(
            index=['cik', 'adsh', 'form', 'period', 'fy', 'fp', 'ddate', 'qtrs', 'uom'], # Group by all context columns
            columns='tag',
            values='value',
            aggfunc='last' # If duplicate tag for same period, take the last one encountered
        ).reset_index()
        logger.info(f"Pivoted data shape: {pivoted.shape}")
    except Exception as e:
         logger.error(f"Error during pivot operation: {e}", exc_info=True)
         # Log sample data causing pivot issues if possible
         logger.error(f"Sample raw data before pivot failure:\n{raw_df.head().to_string()}")
         return pd.DataFrame() # Return empty on pivot error

    # --- Map Tags to Columns ---
    transformed_df = pd.DataFrame()
    transformed_df['cik'] = pivoted['cik']
    transformed_df['period_end_date'] = pd.to_datetime(pivoted['ddate'], errors='coerce')
    transformed_df['period_duration_qtrs'] = pivoted['qtrs']
    transformed_df['fiscal_year'] = pivoted['fy']
    transformed_df['fiscal_period'] = pivoted['fp']
    transformed_df['adsh'] = pivoted['adsh']
    transformed_df['form_type'] = pivoted['form']
    transformed_df['currency'] = pivoted['uom']
    transformed_df['source_api'] = SOURCE_API_NAME_META # Mark as processed

    logger.info("Mapping XBRL tags to table columns...")
    processed_tags_count = 0
    for target_col, source_tags in TAG_MAP.items():
        found_tag = False
        for tag in source_tags:
            if tag in pivoted.columns:
                # Use the first found tag's data for the target column
                transformed_df[target_col] = pivoted[tag]
                # logger.debug(f"Mapped '{tag}' to '{target_col}'")
                found_tag = True
                processed_tags_count +=1
                break # Stop after finding the first matching tag in the priority list
        if not found_tag:
            # Create the column with NaNs if no source tag was found
            transformed_df[target_col] = pd.NA # Use pandas NA for consistency

    logger.info(f"Mapped {processed_tags_count} tag groups to columns.")

    # --- Calculate FCF ---
    logger.info("Calculating Free Cash Flow...")
    cfo_col = 'net_cash_provided_by_used_in_operating_activities' # Name after mapping
    capex_col = 'capital_expenditure' # Name after mapping

    transformed_df['calculated_fcf'] = pd.NA # Initialize column
    if cfo_col in transformed_df.columns and capex_col in transformed_df.columns:
        # Ensure columns are numeric before calculation
        cfo_vals = pd.to_numeric(transformed_df[cfo_col], errors='coerce')
        capex_vals = pd.to_numeric(transformed_df[capex_col], errors='coerce')

        # Perform calculation where both are valid numbers (assuming CapEx is negative)
        # FCF only makes sense for duration periods (qtrs=1 or qtrs=4)
        is_flow_period = transformed_df['period_duration_qtrs'].isin([1, 4])
        valid_calc = is_flow_period & cfo_vals.notna() & capex_vals.notna()

        transformed_df.loc[valid_calc, 'calculated_fcf'] = cfo_vals[valid_calc] + capex_vals[valid_calc]
        num_fcf_calcs = valid_calc.sum()
        logger.info(f"Calculated FCF for {num_fcf_calcs} periods.")
        # Verify CapEx sign convention assumption (optional - check a sample)
        # sample_capex = transformed_df.loc[valid_calc, capex_col].head()
        # logger.debug(f"Sample CapEx used for FCF calc:\n{sample_capex.to_string()}")
    else:
        logger.warning(f"Could not calculate FCF: Mapped CFO ('{cfo_col}') or CapEx ('{capex_col}') column missing.")

    # --- Final Cleanup ---
    # Drop rows where essential period info is missing
    transformed_df.dropna(subset=['cik', 'period_end_date', 'period_duration_qtrs'], inplace=True)
    # Convert Decimal columns back if needed, or handle in upsert
    # Ensure correct data types before insert if DB is strict

    logger.info(f"Transformation complete. Result shape: {transformed_df.shape}")
    return transformed_df


# --- Database Upsert for Pivoted Data ---
def upsert_period_data(connection, period_data_df: pd.DataFrame):
    """ Inserts or updates pivoted period data into the target table. """
    if period_data_df.empty:
        logger.info("No period data to upsert.")
        return 0, 0

    logger.info(f"Upserting {len(period_data_df)} company-period records...")
    cursor = None
    total_processed = 0
    total_errors = 0
    i = 0 # For batch tracking

    # Get column names from DataFrame that match the target table
    db_cols_df = pd.DataFrame(columns=period_data_df.columns) # Empty df with same columns
    # Fetch actual columns from DB table to ensure we only use existing ones
    try:
         cursor = connection.cursor()
         cursor.execute(f"DESCRIBE {TARGET_TABLE};")
         db_table_cols = {row[0] for row in cursor.fetchall()}
         logger.debug(f"Columns in target DB table '{TARGET_TABLE}': {db_table_cols}")
    except Error as e:
         logger.error(f"Could not describe target table '{TARGET_TABLE}': {e}. Cannot proceed with upsert.")
         return 0, len(period_data_df)
    finally:
        if cursor: cursor.close()

    # Filter DataFrame columns to only those existing in the DB table
    cols_to_insert = [col for col in db_cols_df.columns if col in db_table_cols]
    logger.debug(f"Columns prepared for insert/update: {cols_to_insert}")

    # Prepare data tuples, converting NaN/NaT to None
    data_tuples = []
    for idx, row in period_data_df[cols_to_insert].iterrows():
         row_values = [row[col] if not pd.isna(row[col]) else None for col in cols_to_insert]
         # Convert Decimal to string for compatibility if needed, though connector should handle
         # row_values = [str(v) if isinstance(v, Decimal) else v for v in row_values]
         data_tuples.append(tuple(row_values))

    if not data_tuples:
         logger.warning("No valid data tuples generated for upsert.")
         return 0, 0

    cursor = None # Reset cursor
    batch_cleaned = [] # For error logging
    try:
        cursor = connection.cursor(prepared=False)
        # Build SQL dynamically
        placeholders = ', '.join(['%s'] * len(cols_to_insert))
        # Exclude primary key components from update list
        pk_cols = {'cik', 'period_end_date', 'period_duration_qtrs'}
        update_cols = [f"`{col}`=VALUES(`{col}`)" for col in cols_to_insert if col not in pk_cols]
        update_clause = ', '.join(update_cols) + ', updated_at=NOW()' # Always update timestamp

        sql = f"""
            INSERT INTO {TARGET_TABLE} (`{'`, `'.join(cols_to_insert)}`)
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause};
        """
        # logger.debug(f"Upsert SQL: {sql}") # Very verbose

        for i in range(0, len(data_tuples), INSERT_BATCH_SIZE):
            batch_cleaned = data_tuples[i : i + INSERT_BATCH_SIZE] # Already cleaned
            if batch_cleaned:
                try:
                    rows_affected = cursor.executemany(sql, batch_cleaned)
                    connection.commit()
                    logger.debug(f"Committed period batch size {len(batch_cleaned)} (rows_affected={rows_affected}).")
                    total_processed += len(batch_cleaned)
                except Error as batch_e:
                    logger.error(f"DB error during period batch insert (around index {i}): {batch_e}")
                    logger.error(f"Sample failing period batch data (first 3): {batch_cleaned[:3]}")
                    connection.rollback()
                    total_errors += len(batch_cleaned)
                    logger.warning(f"Stopping upsert for this CIK batch due to DB error.")
                    break # Stop processing batches for this CIK group
                except Exception as batch_ex:
                    logger.error(f"Unexpected error during period batch insert (around index {i}): {batch_ex}", exc_info=True)
                    connection.rollback()
                    total_errors += len(batch_cleaned)
                    logger.warning(f"Stopping upsert for this CIK batch due to unexpected error.")
                    break

    except Error as e:
        logger.error(f"DB setup error before batch insert for periods: {e}")
        total_errors += len(data_tuples) # Assume all failed if setup fails
        if connection: connection.rollback()
    except Exception as e:
         logger.error(f"Unexpected error during period DB upsert preparation: {e}", exc_info=True)
         total_errors += len(data_tuples)
         if connection: connection.rollback()
    finally:
        if cursor: cursor.close()
        logger.info(f"Period data upsert finished. Processed: {total_processed}, Errors: {total_errors}")
        return total_processed, total_errors


# --- Main Execution ---
def main():
    parser = argparse.ArgumentParser(description="Transform SEC numeric data to periodic format.")
    # No directory needed if reading from DB
    # parser.add_argument("-d", "--data-dir", required=True, help="Path to the directory containing sub.txt and num.txt")
    # args = parser.parse_args()
    # data_directory = args.data_dir

    start_time = time.time()
    logger.info("==================================================")
    logger.info(f"=== Starting SEC Data Transformation to Periods ===")
    logger.info(f"Source Table: {SOURCE_TABLE}")
    logger.info(f"Target Table: {TARGET_TABLE}")
    logger.info(f"CIK Batch Size: {CIK_BATCH_SIZE}")
    logger.info("==================================================")

    db_connection = create_db_connection()
    if not db_connection: logger.critical("Exiting: Database connection failed."); return

    total_processed_periods = 0
    total_db_errors = 0
    processed_cik_count = 0

    try:
        # 1. Get list of CIKs to process
        ciks = get_ciks_to_process(db_connection)
        if not ciks: logger.warning("No CIKs found to process."); return
        total_ciks = len(ciks)
        logger.info(f"Will process data for {total_ciks} CIKs.")

        # 2. Process CIKs in batches
        for i in range(0, total_ciks, CIK_BATCH_SIZE):
            cik_batch = ciks[i : i + CIK_BATCH_SIZE]
            logger.info(f"--- Processing CIK Batch {i//CIK_BATCH_SIZE + 1}/{(total_ciks + CIK_BATCH_SIZE - 1)//CIK_BATCH_SIZE} (CIKs {cik_batch[0]}...{cik_batch[-1]}) ---")

            # Fetch raw data for this batch
            raw_data_df = fetch_data_for_ciks(db_connection, cik_batch)

            if raw_data_df is None or raw_data_df.empty:
                logger.warning(f"No data fetched for CIK batch, skipping transformation.")
                processed_cik_count += len(cik_batch) # Count as processed even if no data
                continue

            # Transform data (pivot, map tags, calculate FCF)
            period_data_df = transform_data(raw_data_df)
            del raw_data_df # Free memory

            if period_data_df is None or period_data_df.empty:
                logger.warning(f"Transformation yielded no data for CIK batch, skipping upsert.")
                processed_cik_count += len(cik_batch)
                continue

            # Upsert transformed data
            processed, errors = upsert_period_data(db_connection, period_data_df)
            total_processed_periods += processed
            total_db_errors += errors
            processed_cik_count += len(cik_batch) # Count CIKs whose data was attempted to be upserted

            # Optional delay between CIK batches
            # time.sleep(0.5)


    except KeyboardInterrupt: logger.warning("Keyboard interrupt received.")
    except Exception as e: logger.critical(f"An unexpected error occurred in the main transformation loop: {e}", exc_info=True)
    finally:
        if db_connection and db_connection.is_connected():
            try: db_connection.close(); logger.info("Database connection closed.")
            except Error as e: logger.error(f"Error closing database connection: {e}")

    end_time = time.time()
    logger.info("\n==================================================")
    logger.info(f"=== SEC Data Transformation Process Complete ===")
    logger.info(f"Total time taken: {end_time - start_time:.2f} seconds")
    logger.info(f"CIKs processed: {processed_cik_count}/{total_ciks if 'total_ciks' in locals() else 'N/A'}")
    logger.info(f"Total period records processed/upserted: {total_processed_periods}")
    logger.info(f"Total period records failed during DB upsert: {total_db_errors}")
    logger.info("==================================================")


if __name__ == "__main__":
    main()