import pandas as pd
import numpy as np # For CAGR calculation handling
import mysql.connector
from mysql.connector import Error
import logging
import os
from datetime import datetime

# --- Configuration ---
SCREENING_YEARS = 5 # Number of years to look back for screening criteria
MIN_DATA_YEARS_REQUIRED = 5 # Must have at least this many years of data for calcs

# Screening Criteria
MIN_POSITIVE_EBITDA_YEARS = 4 # <--- Changed from 3 to 4 per request
MIN_POSITIVE_FCF_YEARS = 3
MIN_EBITDA_GROWTH_PERCENT = 15.0 # CAGR

# --- Database Configuration ---
DB_HOST = os.environ.get("DB_HOST", "192.168.1.142")
DB_PORT = os.environ.get("DB_PORT", 3306)
DB_NAME = os.environ.get("DB_NAME", "nextcloud")
DB_USER = os.environ.get("DB_USER", "your_db_user") # <-- REPLACE or set env var
DB_PASSWORD = os.environ.get("DB_PASSWORD", "your_db_password") # <-- REPLACE or set env var
DB_TABLE_ANNUAL = "stock_annual_financials" # Read from this table

# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)-8s - %(message)s',
    handlers=[
        logging.FileHandler("screenAnnualData.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Database Connection ---
def create_db_connection() -> Optional[mysql.connector.MySQLConnection]:
    """Creates and returns a database connection."""
    # (Identical to import script)
    connection = None; logger.debug("Attempting DB connection...")
    try:
        connection = mysql.connector.connect(host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, connection_timeout=10)
        if connection.is_connected(): logger.info("MariaDB connection successful")
        else: logger.error("MariaDB connection failed."); connection = None
    except Error as e: logger.error(f"Error connecting to MariaDB: {e}")
    except Exception as e: logger.error(f"Unexpected error during DB connection: {e}")
    return connection

# --- Data Fetching for Screening ---
def fetch_screening_data(connection) -> Optional[pd.DataFrame]:
    """ Fetches recent years of necessary financial data for all tickers. """
    if not connection or not connection.is_connected():
        logger.error("Cannot fetch screening data, DB connection invalid.")
        return None

    # Fetch slightly more than needed years to handle potential gaps before the window
    years_buffer = SCREENING_YEARS + 1
    current_year = datetime.now().year
    start_year = current_year - years_buffer

    logger.info(f"Fetching annual data from year {start_year} onwards...")
    try:
        # Select only the columns absolutely needed for calculation and display
        query = f"""
            SELECT
                ticker,
                year,
                ebitda,
                operating_cash_flow,
                capital_expenditure,
                free_cash_flow -- Use FMP's FCF if available and preferred
                -- Add other columns if needed for display later
            FROM {DB_TABLE_ANNUAL}
            WHERE year >= %s
            -- Optional: Filter only recently updated tickers?
            -- AND updated_at >= CURDATE() - INTERVAL 7 DAY
            ORDER BY ticker, year ASC -- Order ascending for easier slicing later
        """
        # Use pandas read_sql for efficiency
        df = pd.read_sql(query, connection, params=(start_year,))
        logger.info(f"Fetched {len(df)} annual records for screening.")

        # Convert types after fetch
        numeric_cols = ['ebitda', 'operating_cash_flow', 'capital_expenditure', 'free_cash_flow']
        for col in numeric_cols:
             if col in df.columns:
                 df[col] = pd.to_numeric(df[col], errors='coerce')

        return df

    except Error as e:
        logger.error(f"Database error fetching screening data: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching screening data: {e}", exc_info=True)
        return None

# --- Screening Logic ---
def screen_stocks(financial_df: pd.DataFrame) -> List[Dict]:
    """ Screens stocks based on the fetched financial data DataFrame. """
    if financial_df is None or financial_df.empty:
        logger.warning("No financial data provided for screening.")
        return []

    passed_screening = []
    processed_tickers = 0
    skipped_insufficient_data = 0

    # Group data by ticker to process each stock individually
    grouped = financial_df.groupby('ticker')
    total_tickers = len(grouped)
    logger.info(f"Screening {total_tickers} unique tickers from fetched data...")

    for ticker, group_df in grouped:
        processed_tickers += 1
        logger.debug(f"--- Processing Ticker: {ticker} ---")
        logger.debug(f"Data for {ticker}:\n{group_df.to_string()}")

        # Calculate FCF if needed (or use FMP's value)
        # Check if FMP's FCF is mostly available, otherwise calculate
        use_calculated_fcf = False
        if 'free_cash_flow' not in group_df.columns or group_df['free_cash_flow'].isnull().mean() > 0.5: # If more than 50% missing, calculate
            use_calculated_fcf = True
            if 'operating_cash_flow' in group_df.columns and 'capital_expenditure' in group_df.columns:
                # Assume CapEx is negative from FMP
                group_df['fcf_calculated'] = group_df['operating_cash_flow'] + group_df['capital_expenditure'].fillna(0)
                logger.debug(f"[{ticker}] Calculated FCF = CFO + CapEx.")
            else:
                logger.warning(f"[{ticker}] Cannot calculate FCF (missing CFO or CapEx). Skipping FCF checks.")
                group_df['fcf_calculated'] = np.nan # Mark as NaN if cannot calculate

        fcf_col_to_use = 'fcf_calculated' if use_calculated_fcf else 'free_cash_flow'
        logger.debug(f"[{ticker}] Using '{fcf_col_to_use}' column for FCF checks.")

        # Ensure we have enough years of data *after* potential NaN introduction
        required_cols = ['ebitda', fcf_col_to_use]
        valid_years_df = group_df.dropna(subset=required_cols).tail(SCREENING_YEARS) # Look at latest available years up to SCREENING_YEARS

        if len(valid_years_df) < MIN_DATA_YEARS_REQUIRED:
            logger.debug(f"[{ticker}] Skipped: Insufficient valid data years ({len(valid_years_df)} < {MIN_DATA_YEARS_REQUIRED}) for EBITDA and FCF.")
            skipped_insufficient_data += 1
            continue

        # --- Apply Screening Criteria ---
        # 1. Positive EBITDA Years
        positive_ebitda_count = (valid_years_df['ebitda'] > 0).sum()
        if positive_ebitda_count < MIN_POSITIVE_EBITDA_YEARS:
            logger.debug(f"[{ticker}] Failed: Positive EBITDA years ({positive_ebitda_count} < {MIN_POSITIVE_EBITDA_YEARS})")
            continue

        # 2. Positive FCF Years
        positive_fcf_count = (valid_years_df[fcf_col_to_use] > 0).sum()
        if positive_fcf_count < MIN_POSITIVE_FCF_YEARS:
            logger.debug(f"[{ticker}] Failed: Positive FCF years ({positive_fcf_count} < {MIN_POSITIVE_FCF_YEARS}) using '{fcf_col_to_use}'")
            continue

        # 3. EBITDA Growth (CAGR over the SCREENING_YEARS period)
        ebitda_cagr = None
        is_turnaround = False
        try:
            # Use the actual data points available in the window (could be < SCREENING_YEARS if gaps existed before tail())
            actual_years_in_window = len(valid_years_df)
            if actual_years_in_window >= 2: # Need at least 2 points for growth
                 earliest_ebitda = valid_years_df['ebitda'].iloc[0]
                 latest_ebitda = valid_years_df['ebitda'].iloc[-1]
                 num_periods = actual_years_in_window - 1

                 if pd.notna(earliest_ebitda) and pd.notna(latest_ebitda):
                     if earliest_ebitda <= 0:
                         if latest_ebitda > 0: is_turnaround = True; logger.debug(f"[{ticker}] EBITDA Turnaround detected.")
                         else: pass # Non-positive to non-positive
                     elif latest_ebitda <= 0: pass # Positive to non-positive
                     else: # Positive to positive
                         if earliest_ebitda > 0: # Avoid division by zero
                              base = latest_ebitda / earliest_ebitda
                              if base > 0: # Avoid complex roots
                                   growth_rate = (base ** (1 / num_periods)) - 1
                                   ebitda_cagr = growth_rate * 100
                              else: logger.warning(f"[{ticker}] Negative base ratio for CAGR calc.")
                         else: logger.warning(f"[{ticker}] Zero earliest EBITDA in pos-pos CAGR.")
                 else: logger.debug(f"[{ticker}] Cannot calc CAGR: NaN endpoints.")
            else: logger.debug(f"[{ticker}] Cannot calc CAGR: Less than 2 valid data points in window.")

        except Exception as e: logger.error(f"[{ticker}] Error calculating CAGR: {e}")

        # Check growth condition
        growth_passed = False
        if is_turnaround:
            growth_passed = True
        elif ebitda_cagr is not None and ebitda_cagr >= MIN_EBITDA_GROWTH_PERCENT:
            growth_passed = True

        if not growth_passed:
            cagr_str = f"{ebitda_cagr:.2f}%" if ebitda_cagr is not None else ("Turnaround" if is_turnaround else "N/A")
            logger.debug(f"[{ticker}] Failed: EBITDA Growth ({cagr_str} < {MIN_EBITDA_GROWTH_PERCENT}% or not turnaround)")
            continue

        # --- If all criteria passed ---
        logger.info(f"PASSED: {ticker} (Pos EBITDA: {positive_ebitda_count}/{actual_years_in_window}, Pos FCF: {positive_fcf_count}/{actual_years_in_window}, CAGR: {cagr_str if 'cagr_str' in locals() else 'N/A'})")
        passed_screening.append({
            "ticker": ticker,
            "positive_ebitda_years": positive_ebitda_count,
            "positive_fcf_years": positive_fcf_count,
            "ebitda_cagr_percent": ebitda_cagr,
            "is_ebitda_turnaround": is_turnaround,
            "years_analyzed": actual_years_in_window,
            "last_data_year": valid_years_df.index.max() if isinstance(valid_years_df.index, pd.Index) else valid_years_df['year'].max() # Use year column if index not set
        })

    logger.info(f"Screening finished. Processed {processed_tickers} tickers. Skipped {skipped_insufficient_data} due to insufficient data. Passed: {len(passed_screening)}.")
    return passed_screening

# --- Display Results ---
def display_results(results: List[Dict]):
    if not results:
        logger.info("No stocks met the screening criteria.")
        return

    df = pd.DataFrame(results)

    # Format for display
    df['EBITDA Growth'] = df.apply(lambda row: "Turnaround" if row['is_ebitda_turnaround'] else (f"{row['ebitda_cagr_percent']:.2f}%" if pd.notna(row['ebitda_cagr_percent']) else "N/A"), axis=1)
    df['Pos EBITDA'] = df.apply(lambda row: f"{row['positive_ebitda_years']}/{row['years_analyzed']}", axis=1)
    df['Pos FCF'] = df.apply(lambda row: f"{row['positive_fcf_years']}/{row['years_analyzed']}", axis=1)

    display_df = df[['ticker', 'EBITDA Growth', 'Pos EBITDA', 'Pos FCF', 'last_data_year']]
    display_df = display_df.rename(columns={'ticker': 'Ticker', 'last_data_year': 'Last Year'})

    # Sort results
    display_df = display_df.sort_values(by=['EBITDA Growth'], ascending=False, key=lambda col: col.map(lambda x: float('-inf') if x=='N/A' else (float('inf') if x=='Turnaround' else float(x[:-1]))))


    print("\n--- Stocks Meeting Screening Criteria ---")
    print(display_df.to_string(index=False))

    # Optionally save to CSV
    try:
         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
         filename = f"screened_annual_stocks_{timestamp}.csv"
         display_df.to_csv(filename, index=False)
         logger.info(f"Screening results saved to {filename}")
    except Exception as e:
         logger.error(f"Failed to save results to CSV: {e}")

# --- Main Execution ---
def main():
    start_time = time.time()
    logger.info("==================================================")
    logger.info("=== Starting Stock Screener from Annual Data ===")
    logger.info(f"Screening Period: Last {SCREENING_YEARS} years")
    logger.info(f"Criteria: Pos EBITDA >= {MIN_POSITIVE_EBITDA_YEARS}, Pos FCF >= {MIN_POSITIVE_FCF_YEARS}, EBITDA Growth >= {MIN_EBITDA_GROWTH_PERCENT}%")
    logger.info("==================================================")

    db_connection = create_db_connection()
    if not db_connection: logger.critical("Exiting: Database connection failed."); return

    passed_stocks = []
    try:
        # 1. Fetch Data
        financial_data = fetch_screening_data(db_connection)

        # 2. Screen Data
        if financial_data is not None and not financial_data.empty:
            passed_stocks = screen_stocks(financial_data)
        else:
            logger.warning("No data fetched from database for screening.")

        # 3. Display Results
        display_results(passed_stocks)

    except KeyboardInterrupt: logger.warning("Keyboard interrupt received.")
    except Exception as e: logger.critical(f"An unexpected error occurred in the main screening process: {e}", exc_info=True)
    finally:
        if db_connection and db_connection.is_connected():
            try: db_connection.close(); logger.info("Database connection closed.")
            except Error as e: logger.error(f"Error closing database connection: {e}")

    end_time = time.time()
    logger.info("\n==================================================")
    logger.info("=== Screening Process Complete ===")
    logger.info(f"Total time taken: {end_time - start_time:.2f} seconds")
    logger.info(f"Stocks passing criteria: {len(passed_stocks)}")
    logger.info("==================================================")


if __name__ == "__main__":
    main()