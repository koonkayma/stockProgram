import mysql.connector
from mysql.connector import Error
import logging
import pandas as pd # For formatting output nicely
import os
from datetime import datetime

# --- Configuration ---
YEARS_HISTORY_FILTER = 5 # Should match the period used in importData.py
MIN_POSITIVE_EBITDA_YEARS = 3
MIN_POSITIVE_FCF_YEARS = 3
MIN_EBITDA_GROWTH_PERCENT = 15.0

# --- Database Configuration ---
# **IMPORTANT**: Use environment variables or a secure config file in production!
DB_HOST = os.environ.get("DB_HOST", "192.168.1.142")
DB_PORT = os.environ.get("DB_PORT", 3306)
DB_NAME = os.environ.get("DB_NAME", "nextcloud")
DB_USER = os.environ.get("DB_USER", "your_db_user") # Replace or set env var
DB_PASSWORD = os.environ.get("DB_PASSWORD", "your_db_password") # Replace or set env var
DB_TABLE = "stock_financial_summary"

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Database Connection ---
def create_db_connection():
    """Creates and returns a database connection."""
    connection = None
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logging.info("MariaDB connection successful")
    except Error as e:
        logging.error(f"Error connecting to MariaDB: {e}")
    return connection

# --- Main Screening Logic ---
def screen_stocks_from_db():
    """Connects to DB, executes screening query, and returns results."""
    results = []
    connection = create_db_connection()
    if not connection:
        return results # Return empty list if connection failed

    cursor = None
    try:
        cursor = connection.cursor(dictionary=True) # Fetch results as dictionaries

        # SQL Query using placeholders for criteria
        sql_query = f"""
            SELECT
                ticker,
                ebitda_cagr_percent,
                is_ebitda_turnaround,
                positive_ebitda_years_count,
                positive_fcf_years_count,
                latest_data_year,
                updated_at
            FROM
                {DB_TABLE}
            WHERE
                data_fetch_error = FALSE   -- Exclude entries with fetch errors
                AND
                data_period_years = %s     -- Ensure data covers the target period
                AND
                positive_ebitda_years_count >= %s -- Min positive EBITDA years
                AND
                positive_fcf_years_count >= %s    -- Min positive FCF years
                AND
                (
                    is_ebitda_turnaround = TRUE     -- Pass if it's a turnaround OR
                    OR
                    (ebitda_cagr_percent IS NOT NULL AND ebitda_cagr_percent >= %s) -- Pass if CAGR meets threshold
                )
            ORDER BY
                is_ebitda_turnaround DESC,  -- Show turnarounds first
                ebitda_cagr_percent DESC;   -- Then sort by growth
        """

        # Parameters for the query
        params = (
            YEARS_HISTORY_FILTER,
            MIN_POSITIVE_EBITDA_YEARS,
            MIN_POSITIVE_FCF_YEARS,
            MIN_EBITDA_GROWTH_PERCENT
        )

        logging.info("Executing screening query...")
        cursor.execute(sql_query, params)
        results = cursor.fetchall()
        logging.info(f"Query executed. Found {len(results)} potential matches.")

    except Error as e:
        logging.error(f"Database error during screening query: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during screening: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            logging.info("Database connection closed.")

    return results

# --- Display Results ---
def display_results(screened_data):
    """Formats and prints the screening results using Pandas."""
    if not screened_data:
        logging.info("No stocks met the screening criteria based on the data in the database.")
        return

    df = pd.DataFrame(screened_data)

    # Format columns for readability
    def format_growth(row):
        if row['is_ebitda_turnaround']: # MariaDB might return 0/1 for BOOLEAN
            return "Positive Turnaround"
        elif pd.notna(row['ebitda_cagr_percent']):
            return f"{row['ebitda_cagr_percent']:.2f}%"
        else:
            return "N/A"

    df['EBITDA Growth'] = df.apply(format_growth, axis=1)
    df['Pos EBITDA Yrs'] = df['positive_ebitda_years_count'].apply(lambda x: f"{x}/{YEARS_HISTORY_FILTER}")
    df['Pos FCF Yrs'] = df['positive_fcf_years_count'].apply(lambda x: f"{x}/{YEARS_HISTORY_FILTER}")
    df['Last Updated'] = pd.to_datetime(df['updated_at']).dt.strftime('%Y-%m-%d %H:%M') # Format timestamp

    # Select and reorder columns for display
    display_df = df[[
        'ticker',
        'EBITDA Growth',
        'Pos EBITDA Yrs',
        'Pos FCF Yrs',
        'latest_data_year',
        'Last Updated'
    ]]
    display_df = display_df.rename(columns={'ticker': 'Ticker', 'latest_data_year': 'Latest Year'})


    print("\n--- Stocks Meeting Screening Criteria ---")
    print(display_df.to_string(index=False))

    # Optionally save to CSV
    try:
         timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
         filename = f"screened_stocks_{timestamp}.csv"
         display_df.to_csv(filename, index=False)
         logging.info(f"Screening results saved to {filename}")
    except Exception as e:
         logging.error(f"Failed to save results to CSV: {e}")


# --- Main Execution ---
if __name__ == "__main__":
    logging.info("Starting stock screening from database...")
    results = screen_stocks_from_db()
    display_results(results)
    logging.info("Screening process finished.")