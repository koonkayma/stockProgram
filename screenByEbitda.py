import yfinance as yf
import pandas as pd
import time
import logging
from typing import List, Optional, Tuple

# --- Configuration ---
# How many years of history to check
YEARS_HISTORY = 5
# Minimum number of positive EBITDA years required
MIN_POSITIVE_EBITDA_YEARS = 3
# Minimum required EBITDA growth percentage compared to N years ago
MIN_EBITDA_GROWTH_PERCENT = 15.0
# Delay between fetching data for each stock (in seconds) to avoid blocking
DELAY_BETWEEN_CALLS = 1.5 # Increase if you get errors

# --- Setup Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Function to get Tickers ---
def get_sp500_tickers() -> List[str]:
    """Fetches S&P 500 tickers from Wikipedia."""
    try:
        logging.info("Fetching S&P 500 tickers from Wikipedia...")
        # Adding headers to mimic a browser
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        payload = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', header=0, attrs = {'id': 'constituents'}, storage_options=headers)
        # If read_html worked without User-Agent, remove storage_options=headers
        # payload = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', header=0, attrs = {'id': 'constituents'})
        
        sp500_df = payload[0]
        tickers = sp500_df['Symbol'].tolist()
        # Clean up tickers (e.g., 'BRK.B' -> 'BRK-B' for yfinance)
        tickers = [ticker.replace('.', '-') for ticker in tickers]
        logging.info(f"Successfully fetched {len(tickers)} S&P 500 tickers.")
        return tickers
    except Exception as e:
        logging.error(f"Error fetching S&P 500 tickers: {e}")
        logging.warning("Falling back to a small default list.")
        return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META', 'TSLA', 'NVDA', 'JPM', 'JNJ', 'V'] # Example list

def get_tickers_from_file(filename: str) -> List[str]:
    """Reads tickers from a text file (one ticker per line)."""
    try:
        with open(filename, 'r') as f:
            tickers = [line.strip() for line in f if line.strip()]
        logging.info(f"Read {len(tickers)} tickers from {filename}")
        return tickers
    except FileNotFoundError:
        logging.error(f"Ticker file '{filename}' not found.")
        return []
    except Exception as e:
        logging.error(f"Error reading ticker file {filename}: {e}")
        return []

# --- Screening Function ---
def screen_stock(ticker_symbol: str) -> Optional[Tuple[str, float, int]]:
    """
    Screens a single stock based on EBITDA criteria.

    Args:
        ticker_symbol: The stock ticker symbol.

    Returns:
        A tuple (ticker, growth_rate, positive_years) if it passes, otherwise None.
    """
    try:
        logging.debug(f"Processing ticker: {ticker_symbol}")
        stock = yf.Ticker(ticker_symbol)

        # Get annual financial data
        # financials = stock.financials # Often gives quarterly first, need annual
        financials = stock.get_financials(freq='yearly') # Explicitly request annual

        if financials.empty:
            logging.warning(f"{ticker_symbol}: No financial data found.")
            return None

        # Find EBITDA row - names can vary slightly, check common ones
        ebitda_row_name = None
        possible_names = ['Ebitda', 'EBITDA', 'Normalized EBITDA'] # Check common variations
        for name in possible_names:
             if name in financials.index:
                 ebitda_row_name = name
                 break

        if ebitda_row_name is None:
            logging.warning(f"{ticker_symbol}: EBITDA data not found in financials index. Found: {list(financials.index)}")
            return None

        ebitda_data = financials.loc[ebitda_row_name]

        # Ensure data is numeric and handle potential NaNs
        ebitda_data = pd.to_numeric(ebitda_data, errors='coerce').dropna()

        # Check if we have enough historical data (at least YEARS_HISTORY)
        if len(ebitda_data) < YEARS_HISTORY:
            logging.info(f"{ticker_symbol}: Insufficient data ({len(ebitda_data)} years found, need {YEARS_HISTORY}).")
            return None

        # Get the relevant N years (yfinance usually returns newest first)
        # Ensure columns are sorted chronologically if needed (usually they are descending date)
        # Let's explicitly sort by date descending to be sure
        ebitda_data = ebitda_data.sort_index(ascending=False)
        last_n_years_ebitda = ebitda_data.iloc[:YEARS_HISTORY]

        # --- Criterion 1: At least MIN_POSITIVE_EBITDA_YEARS positive EBITDA ---
        positive_ebitda_count = (last_n_years_ebitda > 0).sum()
        if positive_ebitda_count < MIN_POSITIVE_EBITDA_YEARS:
            logging.debug(f"{ticker_symbol}: Failed positive EBITDA count ({positive_ebitda_count}/{MIN_POSITIVE_EBITDA_YEARS}).")
            return None
        logging.debug(f"{ticker_symbol}: Passed positive EBITDA count ({positive_ebitda_count}/{MIN_POSITIVE_EBITDA_YEARS}).")

        # --- Criterion 2: At least MIN_EBITDA_GROWTH_PERCENT growth vs N years ago ---
        # last_n_years_ebitda is sorted newest to oldest
        latest_ebitda = last_n_years_ebitda.iloc[0]
        ebitda_n_years_ago = last_n_years_ebitda.iloc[YEARS_HISTORY - 1]

        # Handle zero or negative base year EBITDA for growth calculation
        if ebitda_n_years_ago <= 0:
            # If EBITDA was zero/negative N years ago and is positive now,
            # that's significant improvement, but percentage growth is undefined or infinite.
            # We could count this as passing if the latest is positive, or fail it.
            # Let's be strict based on the prompt "growth compare with 5 years ago":
            # requires a meaningful base value for percentage calculation.
            if latest_ebitda > 0:
                 logging.debug(f"{ticker_symbol}: EBITDA was <= 0 {YEARS_HISTORY} years ago ({ebitda_n_years_ago:.2f}), positive now ({latest_ebitda:.2f}). Skipping growth % check.")
                 # Decide if this situation meets the *spirit* of the growth requirement.
                 # For now, let's fail it based on strict percentage calculation needs.
                 # To pass it, you could uncomment the next line and comment the return None
                 # pass # or maybe return True here if criteria 1 passed
                 # return None # Strict interpretation: cannot calculate % growth from non-positive base
                 
                 # Let's allow it if latest is positive - signifies strong turnaround
                 logging.debug(f"{ticker_symbol}: Allowing pass on growth due to turnaround from non-positive base.")
                 growth_rate = float('inf') # Represent turnaround growth

            else:
                 logging.debug(f"{ticker_symbol}: Failed growth check (EBITDA was <= 0 {YEARS_HISTORY} years ago and not positive now).")
                 return None
        else:
            # Calculate percentage growth
            growth = ((latest_ebitda - ebitda_n_years_ago) / abs(ebitda_n_years_ago)) * 100
            growth_rate = growth # Store for return value

            if growth < MIN_EBITDA_GROWTH_PERCENT:
                logging.debug(f"{ticker_symbol}: Failed growth check ({growth:.2f}% < {MIN_EBITDA_GROWTH_PERCENT}%). Latest: {latest_ebitda:.2f}, {YEARS_HISTORY}yrs ago: {ebitda_n_years_ago:.2f}")
                return None
            logging.debug(f"{ticker_symbol}: Passed growth check ({growth:.2f}% >= {MIN_EBITDA_GROWTH_PERCENT}%).")


        # If both criteria passed
        logging.info(f"PASSED: {ticker_symbol} (Positive Years: {positive_ebitda_count}, Growth: {growth_rate:.2f}%)")
        return ticker_symbol, growth_rate, positive_ebitda_count

    except AttributeError as e:
         # Handles cases where .financials or similar might be missing completely
        logging.warning(f"{ticker_symbol}: Attribute error, likely missing fundamental data structure: {e}")
        return None
    except KeyError as e:
        # Handles cases where specific rows like 'Ebitda' are missing
        logging.warning(f"{ticker_symbol}: Key error, likely missing specific data field like EBITDA: {e}")
        return None
    except IndexError as e:
        # Handles cases where there isn't enough historical data
        logging.warning(f"{ticker_symbol}: Index error, likely insufficient historical data points: {e}")
        return None
    except Exception as e:
        # Catch any other unexpected errors for a specific ticker
        logging.error(f"{ticker_symbol}: Unexpected error during screening: {e}")
        return None

# --- Main Execution ---
if __name__ == "__main__":
    logging.info("Starting stock screener...")

    # --- Choose how to get your list of tickers ---
    # Option 1: S&P 500 tickers (requires pandas, lxml, html5lib, beautifulsoup4)
    # You might need to install these: pip install pandas lxml html5lib beautifulsoup4 requests
    stock_tickers = get_sp500_tickers()

    # Option 2: Read tickers from a file (e.g., tickers.txt)
    # Create a file named 'tickers.txt' in the same directory
    # and list one ticker symbol per line (e.g., AAPL, MSFT, GOOGL)
    # ticker_file = 'tickers.txt'
    # stock_tickers = get_tickers_from_file(ticker_file)

    # Option 3: Use a predefined list
    # stock_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'JPM', 'JNJ', 'V', 'UNH', 'XOM', 'META'] # Example list

    if not stock_tickers:
        logging.error("No tickers to screen. Exiting.")
        exit()

    logging.info(f"Screening {len(stock_tickers)} tickers...")
    passed_screening = []
    failed_screening = [] # Optional: keep track of failures/reasons

    for i, ticker in enumerate(stock_tickers):
        logging.info(f"Processing {i + 1}/{len(stock_tickers)}: {ticker}")
        result = screen_stock(ticker)
        if result:
            passed_screening.append(result)
        else:
            failed_screening.append(ticker) # Keep track of which ones failed

        # --- IMPORTANT: Add delay to avoid IP blocking ---
        time.sleep(DELAY_BETWEEN_CALLS)

    # --- Display Results ---
    logging.info("\n--- Screening Complete ---")

    if passed_screening:
        logging.info(f"Stocks meeting the criteria ({len(passed_screening)}):")
        # Sort results alphabetically by ticker or by growth rate
        passed_screening.sort(key=lambda x: x[1], reverse=True) # Sort by growth rate descending

        print("\nTicker | EBITDA Growth % | Positive EBITDA Years")
        print("-------|-----------------|------------------------")
        for ticker, growth, pos_years in passed_screening:
            growth_str = f"{growth:.2f}%" if growth != float('inf') else "Positive Turnaround"
            print(f"{ticker:<6} | {growth_str:<15} | {pos_years}/{YEARS_HISTORY}")
    else:
        logging.info("No stocks met the screening criteria.")

    # Optional: Log tickers that failed or had issues
    # logging.info(f"\nTickers that did not pass or had errors ({len(failed_screening)}): {', '.join(failed_screening)}")
