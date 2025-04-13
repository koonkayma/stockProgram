import requests
import pandas as pd
import time
import logging
from typing import List, Optional, Tuple
from datetime import datetime, timedelta

# --- Configuration ---
YEARS_HISTORY = 5
MIN_POSITIVE_EBITDA_YEARS = 3
MIN_POSITIVE_FCF_YEARS = 3
MIN_EBITDA_GROWTH_PERCENT = 15.0
DELAY_BETWEEN_CALLS = 1.5  # Delay between API calls to avoid rate limiting

# TradingView API configuration
TRADINGVIEW_API_URL = "https://scanner.tradingview.com/america/scan"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Content-Type": "application/json"
}

# --- Setup Logging ---
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def get_all_us_stocks() -> List[str]:
    """Fetches all US stocks from TradingView scanner."""
    try:
        logging.info("Fetching US stocks from TradingView...")
        
        # TradingView scanner payload
        payload = {
            "filter": [
                {"left": "exchange", "operation": "in_range", "right": ["NYSE", "NASDAQ", "AMEX"]},
                {"left": "type", "operation": "in_range", "right": ["stock"]}
            ],
            "options": {"lang": "en"},
            "symbols": {"query": {"types": []}, "tickers": []},
            "columns": ["name", "close", "volume"],
            "sort": {"sortBy": "volume", "sortOrder": "desc"},
            "range": [0, 1000]  # Adjust range as needed
        }

        response = requests.post(TRADINGVIEW_API_URL, json=payload, headers=HEADERS)
        response.raise_for_status()
        
        data = response.json()
        tickers = [item['s'] for item in data['data']]
        logging.info(f"Successfully fetched {len(tickers)} US stocks.")
        return tickers
    except Exception as e:
        logging.error(f"Error fetching US stocks: {e}")
        return []

def get_financial_data(ticker: str) -> Optional[Tuple[pd.Series, pd.Series]]:
    """Fetches EBITDA and FCF data for a given ticker."""
    try:
        # TradingView financial data payload
        payload = {
            "filter": [
                {"left": "name", "operation": "equal", "right": ticker}
            ],
            "options": {"lang": "en"},
            "columns": [
                "fundamental.ebitda",  # EBITDA
                "fundamental.free_cash_flow",  # Free Cash Flow
                "fundamental.ebitda_ttm",  # Trailing Twelve Months EBITDA
                "fundamental.free_cash_flow_ttm"  # Trailing Twelve Months FCF
            ],
            "range": [0, 1]  # Get only one result
        }

        # Print the request details for debugging
        logging.debug(f"Request URL: {TRADINGVIEW_API_URL}")
        logging.debug(f"Request Headers: {HEADERS}")
        logging.debug(f"Request Payload: {payload}")

        response = requests.post(TRADINGVIEW_API_URL, json=payload, headers=HEADERS)
        
        # Print response details if there's an error
        if response.status_code != 200:
            logging.error(f"Response Status Code: {response.status_code}")
            logging.error(f"Response Headers: {response.headers}")
            try:
                logging.error(f"Response Body: {response.json()}")
            except:
                logging.error(f"Response Text: {response.text}")

        response.raise_for_status()
        
        data = response.json()
        if not data['data']:
            return None

        # Extract EBITDA and FCF data
        ebitda_data = pd.Series(data['data'][0]['d'][0])  # EBITDA data
        fcf_data = pd.Series(data['data'][0]['d'][1])     # FCF data

        return ebitda_data, fcf_data
    except Exception as e:
        logging.error(f"Error fetching financial data for {ticker}: {str(e)}")
        logging.error(f"Error type: {type(e).__name__}")
        return None

def screen_stock(ticker: str) -> Optional[Tuple[str, float, int, int]]:
    """
    Screens a single stock based on EBITDA and FCF criteria.

    Returns:
        A tuple (ticker, growth_rate, positive_ebitda_years, positive_fcf_years) if it passes,
        otherwise None.
    """
    try:
        logging.debug(f"Processing ticker: {ticker}")
        
        financial_data = get_financial_data(ticker)
        if not financial_data:
            return None

        ebitda_data, fcf_data = financial_data

        # Ensure we have enough data points
        if len(ebitda_data) < YEARS_HISTORY or len(fcf_data) < YEARS_HISTORY:
            logging.debug(f"{ticker}: Insufficient data points")
            return None

        # Check positive EBITDA years
        positive_ebitda_years = (ebitda_data > 0).sum()
        if positive_ebitda_years < MIN_POSITIVE_EBITDA_YEARS:
            logging.debug(f"{ticker}: Failed positive EBITDA count ({positive_ebitda_years}/{MIN_POSITIVE_EBITDA_YEARS})")
            return None

        # Check positive FCF years
        positive_fcf_years = (fcf_data > 0).sum()
        if positive_fcf_years < MIN_POSITIVE_FCF_YEARS:
            logging.debug(f"{ticker}: Failed positive FCF count ({positive_fcf_years}/{MIN_POSITIVE_FCF_YEARS})")
            return None

        # Calculate EBITDA growth
        latest_ebitda = ebitda_data.iloc[0]
        ebitda_5y_ago = ebitda_data.iloc[YEARS_HISTORY - 1]
        
        if ebitda_5y_ago <= 0:
            if latest_ebitda > 0:
                growth_rate = float('inf')  # Represent turnaround growth
            else:
                return None
        else:
            growth_rate = ((latest_ebitda - ebitda_5y_ago) / abs(ebitda_5y_ago)) * 100
            if growth_rate < MIN_EBITDA_GROWTH_PERCENT:
                logging.debug(f"{ticker}: Failed growth check ({growth_rate:.2f}% < {MIN_EBITDA_GROWTH_PERCENT}%)")
                return None

        logging.info(f"PASSED: {ticker} (EBITDA Growth: {growth_rate:.2f}%, "
                    f"Positive EBITDA Years: {positive_ebitda_years}, "
                    f"Positive FCF Years: {positive_fcf_years})")
        
        return ticker, growth_rate, positive_ebitda_years, positive_fcf_years

    except Exception as e:
        logging.error(f"{ticker}: Error during screening: {e}")
        return None

def main():
    logging.info("Starting stock screener...")
    
    # Get all US stocks
    stock_tickers = get_all_us_stocks()
    if not stock_tickers:
        logging.error("No tickers to screen. Exiting.")
        return

    logging.info(f"Screening {len(stock_tickers)} stocks...")
    passed_screening = []

    for i, ticker in enumerate(stock_tickers):
        logging.info(f"Processing {i + 1}/{len(stock_tickers)}: {ticker}")
        result = screen_stock(ticker)
        if result:
            passed_screening.append(result)
        
        time.sleep(DELAY_BETWEEN_CALLS)

    # Display results
    logging.info("\n--- Screening Complete ---")
    
    if passed_screening:
        logging.info(f"Stocks meeting the criteria ({len(passed_screening)}):")
        passed_screening.sort(key=lambda x: x[1], reverse=True)  # Sort by growth rate

        print("\nTicker | EBITDA Growth % | Positive EBITDA Years | Positive FCF Years")
        print("-------|-----------------|----------------------|-------------------")
        for ticker, growth, ebitda_years, fcf_years in passed_screening:
            growth_str = f"{growth:.2f}%" if growth != float('inf') else "Positive Turnaround"
            print(f"{ticker:<6} | {growth_str:<15} | {ebitda_years}/{YEARS_HISTORY:<20} | {fcf_years}/{YEARS_HISTORY}")
    else:
        logging.info("No stocks met the screening criteria.")

if __name__ == "__main__":
    main() 