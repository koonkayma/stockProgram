# File: edgar_financial_importer.py
import os
import mysql.connector
from mysql.connector import Error, pooling

# --- Imports for the 'edgar-py' library ---
try:
    # Common pattern for edgar-py
    from edgar import Session, Filing, Company, set_identity # set_identity is crucial
    print("Using imports from 'edgar-py' library (Session, Filing, Company)")
except ImportError as e:
    print(f"Failed to import Session, Filing, Company from edgar: {e}")
    print("Please ensure the 'edgar' (edgar-py) library is installed correctly.")
    raise e

# Note: XbrlFacts might not be directly importable.
# Access to XBRL data is usually through methods on the Filing object in edgar-py.
# We will adapt the extraction logic later if needed.

from datetime import datetime, timedelta
# ... rest of imports

# --- Configuration ---
DB_CONFIG = { # ... (keep as before)
    'host': 'localhost',
    'user': 'nextcloud',
    'password': 'Ks120909090909#',
    'database': 'nextcloud'
}
EDGAR_USER_AGENT = "Your Name Your Organization your.email@example.com" # *** CHANGE THIS ***
TABLE_NAME = 'edgartools_annual_reports'
ANNUAL_FORMS = ["10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"]
PROCESS_START_YEAR = 2022
PROCESS_END_YEAR = datetime.now().year
CACHE_DIR = Path("edgar_cache").absolute()
CACHE_DIR.mkdir(exist_ok=True)

# --- Logging Setup ---
# ... (keep as before)
log_file = CACHE_DIR / 'edgar_importer.log'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_file), logging.StreamHandler()])


# --- EDGAR Initialization (MUST use set_identity for edgar-py) ---
try:
    # *** CRITICAL: Set the identity for edgar-py ***
    set_identity(EDGAR_USER_AGENT)
    logging.info(f"Edgar identity set: {EDGAR_USER_AGENT}")
    # The Session object is often not needed globally like the Edgar object was.
    # We might create it as needed or pass the identity implicitly.
    # For now, we remove the global `edgar = Edgar(...)` line.
except Exception as e:
    logging.error(f"Failed to set Edgar identity: {e}")
    exit()

# --- Database Connection Pool ---
# ... (keep as before)
db_pool = None
try:
    db_pool = mysql.connector.pooling.MySQLConnectionPool(
        pool_name="edgarpool",
        pool_size=5,
        **DB_CONFIG
    )
    logging.info("MariaDB connection pool created successfully.")
except Error as e:
    logging.error(f"Error creating MariaDB connection pool: {e}")

def get_db_connection(): # ... (keep as before)
    if db_pool is None: logging.error("Database pool is not initialized."); return None
    try: return db_pool.get_connection()
    except Error as e: logging.error(f"Error getting connection from pool: {e}"); return None

# --- Database Functions ---
# ... (keep check_filing_processed, upsert_filing_metadata, update_financial_data as before) ...
# You might need slight adjustments inside these later if the Filing object properties change.

# --- XBRL Extraction Logic ---
# *** THIS SECTION NEEDS SIGNIFICANT CHANGES FOR edgar-py ***
# edgar-py handles XBRL differently. It usually parses it automatically
# when you access certain attributes or methods of the Filing object.
# XbrlFacts class is likely not used. We need to adapt find_xbrl_fact.

XBRL_TAGS_MAP = { # ... (keep the map as before for now) ...
    # Income Statement
    'revenue': ['us-gaap:Revenues', 'us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax', 'us-gaap:SalesRevenueNet'],
    'cost_of_revenue': ['us-gaap:CostOfRevenue', 'us-gaap:CostOfGoodsAndServicesSold'],
    'gross_profit': ['us-gaap:GrossProfit'],
    'research_development_expense': ['us-gaap:ResearchAndDevelopmentExpense'],
    'sga_expense': ['us-gaap:SellingGeneralAndAdministrativeExpense'],
    'operating_income_loss': ['us-gaap:OperatingIncomeLoss'],
    'interest_expense': ['us-gaap:InterestExpense'],
    'income_before_tax': ['us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxExclusiveOfIncomeTaxExpenseBenefit','us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxExpenseBenefit'],
    'income_tax_expense_benefit': ['us-gaap:IncomeTaxExpenseBenefit'],
    'net_income_loss': ['us-gaap:NetIncomeLoss', 'us-gaap:ProfitLoss', 'us-gaap:IncomeLossFromContinuingOperations'],
    # Per Share
    'eps_basic': ['us-gaap:EarningsPerShareBasic'],
    'eps_diluted': ['us-gaap:EarningsPerShareDiluted'],
    'weighted_avg_shares_basic': ['us-gaap:WeightedAverageNumberOfSharesOutstandingBasic', 'us-gaap:WeightedAverageNumberOfShareOutstandingBasicAndDiluted'], # Sometimes basic is reused
    'weighted_avg_shares_diluted': ['us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding'],
    # Balance Sheet - Assets
    'cash_and_equivalents': ['us-gaap:CashAndCashEquivalentsAtCarryingValue'],
    'short_term_investments': ['us-gaap:ShortTermInvestments', 'us-gaap:MarketableSecuritiesCurrent'],
    'accounts_receivable': ['us-gaap:AccountsReceivableNetCurrent'],
    'inventory': ['us-gaap:InventoryNet', 'us-gaap:InventoryNet'],
    'total_current_assets': ['us-gaap:AssetsCurrent'],
    'property_plant_equipment_net': ['us-gaap:PropertyPlantAndEquipmentNet'],
    'goodwill': ['us-gaap:Goodwill'],
    'intangible_assets_net': ['us-gaap:IntangibleAssetsNetExcludingGoodwill'],
    'total_noncurrent_assets': ['us-gaap:AssetsNoncurrent'],
    'total_assets': ['us-gaap:Assets'],
    # Balance Sheet - Liabilities & Equity
    'accounts_payable': ['us-gaap:AccountsPayableCurrent'],
    'short_term_debt': ['us-gaap:ShortTermBorrowings', 'us-gaap:DebtCurrent', 'us-gaap:CurrentPortionOfLongTermDebt'],
    'deferred_revenue_current': ['us-gaap:DeferredRevenueCurrent', 'us-gaap:ContractWithCustomerLiabilityCurrent'],
    'total_current_liabilities': ['us-gaap:LiabilitiesCurrent'],
    'long_term_debt': ['us-gaap:LongTermDebtNoncurrent', 'us-gaap:LongTermDebt'],
    'total_noncurrent_liabilities': ['us-gaap:LiabilitiesNoncurrent'],
    'total_liabilities': ['us-gaap:Liabilities'],
    'common_stock_value': ['us-gaap:CommonStockValue', 'us-gaap:CommonStocksIncludingAdditionalPaidInCapital'],
    'retained_earnings': ['us-gaap:RetainedEarningsAccumulatedDeficit'],
    'total_stockholders_equity': ['us-gaap:StockholdersEquity', 'us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest'],
    'total_liabilities_and_equity': ['us-gaap:LiabilitiesAndStockholdersEquity'],
    # Cash Flow
    'cf_net_cash_operating': ['us-gaap:NetCashProvidedByUsedInOperatingActivities'],
    'cf_capital_expenditures': ['us-gaap:PaymentsToAcquirePropertyPlantAndEquipment', 'us-gaap:PurchaseOfPropertyPlantAndEquipment'], # Usually reported negative
    'cf_net_cash_investing': ['us-gaap:NetCashProvidedByUsedInInvestingActivities'],
    'cf_net_cash_financing': ['us-gaap:NetCashProvidedByUsedInFinancingActivities'],
    'cf_net_change_in_cash': ['us-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease', 'us-gaap:CashPeriodIncreaseDecrease'],
}

def find_edgar_py_fact(facts_dict: dict, tags: list[str], period_end_date_str: str):
    """
    Adapts fact finding for edgar-py's data structure.
    Assumes facts_dict is structured like: {'us-gaap:Revenues': [{'end_date': '...', 'value': ...}, ...]}
    """
    if not facts_dict or not tags or not period_end_date_str:
        return None

    best_value = None
    for tag in tags:
        if tag in facts_dict:
            fact_list = facts_dict[tag]
            for fact_data in fact_list:
                # Check if the fact's period matches the target period end date string
                # edgar-py might use 'period', 'end_date', 'instant' etc. Check its output.
                # Assuming 'end_date' or 'instant' for simplicity here.
                fact_date_str = fact_data.get('end_date') or fact_data.get('instant')
                if fact_date_str == period_end_date_str:
                    # Basic match found - edgar-py often pre-filters dimensions,
                    # but more complex logic might be needed for context.
                    # For now, take the first match for the period.
                    raw_value = fact_data.get('value')
                    if raw_value is not None:
                         try:
                            cleaned_value = str(raw_value).replace(',', '').strip()
                            if cleaned_value:
                                best_value = Decimal(cleaned_value)
                                logging.debug(f"Found fact for tag {tag} on {period_end_date_str}. Value: {best_value}")
                                return best_value # Return first match
                         except (InvalidOperation, ValueError, TypeError) as dec_err:
                             logging.warning(f"Could not convert value '{raw_value}' to Decimal for tag {tag}: {dec_err}")
                             continue # Try next fact in list
            # If we finished the inner loop for this tag without returning, continue to the next tag in the list
    return None # Return None if no match found for any tag


def extract_financials_for_filing(filing: Filing):
    """
    Re-implement financial extraction using edgar-py Filing object methods.
    """
    accession_number = filing.accession_no
    if not accession_number:
        logging.warning("Cannot extract financials, missing accession number.")
        return {}, "Missing Info"

    # Get period end date (fiscal year end) - adjust attribute name if needed
    period_date = filing.filing_period # Common attribute in edgar-py
    if isinstance(period_date, datetime):
        period_date = period_date.date()
    elif isinstance(period_date, str):
         try: period_date = datetime.strptime(period_date, '%Y-%m-%d').date()
         except (ValueError, TypeError): period_date = None
    elif not isinstance(period_date, datetime.date):
         period_date = None

    if not period_date:
        logging.warning(f"Cannot extract financials for {accession_number}, missing or invalid filing_period.")
        # Attempt to get from filing date as a fallback? Risky.
        # For now, mark as Missing Period
        return {}, "Missing Period"

    period_date_str = period_date.strftime('%Y-%m-%d')
    logging.debug(f"Attempting XBRL extraction for {accession_number} (Period: {period_date_str})")
    financial_data = {}
    status = "Extraction Attempted"

    try:
        # In edgar-py, XBRL data is often accessed via a method/property like .get_facts() or .facts
        # This usually triggers the download and parsing if needed.
        facts_data = filing.get_facts() # Or filing.facts, check documentation

        if not facts_data or not hasattr(facts_data, 'us_gaap'): # edgar-py often organizes by taxonomy
            logging.warning(f"No XBRL facts found or parsed (or no us_gaap section) for {accession_number}.")
            return {}, "No XBRL Data"

        # Access the US-GAAP facts dictionary
        us_gaap_facts = facts_data.us_gaap

        # Iterate through our map and use the adapted fact-finding function
        for db_col, tags in XBRL_TAGS_MAP.items():
            financial_data[db_col] = find_edgar_py_fact(us_gaap_facts, tags, period_date_str)

        # --- Post-processing (Calculations) ---
        # ... (Keep the calculation logic as before, it uses the financial_data dict) ...
        if financial_data.get('gross_profit') is None and financial_data.get('revenue') is not None and financial_data.get('cost_of_revenue') is not None:
             try: financial_data['gross_profit'] = financial_data['revenue'] - financial_data['cost_of_revenue']; logging.debug(f"Calculated Gross Profit for {accession_number}")
             except TypeError: pass
        if financial_data.get('operating_income_loss') is None and financial_data.get('gross_profit') is not None:
             op_expenses = Decimal(0)
             if financial_data.get('research_development_expense') is not None: op_expenses += financial_data['research_development_expense']
             if financial_data.get('sga_expense') is not None: op_expenses += financial_data['sga_expense']
             if op_expenses != Decimal(0):
                  try: financial_data['operating_income_loss'] = financial_data['gross_profit'] - op_expenses; logging.debug(f"Calculated Operating Income for {accession_number}")
                  except TypeError: pass

        # Determine final status
        if any(v is not None for v in financial_data.values()):
            status = "Success"
        else:
            status = "Parse Issue"
            logging.warning(f"Found XBRL for {accession_number}, but could not extract desired financial data using mapped tags.")

    except AttributeError as ae:
        logging.warning(f"Attribute error during XBRL processing for {accession_number} (e.g., no 'get_facts' method or 'us_gaap'): {ae}")
        status = "No XBRL Method/Data"
    except FileNotFoundError: # Might still occur if underlying files are missing
         logging.warning(f"XBRL file not found for {accession_number}.")
         status = "No XBRL File"
    except Exception as e:
        logging.error(f"Failed to parse XBRL or extract data for {accession_number}: {e}", exc_info=True) # Show full traceback for parsing errors
        status = "Parse Error"

    return financial_data, status


# --- Refactored Filing Processing Logic ---
def process_individual_filing(filing: Filing):
    """
    Handles processing for a single Filing object using edgar-py objects.
    """
    if not filing or not filing.accession_no:
        logging.warning("Skipping invalid filing object.")
        return "Error - Invalid Filing Object"

    accession_num = filing.accession_no
    status = "Unknown Error"

    try:
        if check_filing_processed(accession_num):
            return "Skipped - Already Processed"

        # Get necessary info from the edgar-py Filing object
        # Adjust attribute names based on edgar-py documentation if needed
        cik = filing.cik
        company_name = filing.company # Might need filing.company.name if it's an object
        form_type = filing.form
        filing_date = filing.filing_date # Expect datetime
        period_date = filing.filing_period # Expect datetime or date

        # Basic validation
        if not all([cik, company_name, form_type, filing_date, accession_num]):
             logging.warning(f"Skipping {accession_num} due to missing core metadata from Filing object.")
             return "Error - Missing Metadata"

        # Ensure company_name is a string
        if not isinstance(company_name, str):
             # Try common attributes if it's an object (like edgar.Company)
             if hasattr(company_name, 'name'): company_name = company_name.name
             elif hasattr(company_name, 'title'): company_name = company_name.title
             else: company_name = str(company_name) # Fallback to string conversion

        # Ensure dates are date objects
        if isinstance(filing_date, datetime): filing_date = filing_date.date()
        if isinstance(period_date, datetime): period_date = period_date.date()

        logging.debug(f"Processing {form_type} for {company_name} ({cik}), Acc#: {accession_num}")

        metadata = {
            'cik': str(cik),
            'company_name': company_name,
            'form_type': form_type,
            'filing_date': filing_date,
            'period_of_report': period_date,
            'accession_number': accession_num,
            'filing_url': filing.html_url, # edgar-py often provides html_url directly
            'filing_html_url': filing.html_url # Can use the same or specific doc URL if available
        }

        if not upsert_filing_metadata(metadata):
            logging.error(f"Failed to save metadata for {accession_num}.")
            return "Error - Metadata DB Fail"

        # Extract Financials using the adapted function
        financial_data, status = extract_financials_for_filing(filing)
        update_financial_data(accession_num, financial_data, status)

        time.sleep(0.1) # Rate limiting

        return status

    except Exception as e:
        logging.error(f"Unhandled error processing filing {accession_num}: {e}", exc_info=True)
        try: update_financial_data(accession_num, {}, "Processing Error")
        except: pass
        return "Error - Unhandled Exception"


# --- Index Processing Logic (Adapt for edgar-py) ---
def process_filings_from_index(year, quarter):
    """Downloads quarterly index, filters, and processes relevant filings using edgar-py."""
    logging.info(f"--- Processing Index: {year} Q{quarter} ---")
    stats = {"Processed": 0, "Skipped": 0, "Errors": 0}

    try:
        # edgar-py might have a different way to get recent filings or index data
        # Let's assume we can get filings by date range or iterate somehow.
        # A common pattern might be fetching recent filings:
        # filings = edgar.get_filings(year=year, quarter=quarter) # Check edgar-py docs for this
        # OR iterate through index files manually if needed.

        # Placeholder: If edgar-py doesn't have direct index access, this part needs significant change.
        # For now, let's assume a hypothetical function `get_index_filings` exists
        # that returns an iterable of Filing objects for the period.
        # Replace this with the actual edgar-py method.
        logging.warning("Index processing needs verification/implementation specific to 'edgar-py' library.")
        # Example using hypothetical get_recent_filings:
        # start_q = datetime(year, 3 * quarter - 2, 1)
        # end_q = (start_q + timedelta(days=95)).replace(day=1) - timedelta(days=1)
        # recent_filings = Filing.get_filings(start_date=start_q, end_date=end_q, forms=ANNUAL_FORMS)

        # **** START Placeholder for Index Iteration ****
        # You'll need to replace this section based on how edgar-py handles bulk retrieval.
        # If it requires downloading and parsing idx files manually, that's a separate task.
        logging.error("Bulk index processing not implemented for edgar-py in this script. Exiting index processing.")
        # **** END Placeholder for Index Iteration ****


    except Exception as idx_err:
        logging.error(f"Failed to process index for {year} Q{quarter}: {idx_err}", exc_info=True)
        stats["Errors"] += 1

    logging.info(f"--- Finished Index: {year} Q{quarter}. Results: {stats} ---")
    return stats


# --- Single Company Processing Logic (Adapt for edgar-py) ---
def process_single_company(cik_to_process):
    """Fetches and processes relevant annual filings for a single CIK using edgar-py."""
    logging.info(f"--- Processing Single Company: CIK {cik_to_process} ---")
    stats = {"Processed": 0, "Skipped": 0, "Errors": 0}

    try:
        # Get Company object using edgar-py
        company = Company(cik_to_process) # Instantiation might be direct
        if not company: # Check if company lookup was successful
             logging.error(f"Could not find company information for CIK {cik_to_process}.")
             stats["Errors"] += 1
             return stats

        # Get filings for the company
        # edgar-py might use a method like get_filings or iterate through company.filings
        # Filter by form type
        company_filings = company.get_filings(form=ANNUAL_FORMS) # Check signature for filtering
        # Or: company_filings = [f for f in company.get_all_filings() if f.form in ANNUAL_FORMS]

        logging.info(f"Found {len(company_filings)} filings matching {ANNUAL_FORMS} for CIK {cik_to_process}.")

        for filing in company_filings:
            try:
                result_status = process_individual_filing(filing)
                if "Error" in result_status: stats["Errors"] += 1
                elif "Skipped" in result_status: stats["Skipped"] += 1
                else: stats["Processed"] += 1
            except Exception as file_proc_err:
                 stats["Errors"] += 1
                 logging.error(f"Error processing filing {getattr(filing, 'accession_no', 'N/A')} for CIK {cik_to_process}: {file_proc_err}", exc_info=False)

    except Exception as company_err:
        logging.error(f"Failed to process company CIK {cik_to_process}: {company_err}", exc_info=True)
        stats["Errors"] += 1

    logging.info(f"--- Finished Single Company: CIK {cik_to_process}. Results: {stats} ---")
    return stats


# --- Script Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import EDGAR annual report financial data into MariaDB using edgar-py.") # Updated description
    parser.add_argument("--cik", dest="cik_to_process", type=str, required=False, default=None, help="Specify a single CIK to process.")
    args = parser.parse_args()

    logging.info(f"===== Starting EDGAR Financial Data Import Script (using edgar-py) =====")
    total_stats = {"Processed": 0, "Skipped": 0, "Errors": 0}

    if args.cik_to_process:
        logging.info(f"Mode: Processing single CIK {args.cik_to_process}")
        cik_stats = process_single_company(args.cik_to_process)
        total_stats = cik_stats
    else:
        # Warn that index processing is not fully implemented for edgar-py here
        logging.warning("Mode: Processing all companies via index files - THIS MODE IS NOT FULLY IMPLEMENTED for 'edgar-py' in this script.")
        logging.warning("Please use the --cik option or adapt the 'process_filings_from_index' function.")
        # Original loop commented out as it relies on unimplemented part
        # for year in range(PROCESS_START_YEAR, PROCESS_END_YEAR + 1):
        #     for quarter in range(1, 5):
        #         now = datetime.now()
        #         if year == now.year and quarter > (now.month - 1) // 3 + 1: continue
        #         q_stats = process_filings_from_index(year, quarter)
        #         # ... aggregate stats ...

    logging.info(f"===== Script Finished =====")
    logging.info(f"Total Run Statistics: {total_stats}")