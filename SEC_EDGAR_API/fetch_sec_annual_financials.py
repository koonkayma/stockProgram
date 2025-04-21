# <<< fetch_sec_annual_financials.py >>>
# MODIFIED TO SUPPORT BOTH US-GAAP (10-K) and IFRS (20-F) Filings

import requests
import json
import time
import logging
import os
import warnings # To suppress InsecureRequestWarning
import re # Import regex for camel_to_snake
import argparse # Import argparse for command-line arguments
from datetime import datetime
from decimal import Decimal, InvalidOperation

# --- Database Setup (using SQLAlchemy) ---
from sqlalchemy import create_engine, Column, Integer, String, Date, DECIMAL, TIMESTAMP, PrimaryKeyConstraint, BigInteger
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.mysql import INTEGER # For UNSIGNED, specific to MySQL/MariaDB dialect
from sqlalchemy.sql import func

# --- Configuration ---
# !!! IMPORTANT: REPLACE with your actual company/app details and contact email !!!
USER_AGENT = "YourCompanyName/SECDataFetcher YourEmail@example.com" # REPLACE with your details

if "YourCompanyName" in USER_AGENT or "YourEmail@example.com" in USER_AGENT:
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    print("!! WARNING: Default User-Agent detected.                         !!")
    print("!! Please REPLACE the USER_AGENT variable in the script          !!")
    print("!! with your actual Company/Application Name and Contact Email.  !!")
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    time.sleep(5)

DB_CONNECTION_STRING = os.environ.get(
    "DB_CONNECTION_STRING",
    'mysql+mysqlconnector://nextcloud:Ks120909090909#@127.0.0.1:3306/nextcloud' # Example, replace
)

COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANY_FACTS_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

# --- >>> MODIFIED: Financial Tags to Extract <<< ---
# Maps internal concept names to a LIST of possible XBRL tags (US-GAAP, IFRS).
# The script will try these tags in order. Add more IFRS tags as needed/discovered.
# Research required for precise IFRS tag equivalents for all concepts.
DESIRED_TAGS = {
    # Assets
    "Assets": ["us-gaap:Assets", "ifrs-full:Assets"],
    "AssetsCurrent": ["us-gaap:AssetsCurrent", "ifrs-full:CurrentAssets"],
    "CashAndEquivalents": ["us-gaap:CashAndCashEquivalentsAtCarryingValue", "ifrs-full:CashAndCashEquivalents"],
    "MarketableSecuritiesCurrent": ["us-gaap:MarketableSecuritiesCurrent"], # Less direct IFRS equivalent, often part of FinancialAssets
    "MarketableSecuritiesNoncurrent": ["us-gaap:MarketableSecuritiesNoncurrent"], # Less direct IFRS equivalent
    "AccountsReceivableNet": ["us-gaap:AccountsReceivableNetCurrent", "ifrs-full:TradeAndOtherCurrentReceivables"], # Often includes 'Trade'
    "PrepaidExpenseCurrent": ["us-gaap:PrepaidExpenseCurrent", "ifrs-full:CurrentPrepayments"],
    "InventoryNet": ["us-gaap:InventoryNet", "ifrs-full:Inventories"],
    "OtherAssetsCurrent": ["us-gaap:OtherAssetsCurrent", "ifrs-full:OtherCurrentAssets"],
    "DeferredTaxAssetsNetCurrent": ["us-gaap:DeferredTaxAssetsNetCurrent", "ifrs-full:CurrentDeferredTaxAssets"],
    "PropertyPlantEquipmentNet": ["us-gaap:PropertyPlantAndEquipmentNet", "ifrs-full:PropertyPlantAndEquipment"],
    "AccumulatedDepreciation": ["us-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment"], # Often disclosed in notes for IFRS
    "Goodwill": ["us-gaap:Goodwill", "ifrs-full:Goodwill"],
    "IntangibleAssetsNetExcludingGoodwill": ["us-gaap:IntangibleAssetsNetExcludingGoodwill", "ifrs-full:IntangibleAssetsOtherThanGoodwill"],
    "Investments": ["us-gaap:Investments", "ifrs-full:NoncurrentInvestments"], # Needs refinement based on type
    "OtherAssetsNoncurrent": ["us-gaap:OtherAssetsNoncurrent", "ifrs-full:OtherNoncurrentAssets"],
    "DeferredTaxAssetsNetNoncurrent": ["us-gaap:DeferredTaxAssetsNetNoncurrent", "ifrs-full:NoncurrentDeferredTaxAssets"],
    # Liabilities
    "Liabilities": ["us-gaap:Liabilities", "ifrs-full:Liabilities"],
    "LiabilitiesCurrent": ["us-gaap:LiabilitiesCurrent", "ifrs-full:CurrentLiabilities"],
    "AccountsPayable": ["us-gaap:AccountsPayableCurrent", "ifrs-full:TradeAndOtherCurrentPayables"], # Often includes 'Trade'
    "IncomeTaxesPayable": ["us-gaap:IncomeTaxesPayable", "ifrs-full:CurrentTaxLiabilitiesCurrent"],
    "AccruedLiabilitiesCurrent": ["us-gaap:AccruedLiabilitiesCurrent", "ifrs-full:OtherCurrentLiabilities"], # Accruals often part of 'Other'
    "DeferredRevenueCurrent": ["us-gaap:DeferredRevenueCurrent", "ifrs-full:CurrentContractLiabilities"], # Or CurrentDeferredRevenue
    "DebtCurrent": ["us-gaap:DebtCurrent", "ifrs-full:CurrentBorrowings"],
    "OtherLiabilitiesCurrent": ["us-gaap:OtherLiabilitiesCurrent", "ifrs-full:OtherCurrentLiabilities"],
    "LongTermDebtNoncurrent": ["us-gaap:LongTermDebtNoncurrent", "ifrs-full:NoncurrentBorrowings"],
    "DeferredRevenueNoncurrent": ["us-gaap:DeferredRevenueNoncurrent", "ifrs-full:NoncurrentContractLiabilities"], # Or NoncurrentDeferredRevenue
    "OtherLiabilitiesNoncurrent": ["us-gaap:OtherLiabilitiesNoncurrent", "ifrs-full:OtherNoncurrentLiabilities"],
    "DeferredTaxLiabilitiesNoncurrent": ["us-gaap:DeferredTaxLiabilitiesNoncurrent", "ifrs-full:NoncurrentDeferredTaxLiabilities"],
    "CommitmentsAndContingencies": ["us-gaap:CommitmentsAndContingencies"], # Often disclosed in notes for IFRS
    # Equity
    "StockholdersEquity": ["us-gaap:StockholdersEquity", "ifrs-full:Equity"],
    "RetainedEarningsAccumulatedDeficit": ["us-gaap:RetainedEarningsAccumulatedDeficit", "ifrs-full:RetainedEarnings"],
    "AccumulatedOtherComprehensiveIncomeLossNetOfTax": ["us-gaap:AccumulatedOtherComprehensiveIncomeLossNetOfTax", "ifrs-full:OtherReserves"], # Maps loosely
    "CommonStockValue": ["us-gaap:CommonStockValue", "ifrs-full:IssuedCapital"], # Often just 'IssuedCapital' in IFRS
    "AdditionalPaidInCapital": ["us-gaap:AdditionalPaidInCapital", "ifrs-full:SharePremium"],
    "TreasuryStockValue": ["us-gaap:TreasuryStockValue", "ifrs-full:TreasurySharesValue"],
    "PreferredStockValue": ["us-gaap:PreferredStockValue"], # Less common/standard tag in IFRS?
    "NoncontrollingInterest": ["us-gaap:NoncontrollingInterest", "ifrs-full:NoncontrollingInterests"],
    # Income Statement
    "Revenues": ["us-gaap:Revenues", "ifrs-full:Revenue"], # Often 'RevenueFromContractsWithCustomers' too
    "CostOfRevenue": ["us-gaap:CostOfRevenue", "ifrs-full:CostOfSales"],
    "GrossProfit": ["us-gaap:GrossProfit", "ifrs-full:GrossProfit"],
    "DepreciationExpense": ["us-gaap:Depreciation"], # Need specific IFRS location (CF or Notes)
    "AmortizationOfIntangibleAssets": ["us-gaap:AmortizationOfIntangibleAssets"], # Need specific IFRS location (CF or Notes)
    "OperatingExpenses": ["us-gaap:OperatingExpenses"], # IFRS often lists expenses by function/nature directly
    "SellingGeneralAndAdministrativeExpense": ["us-gaap:SellingGeneralAndAdministrativeExpense", "ifrs-full:SellingGeneralAndAdministrativeExpense"], # May exist or be split
    "ResearchAndDevelopmentExpense": ["us-gaap:ResearchAndDevelopmentExpense", "ifrs-full:ResearchAndDevelopmentExpense"],
    "OperatingIncomeLoss": ["us-gaap:OperatingIncomeLoss", "ifrs-full:OperatingIncomeLoss", "ifrs-full:ProfitLossFromOperatingActivities"],
    "OtherOperatingIncomeExpenseNet": ["us-gaap:OtherOperatingIncomeExpenseNet"], # Often split in IFRS
    "InterestExpense": ["us-gaap:InterestExpense", "ifrs-full:FinanceCosts"], # Often 'Finance Costs'
    "InterestIncome": ["us-gaap:InterestIncome", "ifrs-full:FinanceIncome"], # Often 'Finance Income'
    "InterestIncomeExpenseNet": ["us-gaap:InterestIncomeExpenseNet"], # Calculate if needed
    "OtherNonoperatingIncomeExpense": ["us-gaap:OtherNonoperatingIncomeExpense"], # Often split in IFRS
    "IncomeBeforeTax": ["us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxExtraordinaryItemsNoncontrollingInterest", "ifrs-full:ProfitLossBeforeTax"],
    "IncomeTaxExpenseBenefit": ["us-gaap:IncomeTaxExpenseBenefit", "ifrs-full:IncomeTaxExpenseContinuingOperations"],
    "IncomeTaxExpenseBenefitContinuingOperations": ["us-gaap:IncomeTaxExpenseBenefitContinuingOperations", "ifrs-full:IncomeTaxExpenseContinuingOperations"],
    "IncomeLossFromContinuingOperationsAfterTax": ["us-gaap:IncomeLossFromContinuingOperationsAfterTax", "ifrs-full:ProfitLossFromContinuingOperations"],
    "IncomeLossFromDiscontinuedOperationsNetOfTax": ["us-gaap:IncomeLossFromDiscontinuedOperationsNetOfTax", "ifrs-full:ProfitLossFromDiscontinuedOperations"],
    "NetIncomeLoss": ["us-gaap:NetIncomeLoss", "ifrs-full:ProfitLoss", "ifrs-full:AccountingProfit"], # ProfitLoss is primary IFRS term
    "ComprehensiveIncomeNetOfTax": ["us-gaap:ComprehensiveIncomeNetOfTax", "ifrs-full:ComprehensiveIncome"],
    # EPS & Shares
    "EPSBasic": ["us-gaap:EarningsPerShareBasic", "ifrs-full:BasicEarningsLossPerShare"],
    "EPSDiluted": ["us-gaap:EarningsPerShareDiluted", "ifrs-full:DilutedEarningsLossPerShare"],
    "SharesOutstanding": ["dei:EntityCommonStockSharesOutstanding"], # DEI often used by both
    "SharesBasicWeightedAvg": ["us-gaap:WeightedAverageNumberOfSharesOutstandingBasic", "ifrs-full:WeightedAverageNumberOfSharesOutstandingBasic"], # Check IFRS tag detail
    "SharesDilutedWeightedAvg": ["us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding", "ifrs-full:WeightedAverageNumberOfDilutedSharesOutstandingDiluted"], # Check IFRS tag detail
    # Cash Flow
    "OperatingCashFlow": ["us-gaap:NetCashProvidedByUsedInOperatingActivities", "ifrs-full:CashFlowsFromUsedInOperatingActivities"],
    "CapitalExpenditures": ["us-gaap:PaymentsToAcquirePropertyPlantAndEquipment", "ifrs-full:PurchaseOfPropertyPlantAndEquipment"],
    "DepreciationAndAmortization": ["us-gaap:DepreciationAndAmortization", "ifrs-full:DepreciationAndAmortisationExpense"], # Found in CF or notes
    "DividendsPaid": ["us-gaap:PaymentsOfDividends", "ifrs-full:DividendsPaidClassifiedAsFinancingActivities"], # Location can vary
}

REQUEST_DELAY = 0.11

# --- Logging Setup ---
LOG_LEVEL = logging.INFO # Change to logging.DEBUG for more detail
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# --- Helper Functions --- (No changes needed in helpers)
def camel_to_snake(name):
    """Converts CamelCase names to snake_case."""
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', name)
    return name.lower()

def get_sec_data(url, headers, verify_ssl=True):
    """Fetches data from SEC API with error handling, retries, and rate limit awareness."""
    retries = 3; base_delay = 1; last_exception = None
    for attempt in range(retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=30, verify=verify_ssl)
            if response.status_code == 403: logging.error(f"HTTP 403 Forbidden for {url}. CRITICAL: CHECK USER-AGENT!"); return None
            if response.status_code == 404: logging.warning(f"HTTP 404 Not Found for {url}."); return None
            response.raise_for_status()
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' in content_type: return response.json()
            else:
                logging.warning(f"Unexpected Content-Type '{content_type}' from {url}. Trying decode.");
                try: return response.json()
                except json.JSONDecodeError as e: logging.error(f"Decode failed: {e}"); return None
        except requests.exceptions.HTTPError as e:
            last_exception = e
            if response.status_code == 429: wait_time = (base_delay * (2 ** attempt)) + (5 * (attempt + 1)); logging.warning(f"HTTP 429 Rate limit. Retrying in {wait_time:.2f}s..."); time.sleep(wait_time)
            elif 500 <= response.status_code < 600: wait_time = base_delay * (2 ** attempt); logging.warning(f"HTTP {response.status_code} Server Error. Retrying in {wait_time:.2f}s..."); time.sleep(wait_time)
            else: logging.error(f"Unhandled HTTP Error {response.status_code} for {url}: {e}"); return None
        except requests.exceptions.SSLError as e: logging.error(f"SSL Error: {e}"); last_exception = e; return None
        except requests.exceptions.Timeout as e: last_exception = e; wait_time = base_delay * (2 ** attempt); logging.warning(f"Timeout. Retrying in {wait_time:.2f}s..."); time.sleep(wait_time)
        except requests.exceptions.ConnectionError as e: last_exception = e; wait_time = base_delay * (2 ** attempt); logging.warning(f"Connection error: {e}. Retrying in {wait_time:.2f}s..."); time.sleep(wait_time)
        except json.JSONDecodeError as e: last_exception = e; logging.error(f"JSON Decode Error: {e}"); return None
        except Exception as e: last_exception = e; logging.error(f"Unexpected error fetching {url}: {type(e).__name__} - {e}", exc_info=True); wait_time = base_delay * (2 ** attempt); time.sleep(wait_time)
    logging.error(f"Failed fetch {url} after {retries + 1} attempts. Last error: {last_exception}"); return None

def parse_date(date_str):
    if not date_str or not isinstance(date_str, str): return None
    try: return datetime.strptime(date_str, '%Y-%m-%d').date()
    except (ValueError, TypeError): logging.debug(f"Could not parse date string: '{date_str}'"); return None

def safe_decimal(value):
    if value is None: return None
    if isinstance(value, Decimal): return value
    try:
        if isinstance(value, str): value = value.strip();
        if not value: return None
        try: return Decimal(value)
        except InvalidOperation: float_val = float(value); return Decimal(str(float_val)) # Handle sci notation via float
    except (ValueError, TypeError, InvalidOperation) as e: logging.warning(f"Could not convert '{value}' (type: {type(value)}) to Decimal: {e}"); return None

def safe_int_or_bigint(value):
    if value is None: return None
    if isinstance(value, int): return value
    try:
        dec_val = Decimal(str(value).strip())
        if dec_val % 1 != 0: logging.debug(f"Truncating fractional part for integer conversion: {value}")
        return int(dec_val)
    except (InvalidOperation, ValueError, TypeError) as e: logging.warning(f"Could not convert '{value}' to integer: {e}"); return None

# --- SQLAlchemy Model Definition --- (No changes needed in model definition itself)
Base = declarative_base()
class AnnualData(Base):
    __tablename__ = 'sec_annual_data'
    # ... (SQLAlchemy model definition remains the same as your previous version) ...
    # Core identifying columns
    cik = Column(INTEGER(unsigned=True), primary_key=True, comment='Company Identifier (CIK)')
    year = Column(Integer, primary_key=True, comment='Fiscal Year')

    # Metadata columns
    ticker = Column(String(20), nullable=True, index=True, comment='Trading Symbol at time of fetch')
    company_name = Column(String(255), nullable=True, comment='Company Name from SEC data')
    form = Column(String(10), nullable=True, comment='Source Form (e.g., 10-K, 20-F)') # Modified comment
    filed_date = Column(Date, nullable=True, comment='Filing Date of the source form')
    period_end_date = Column(Date, nullable=True, comment='Period End Date for the fiscal year')

    # Dynamically create financial data columns based on DESIRED_TAGS keys (concepts)
    _column_definitions = {}
    # Use the *keys* from DESIRED_TAGS (Assets, Liabilities, etc.) to generate columns
    for key in DESIRED_TAGS.keys(): # Iterate through concepts
        col_name = camel_to_snake(key)
        col_type = None
        # Comment now reflects the concept, not a single tag
        comment_str = f'{key} (Mapped from US-GAAP/IFRS)'
        if 'shares' in col_name: col_type = BigInteger().with_variant(INTEGER(unsigned=True), "mysql")
        elif 'eps' in col_name: col_type = DECIMAL(18, 6)
        else: col_type = DECIMAL(28, 4)
        _column_definitions[col_name] = Column(col_type, nullable=True, comment=comment_str)
    locals().update(_column_definitions)

    # Calculated Free Cash Flow Column
    calculated_free_cash_flow = Column(DECIMAL(28, 4), nullable=True, comment='Calculated Free Cash Flow (OperatingCashFlow - CapitalExpenditures)')

    # Audit timestamp column
    updated_at = Column(TIMESTAMP, nullable=False, server_default=func.now(), onupdate=func.now(), comment='Record last updated timestamp')

    __table_args__ = (
        PrimaryKeyConstraint('cik', 'year', name='pk_sec_annual_data'),
        {'comment': 'Stores annual financial data from SEC API (GAAP/IFRS)'} # Modified comment
    )
    def __repr__(self): return f"<AnnualData(cik={self.cik}, year={self.year}, ticker='{self.ticker}')>"


# --- >>> MODIFIED: Processing Function <<< ---
def process_company_facts(cik, company_data):
    """Processes the raw JSON facts data for a single company, attempting to use US-GAAP or IFRS tags."""
    if not company_data or 'facts' not in company_data:
        logging.debug(f"CIK {cik}: No 'facts' key found.")
        return {}, None

    company_name = company_data.get('entityName', 'N/A')
    facts = company_data['facts']
    annual_results = {} # { year: {'col_name_snake': value, ...}, ... }

    # Iterate through desired *concepts* (keys of DESIRED_TAGS)
    for key, possible_tags in DESIRED_TAGS.items():
        db_col_snake = camel_to_snake(key)
        found_tag_data = None
        used_full_tag = None # Keep track of which tag was actually found and used

        # Try each possible tag for the current concept (key)
        for full_tag in possible_tags:
            try:
                # Basic check for valid tag format
                if ':' not in full_tag:
                    logging.warning(f"CIK {cik}: Invalid tag format '{full_tag}' in DESIRED_TAGS for key '{key}'")
                    continue

                taxonomy, tag_name = full_tag.split(':', 1) # Split only once

                if taxonomy in facts and tag_name in facts[taxonomy]:
                    # Found a usable tag for this concept!
                    found_tag_data = facts[taxonomy][tag_name]
                    used_full_tag = full_tag
                    logging.debug(f"CIK {cik}: Found match for concept '{key}' using tag '{used_full_tag}'")
                    break # Stop searching for tags for this concept
            except ValueError:
                 logging.warning(f"CIK {cik}: Malformed tag string '{full_tag}' in DESIRED_TAGS for key '{key}'")
                 continue
            except Exception as e:
                 logging.error(f"CIK {cik}: Error checking tag {full_tag} for key {key}: {e}")
                 continue

        # If no tag was found for this concept after checking all possibilities
        if not found_tag_data:
            logging.debug(f"CIK {cik}: No usable tag found for concept '{key}' from list: {possible_tags}")
            continue # Skip to the next concept in DESIRED_TAGS

        # --- Process the data from the tag that was found ---
        try:
            units = found_tag_data.get('units')
            if not units:
                logging.debug(f"CIK {cik}: Tag '{used_full_tag}' (for concept '{key}') has no units.")
                continue

            # Determine unit key (USD, shares, USD/shares)
            unit_key = None
            is_share_metric = 'shares' in db_col_snake
            is_eps_metric = 'eps' in db_col_snake

            # Prioritize standard units
            if is_share_metric and 'shares' in units: unit_key = 'shares'
            elif is_eps_metric and 'USD/shares' in units: unit_key = 'USD/shares'
            elif not is_share_metric and not is_eps_metric and 'USD' in units: unit_key = 'USD'
            elif units: # Fallback: use the first unit found if primary is missing
                first_unit = list(units.keys())[0]
                unit_key = first_unit
                expected_unit = 'shares' if is_share_metric else ('USD/shares' if is_eps_metric else 'USD')
                logging.debug(f"CIK {cik}, Tag {used_full_tag}: Expected unit containing '{expected_unit}', using first available '{first_unit}'.")

            if not unit_key or unit_key not in units:
                logging.debug(f"CIK {cik}: Tag {used_full_tag} - No valid unit key found or unit key '{unit_key}' not in units {list(units.keys())}")
                continue

            unit_data = units[unit_key]
            yearly_data_for_tag = {} # Stores best entry per year { year: {data} } for this specific tag

            # --- Loop through individual data points for the found tag ---
            for entry in unit_data:
                 # Only consider FY (Fiscal Year) data points with a valid year
                if entry.get('form') and entry.get('fp') == 'FY' and entry.get('fy') is not None:
                    fy = entry.get('fy')
                    filed_date = parse_date(entry.get('filed'))
                    if not filed_date: continue # Skip if filing date is invalid

                    # --- MODIFIED: Select best entry logic (Prefer 10-K/20-F, then latest filing) ---
                    is_better_candidate = False
                    current_best_str = ""
                    if fy not in yearly_data_for_tag:
                        is_better_candidate = True
                    else:
                        current_best = yearly_data_for_tag[fy]
                        current_form = current_best.get('form')
                        entry_form = entry.get('form')
                        current_best_str = f"(Current best: Form={current_form}, Filed={current_best.get('filed_date')})"

                        # Check if forms are primary annual reports (10-K or 20-F)
                        entry_is_annual_report = entry_form in ('10-K', '20-F')
                        current_is_annual_report = current_form in ('10-K', '20-F')

                        if entry_is_annual_report and not current_is_annual_report:
                            is_better_candidate = True # Prefer annual report over others
                        elif entry_is_annual_report == current_is_annual_report:
                            # If both same type (both annual OR both not), prefer latest filing
                            if filed_date > current_best['filed_date']:
                                is_better_candidate = True
                        # Else (entry is not annual, current is) -> not better

                    if is_better_candidate:
                        # Parse value based on metric type
                        value_to_store = safe_int_or_bigint(entry.get('val')) if is_share_metric else safe_decimal(entry.get('val'))

                        if value_to_store is not None:
                            yearly_data_for_tag[fy] = {
                                'val': value_to_store,
                                'filed_date': filed_date,
                                'form': entry.get('form'), # Store the actual form (10-K, 20-F, etc.)
                                'end_date': parse_date(entry.get('end'))
                            }
                            logging.debug(f"CIK {cik} FY {fy}: Stored '{key}' ({used_full_tag}) val={value_to_store} from {entry.get('form')} filed {filed_date}")
                        else:
                             logging.debug(f"CIK {cik} FY {fy}: Value parsing failed for '{key}' ({used_full_tag}) val='{entry.get('val')}'")


            # --- Integrate best data for this tag/concept into the main results ---
            for year, data in yearly_data_for_tag.items():
                if year not in annual_results:
                    annual_results[year] = {'cik': cik, 'year': year} # Initialize dict for the year

                # Store the financial value using the snake_case column name
                annual_results[year][db_col_snake] = data['val']

                # --- MODIFIED: Update metadata (form, dates) logic ---
                # Prioritize metadata from Annual Reports (10-K/20-F), then by latest filing date
                update_metadata = False
                if 'form' not in annual_results[year] or not annual_results[year].get('filed_date'):
                    update_metadata = True # Always update if missing
                else:
                    current_meta_form = annual_results[year].get('form')
                    current_meta_filed = annual_results[year].get('filed_date')
                    tag_entry_form = data['form']
                    tag_entry_filed = data['filed_date']

                    entry_is_annual_report = tag_entry_form in ('10-K', '20-F')
                    current_is_annual_report = current_meta_form in ('10-K', '20-F')

                    if entry_is_annual_report and not current_is_annual_report:
                        update_metadata = True # Prefer metadata from annual report form
                    elif entry_is_annual_report == current_is_annual_report:
                         # If both same type (both annual OR both not), prefer metadata from later filing
                        if tag_entry_filed and current_meta_filed and tag_entry_filed > current_meta_filed:
                            update_metadata = True
                    # Else (entry not annual, current is) -> don't update

                if update_metadata:
                    logging.debug(f"CIK {cik} FY {year}: Updating metadata (form, dates) based on tag '{used_full_tag}'")
                    annual_results[year]['form'] = data['form']
                    annual_results[year]['filed_date'] = data['filed_date']
                    annual_results[year]['period_end_date'] = data['end_date']

        except Exception as e:
            logging.error(f"Error processing data for tag {used_full_tag} (concept: {key}, col: {db_col_snake}) for CIK {cik}: {type(e).__name__} - {e}", exc_info=False)

    # --- >>> END Loop through DESIRED_TAGS concepts <<< ---

    # --- Add logging for final annual_results structure if debugging ---
    if LOG_LEVEL <= logging.DEBUG:
        for yr, data in annual_results.items():
            logging.debug(f"CIK {cik} - Final data prepared for DB (Year {yr}): {data}")
    # ---

    return annual_results, company_name


# --- Main Execution Block ---
# (No changes needed in the main __main__ block from your previous version)
if __name__ == "__main__":
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Fetch SEC annual financial data (GAAP/IFRS) and store it.")
    parser.add_argument("--cik", type=str, help="Optional: Process only a specific CIK.", required=False)
    args = parser.parse_args()
    target_cik = args.cik.lstrip('0') if args.cik else None

    logging.info("="*60 + "\nStarting SEC Annual Financial Data Fetcher (GAAP/IFRS)\n" + "="*60)
    # ... (rest of the main block: logging setup, DB connection, ticker list fetching, company processing loop) ...
    # ... (The loop calls the modified process_company_facts) ...
    # ... (Database merging logic remains the same) ...
    # ... (Final summary and cleanup) ...

    if target_cik:
        logging.info(f"--- Running in single CIK mode for CIK: {target_cik} ---")
    else:
        logging.info("--- Running in full mode (all companies) ---")

    logging.info(f"Using User-Agent: {USER_AGENT}")
    logging.info(f"Database Target: {DB_CONNECTION_STRING.split('@')[-1] if '@' in DB_CONNECTION_STRING else DB_CONNECTION_STRING}")
    logging.info(f"Logging Level Set To: {logging.getLevelName(LOG_LEVEL)}")
    if LOG_LEVEL > logging.DEBUG:
        logging.info("Set LOG_LEVEL to logging.DEBUG for detailed tag matching/processing logs.")

    # --- Database Setup & Connection ---
    db_session = None
    try:
        engine = create_engine(DB_CONNECTION_STRING, pool_recycle=3600, pool_pre_ping=True, echo=False)
        logging.info("Ensuring database table 'sec_annual_data' schema...")
        Base.metadata.create_all(engine) # Should be safe, won't drop existing columns
        Session = sessionmaker(bind=engine)
        db_session = Session()
        logging.info("Database connection successful and schema ensured.")
    except Exception as e: logging.error(f"FATAL: Database connection/setup failed: {e}", exc_info=True); exit(1)

    # --- Fetch Company Ticker List ---
    logging.info(f"Fetching company CIK/ticker list from {COMPANY_TICKERS_URL}...")
    ticker_fetch_headers = { 'User-Agent': USER_AGENT }
    company_tickers_data = get_sec_data(COMPANY_TICKERS_URL, ticker_fetch_headers)
    time.sleep(max(REQUEST_DELAY, 0.5)) # Be nice after the list fetch
    if not company_tickers_data:
        logging.error("FATAL: Failed fetch company tickers list. Exiting.");
        if db_session and db_session.is_active: db_session.close()
        exit(1)

    # --- Process Ticker List ---
    all_companies = {}
    try:
        # Handle both dict and list formats SEC might return
        if isinstance(company_tickers_data, dict):
            all_companies = { str(info['cik_str']): {'title': info['title'], 'ticker': info['ticker']}
                              for _, info in company_tickers_data.items()
                              if isinstance(info, dict) and all(k in info for k in ['cik_str', 'title', 'ticker']) }
        elif isinstance(company_tickers_data, list):
             all_companies = { str(info['cik_str']): {'title': info['title'], 'ticker': info['ticker']}
                               for info in company_tickers_data
                               if isinstance(info, dict) and all(k in info for k in ['cik_str', 'title', 'ticker']) }
        else: raise TypeError(f"Unexpected tickers format: {type(company_tickers_data)}")
        logging.info(f"Processed {len(all_companies)} companies from ticker list.")
    except Exception as e: logging.error(f"FATAL: Error processing tickers data: {e}", exc_info=True); db_session.close(); exit(1)
    if not all_companies: logging.error("FATAL: Ticker list processed but empty. Exiting."); db_session.close(); exit(1)

    # --- Select Companies To Process ---
    companies_to_process = []
    if target_cik:
        if target_cik in all_companies:
            companies_to_process = [(target_cik, all_companies[target_cik])]
            logging.info(f"Found specified CIK {target_cik} in the ticker list.")
        else:
            logging.error(f"Specified CIK {target_cik} not found in the fetched ticker list. Exiting.")
            if db_session and db_session.is_active: db_session.close()
            exit(1)
    else:
        companies_to_process = list(all_companies.items())

    # --- Process Each Selected Company ---
    processed_count = 0; error_count = 0; no_facts_count = 0; db_commit_errors = 0; start_time = time.time()
    total_companies_to_process = len(companies_to_process)

    if total_companies_to_process <= 0:
        logging.warning("No companies selected for processing.")
        if db_session and db_session.is_active: db_session.close()
        exit(0)

    logging.info(f"Starting data fetching for {total_companies_to_process} selected company/companies...")
    api_headers = { 'User-Agent': USER_AGENT, 'Accept-Encoding': 'gzip, deflate', 'Host': 'data.sec.gov' }

    for cik_str, company_info in companies_to_process:
        try: cik = int(cik_str)
        except ValueError: logging.warning(f"Invalid CIK '{cik_str}'. Skipping."); processed_count += 1; continue
        ticker = company_info.get('ticker', 'N/A'); title_from_list = company_info.get('title', '').strip() or 'N/A'; processed_count += 1
        log_prefix = f"({processed_count}/{total_companies_to_process}) CIK {cik_str} ({ticker}):"

        # ETA Calculation
        if total_companies_to_process > 1 and processed_count > 10: # Show ETA after initial batch
            elapsed_time = time.time() - start_time; avg_time = elapsed_time / (processed_count -1) if processed_count > 1 else 0
            if avg_time > 0:
                eta_seconds = avg_time * (total_companies_to_process - processed_count)
                eta_str = time.strftime("%Hh %Mm %Ss", time.gmtime(eta_seconds))
                logging.info(f"{log_prefix} Processing '{title_from_list}'... (ETA: {eta_str})")
            else: logging.info(f"{log_prefix} Processing '{title_from_list}'...")
        else: logging.info(f"{log_prefix} Processing '{title_from_list}'...")

        # Fetch Company Facts
        facts_url = COMPANY_FACTS_URL_TEMPLATE.format(cik=cik_str.zfill(10))
        company_facts_json = get_sec_data(facts_url, api_headers)

        # Process Facts and Merge Data
        if company_facts_json:
            # --- Calls the MODIFIED process_company_facts ---
            annual_data_by_year, entity_name_from_facts = process_company_facts(cik, company_facts_json)
            effective_company_name = entity_name_from_facts.strip() if entity_name_from_facts and entity_name_from_facts.strip() and entity_name_from_facts != 'N/A' else title_from_list

            if annual_data_by_year:
                years_found = len(annual_data_by_year); years_merged_count = 0
                logging.info(f"{log_prefix} Found {years_found} year(s) data for '{effective_company_name}'. Merging...")

                # --- Database Merging Loop (No changes needed here) ---
                for year, data_for_year in sorted(annual_data_by_year.items()):
                    try:
                        # Calculate FCF
                        ocf = data_for_year.get('operating_cash_flow')
                        capex = data_for_year.get('capital_expenditures')
                        calculated_fcf = ocf - capex if ocf is not None and capex is not None else None

                        # Prepare data dictionary for ORM object
                        record_data_for_orm = {
                            'ticker': ticker,
                            'company_name': effective_company_name,
                            'calculated_free_cash_flow': calculated_fcf,
                            **data_for_year # Unpack all snake_case keyed financial data
                        }
                        # Remove keys already handled explicitly or not columns
                        record_data_for_orm.pop('cik', None)
                        record_data_for_orm.pop('year', None)
                        record_data_for_orm.pop('val', None) # Should not be present, but safety

                        # Create/Update DB record
                        record_obj = AnnualData(cik=cik, year=year, **record_data_for_orm)
                        db_session.merge(record_obj)
                        years_merged_count += 1

                        # Periodic commit within a large company's data
                        if years_merged_count > 0 and years_merged_count % 50 == 0:
                            logging.debug(f"{log_prefix} Committing batch ({years_merged_count}/{years_found} years)...")
                            db_session.commit()

                    except Exception as e:
                        logging.error(f"{log_prefix} DB merge error Year {year}: {type(e).__name__} - {e}", exc_info=False)
                        error_count += 1
                        db_session.rollback() # Rollback the transaction for this year

                # Final commit for the current CIK after processing all its years
                try:
                    db_session.commit()
                    if years_merged_count > 0: logging.info(f"{log_prefix} Merged data for {years_merged_count} year(s).")
                except Exception as e:
                    logging.error(f"{log_prefix} Final commit error for CIK: {type(e).__name__} - {e}", exc_info=False)
                    db_commit_errors += 1
                    db_session.rollback() # Rollback commit attempt
            else:
                logging.info(f"{log_prefix} No relevant annual (FY) data extracted for '{effective_company_name}'. Check DESIRED_TAGS/company reporting.")
                no_facts_count += 1
        else:
            logging.info(f"{log_prefix} No facts data retrieved for CIK {cik_str} ('{title_from_list}').")
            no_facts_count += 1

        # Rate limit pause between companies
        time.sleep(REQUEST_DELAY)

    # --- Final Summary ---
    end_time = time.time(); total_duration_str = time.strftime("%Hh %Mm %Ss", time.gmtime(end_time - start_time))
    logging.info("="*60 + "\nProcessing Summary\n" + "="*60)
    logging.info(f"Finished processing {processed_count} selected CIK(s).")
    logging.info(f"Total execution time: {total_duration_str}.")
    logging.info(f"  - CIKs with no facts data retrieved: {no_facts_count}") # Adjusted wording
    logging.info(f"  - Database record merge errors (individual years): {error_count}")
    logging.info(f"  - Database final commit errors (per CIK): {db_commit_errors}")
    total_errors = error_count + db_commit_errors
    if total_errors == 0: logging.info("  No database errors encountered.")
    else: logging.warning(f"  Total database errors: {total_errors}")
    logging.info("="*60)

    # --- Cleanup ---
    if db_session and db_session.is_active:
        try:
            db_session.close()
            logging.info("Database session closed.")
        except Exception as e:
            logging.error(f"Error closing database session: {e}")

    logging.info("SEC Annual Financial Data Fetcher finished.")