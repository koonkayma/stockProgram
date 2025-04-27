-- File: create_edgar_table.sql
-- Creates the table to store annual report metadata and EXTENDED extracted financial data
-- in the 'nextcloud' database.

-- Ensure you are using the correct database
-- USE nextcloud;

-- Drop the table if it exists from a previous run (optional, use with caution)
-- DROP TABLE IF EXISTS edgartools_annual_reports;

CREATE TABLE IF NOT EXISTS edgartools_annual_reports (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cik VARCHAR(10) NOT NULL COMMENT 'Central Index Key of the company',
    company_name VARCHAR(255) COMMENT 'Name of the company',
    form_type VARCHAR(20) NOT NULL COMMENT 'Type of the filing (e.g., 10-K, 20-F, 10-K/A)', -- Increased size for amendments
    filing_date DATE NOT NULL COMMENT 'Date the filing was submitted to SEC',
    period_of_report DATE COMMENT 'End date of the fiscal period covered by the report',
    accession_number VARCHAR(50) NOT NULL UNIQUE COMMENT 'Unique identifier for the specific filing instance',
    filing_url VARCHAR(512) COMMENT 'URL to the filing index page',
    filing_html_url VARCHAR(512) NULL COMMENT 'Direct URL to the primary HTML document, if available',

    -- == Expanded Financial Data Columns (Allow NULLs) ==

    -- Income Statement
    revenue DECIMAL(22, 2) NULL COMMENT 'Revenues (us-gaap:Revenues, etc.)',
    cost_of_revenue DECIMAL(22, 2) NULL COMMENT 'Cost of Revenue (us-gaap:CostOfRevenue, CostOfGoodsAndServicesSold)',
    gross_profit DECIMAL(22, 2) NULL COMMENT 'Gross Profit (us-gaap:GrossProfit)',
    research_development_expense DECIMAL(22, 2) NULL COMMENT 'R&D Expense (us-gaap:ResearchAndDevelopmentExpense)',
    sga_expense DECIMAL(22, 2) NULL COMMENT 'Selling, General & Admin Expense (us-gaap:SellingGeneralAndAdministrativeExpense)',
    operating_income_loss DECIMAL(22, 2) NULL COMMENT 'Operating Income/Loss (us-gaap:OperatingIncomeLoss)',
    interest_expense DECIMAL(22, 2) NULL COMMENT 'Interest Expense (us-gaap:InterestExpense)',
    income_before_tax DECIMAL(22, 2) NULL COMMENT 'Income/Loss Before Tax (us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTax)',
    income_tax_expense_benefit DECIMAL(22, 2) NULL COMMENT 'Income Tax Expense/Benefit (us-gaap:IncomeTaxExpenseBenefit)',
    net_income_loss DECIMAL(22, 2) NULL COMMENT 'Net Income/Loss (us-gaap:NetIncomeLoss, ProfitLoss)',

    -- Per Share Data
    eps_basic DECIMAL(10, 2) NULL COMMENT 'Basic EPS (us-gaap:EarningsPerShareBasic)',
    eps_diluted DECIMAL(10, 2) NULL COMMENT 'Diluted EPS (us-gaap:EarningsPerShareDiluted)',
    weighted_avg_shares_basic BIGINT NULL COMMENT 'Weighted Avg Shares Outstanding, Basic (us-gaap:WeightedAverageNumberOfSharesOutstandingBasic)',
    weighted_avg_shares_diluted BIGINT NULL COMMENT 'Weighted Avg Shares Outstanding, Diluted (us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding)',

    -- Balance Sheet - Assets
    cash_and_equivalents DECIMAL(22, 2) NULL COMMENT 'Cash and Cash Equivalents (us-gaap:CashAndCashEquivalentsAtCarryingValue)',
    short_term_investments DECIMAL(22, 2) NULL COMMENT 'Short-term Investments (us-gaap:ShortTermInvestments, MarketableSecuritiesCurrent)',
    accounts_receivable DECIMAL(22, 2) NULL COMMENT 'Accounts Receivable, net (us-gaap:AccountsReceivableNetCurrent)',
    inventory DECIMAL(22, 2) NULL COMMENT 'Inventory, net (us-gaap:InventoryNet)',
    total_current_assets DECIMAL(22, 2) NULL COMMENT 'Total Current Assets (us-gaap:AssetsCurrent)',
    property_plant_equipment_net DECIMAL(22, 2) NULL COMMENT 'Property, Plant & Equipment, net (us-gaap:PropertyPlantAndEquipmentNet)',
    goodwill DECIMAL(22, 2) NULL COMMENT 'Goodwill (us-gaap:Goodwill)',
    intangible_assets_net DECIMAL(22, 2) NULL COMMENT 'Intangible Assets, net (us-gaap:IntangibleAssetsNetExcludingGoodwill)',
    total_noncurrent_assets DECIMAL(22, 2) NULL COMMENT 'Total Noncurrent Assets (us-gaap:AssetsNoncurrent)',
    total_assets BIGINT NULL COMMENT 'Total Assets (us-gaap:Assets)',

    -- Balance Sheet - Liabilities & Equity
    accounts_payable DECIMAL(22, 2) NULL COMMENT 'Accounts Payable (us-gaap:AccountsPayableCurrent)',
    short_term_debt DECIMAL(22, 2) NULL COMMENT 'Short-term Debt (us-gaap:ShortTermBorrowings, DebtCurrent)',
    deferred_revenue_current DECIMAL(22, 2) NULL COMMENT 'Deferred Revenue, Current (us-gaap:DeferredRevenueCurrent)',
    total_current_liabilities DECIMAL(22, 2) NULL COMMENT 'Total Current Liabilities (us-gaap:LiabilitiesCurrent)',
    long_term_debt DECIMAL(22, 2) NULL COMMENT 'Long-term Debt (us-gaap:LongTermDebtNoncurrent)',
    total_noncurrent_liabilities DECIMAL(22, 2) NULL COMMENT 'Total Noncurrent Liabilities (us-gaap:LiabilitiesNoncurrent)',
    total_liabilities BIGINT NULL COMMENT 'Total Liabilities (us-gaap:Liabilities)',
    common_stock_value DECIMAL(22, 2) NULL COMMENT 'Common Stock Value (us-gaap:CommonStockValue)',
    retained_earnings DECIMAL(22, 2) NULL COMMENT 'Retained Earnings (us-gaap:RetainedEarningsAccumulatedDeficit)',
    total_stockholders_equity BIGINT NULL COMMENT 'Total Stockholders Equity (us-gaap:StockholdersEquity)',
    total_liabilities_and_equity BIGINT NULL COMMENT 'Total Liabilities and Stockholders Equity (us-gaap:LiabilitiesAndStockholdersEquity)',

    -- Cash Flow Statement
    cf_net_cash_operating DECIMAL(22, 2) NULL COMMENT 'Net Cash Flow from Operations (us-gaap:NetCashProvidedByUsedInOperatingActivities)',
    cf_capital_expenditures DECIMAL(22, 2) NULL COMMENT 'Capital Expenditures (us-gaap:PaymentsToAcquirePropertyPlantAndEquipment)', -- Often negative in XBRL
    cf_net_cash_investing DECIMAL(22, 2) NULL COMMENT 'Net Cash Flow from Investing (us-gaap:NetCashProvidedByUsedInInvestingActivities)',
    cf_net_cash_financing DECIMAL(22, 2) NULL COMMENT 'Net Cash Flow from Financing (us-gaap:NetCashProvidedByUsedInFinancingActivities)',
    cf_net_change_in_cash DECIMAL(22, 2) NULL COMMENT 'Net Change in Cash (us-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease)',

    -- Status & Timestamps
    extraction_status VARCHAR(50) DEFAULT 'Metadata Only' COMMENT 'Status of financial data extraction',
    metadata_imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when metadata was inserted',
    financials_extracted_at TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp when financial data was last updated',

    INDEX cik_idx (cik),
    INDEX filing_date_idx (filing_date),
    INDEX period_idx (period_of_report),
    INDEX form_type_idx (form_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Stores metadata and expanded financial data from SEC annual filings.';
