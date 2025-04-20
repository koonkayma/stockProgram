-- Connect to your 'nextcloud' database first
-- USE nextcloud;

-- Drop the old table if it exists to avoid conflicts with column additions
DROP TABLE IF EXISTS simfin_financial_data;

-- Create the new, comprehensive table
CREATE TABLE simfin_financial_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    -- Identifiers & Metadata (Same as before)
    ticker VARCHAR(20) NOT NULL,
    simfin_id INT NOT NULL,
    company_name VARCHAR(255),
    fiscal_year INT NOT NULL,
    fiscal_period VARCHAR(10) NOT NULL, -- e.g., 'FY', 'Q1', 'Q2', 'Q3', 'Q4', 'TTM'
    report_date DATE NULL,
    publish_date DATE NULL,
    currency VARCHAR(5) NULL,

    -- Income Statement (PL) Fields
    revenue DECIMAL(20, 2) NULL,
    cost_of_revenue DECIMAL(20, 2) NULL,
    gross_profit DECIMAL(20, 2) NULL,
    research_development DECIMAL(20, 2) NULL,
    selling_general_administrative DECIMAL(20, 2) NULL,
    other_operating_expenses DECIMAL(20, 2) NULL,
    operating_expenses DECIMAL(20, 2) NULL,
    operating_income_loss DECIMAL(20, 2) NULL, -- EBIT
    non_operating_income_loss DECIMAL(20, 2) NULL,
    interest_expense_net DECIMAL(20, 2) NULL,
    pretax_income_loss DECIMAL(20, 2) NULL,
    income_tax_expense_benefit DECIMAL(20, 2) NULL,
    net_income_loss DECIMAL(20, 2) NULL,
    net_income_common DECIMAL(20, 2) NULL,
    eps_basic DECIMAL(10, 4) NULL, -- Using more precision for EPS
    eps_diluted DECIMAL(10, 4) NULL,
    shares_basic DECIMAL(20, 0) NULL, -- Shares are whole numbers
    shares_diluted DECIMAL(20, 0) NULL,

    -- Balance Sheet (BS) Fields - Assets
    cash_and_equivalents DECIMAL(20, 2) NULL,
    short_term_investments DECIMAL(20, 2) NULL,
    accounts_receivable DECIMAL(20, 2) NULL,
    inventories DECIMAL(20, 2) NULL,
    total_current_assets DECIMAL(20, 2) NULL,
    property_plant_equipment_net DECIMAL(20, 2) NULL,
    long_term_investments DECIMAL(20, 2) NULL,
    goodwill_intangible_assets DECIMAL(20, 2) NULL,
    total_non_current_assets DECIMAL(20, 2) NULL,
    total_assets DECIMAL(20, 2) NULL,

    -- Balance Sheet (BS) Fields - Liabilities
    accounts_payable DECIMAL(20, 2) NULL,
    short_term_debt DECIMAL(20, 2) NULL,
    accrued_liabilities DECIMAL(20, 2) NULL,
    deferred_revenue_current DECIMAL(20, 2) NULL,
    total_current_liabilities DECIMAL(20, 2) NULL,
    long_term_debt DECIMAL(20, 2) NULL,
    deferred_revenue_non_current DECIMAL(20, 2) NULL,
    other_non_current_liabilities DECIMAL(20, 2) NULL,
    total_non_current_liabilities DECIMAL(20, 2) NULL,
    total_liabilities DECIMAL(20, 2) NULL,

    -- Balance Sheet (BS) Fields - Equity
    common_stock DECIMAL(20, 2) NULL,
    retained_earnings DECIMAL(20, 2) NULL,
    accumulated_other_comprehensive_income DECIMAL(20, 2) NULL,
    total_equity DECIMAL(20, 2) NULL,
    total_liabilities_equity DECIMAL(20, 2) NULL,

    -- Cash Flow (CF) Fields (Expanded)
    cf_net_income DECIMAL(20, 2) NULL, -- Net income as starting point in CF
    depreciation_amortization DECIMAL(20, 2) NULL,
    stock_based_compensation DECIMAL(20, 2) NULL,
    -- Skipping detailed working capital changes for brevity, focusing on totals
    cash_from_operations DECIMAL(20, 2) NULL, -- Still keep the total CFO
    capital_expenditures DECIMAL(20, 2) NULL, -- Still keep CapEx
    net_change_investments DECIMAL(20, 2) NULL,
    cash_acquisitions_divestitures DECIMAL(20, 2) NULL,
    cash_from_investing DECIMAL(20, 2) NULL, -- Total CFI
    net_change_debt DECIMAL(20, 2) NULL,
    repurchase_common_stock DECIMAL(20, 2) NULL,
    issuance_common_stock DECIMAL(20, 2) NULL,
    dividend_payments DECIMAL(20, 2) NULL,
    cash_from_financing DECIMAL(20, 2) NULL, -- Total CFF
    effect_exchange_rate_cash DECIMAL(20, 2) NULL,
    net_change_cash DECIMAL(20, 2) NULL,
    cash_begin_period DECIMAL(20, 2) NULL,
    cash_end_period DECIMAL(20, 2) NULL,

    -- Calculated Field (Same as before)
    free_cash_flow DECIMAL(20, 2) NULL,

    -- Metadata (Same as before)
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    -- Ensure we don't insert the exact same period data twice
    UNIQUE KEY uk_company_period (simfin_id, fiscal_year, fiscal_period)
);

-- Add indexes for faster querying (Same as before)
ALTER TABLE simfin_financial_data ADD INDEX idx_ticker (ticker);
ALTER TABLE simfin_financial_data ADD INDEX idx_simfin_id (simfin_id);
ALTER TABLE simfin_financial_data ADD INDEX idx_fiscal_year (fiscal_year);
