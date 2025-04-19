-- SQL Example for MariaDB - Includes Dividends
CREATE TABLE IF NOT EXISTS nextcloud.sec_annual_data (
    cik INT UNSIGNED NOT NULL COMMENT 'Company Identifier',
    year INT NOT NULL COMMENT 'Fiscal Year',
    ticker VARCHAR(20) NULL COMMENT 'Trading Symbol at time of fetch',
    company_name VARCHAR(255) NULL COMMENT 'Company Name',
    form VARCHAR(10) NULL COMMENT 'Source Form (e.g., 10-K)',
    filed_date DATE NULL COMMENT 'Filing Date of the source form',
    period_end_date DATE NULL COMMENT 'Period End Date for the fiscal year',

    -- Balance Sheet Metrics --
    assets DECIMAL(28, 4) NULL COMMENT 'Assets (us-gaap:Assets)',
    liabilities DECIMAL(28, 4) NULL COMMENT 'Liabilities (us-gaap:Liabilities)',
    stockholders_equity DECIMAL(28, 4) NULL COMMENT 'StockholdersEquity (us-gaap:StockholdersEquity)',
    cash_and_equivalents DECIMAL(28, 4) NULL COMMENT 'CashAndEquivalents (us-gaap:CashAndCashEquivalentsAtCarryingValue)',
    accounts_receivable_net DECIMAL(28, 4) NULL COMMENT 'AccountsReceivableNet (us-gaap:AccountsReceivableNetCurrent)',
    inventory_net DECIMAL(28, 4) NULL COMMENT 'InventoryNet (us-gaap:InventoryNet)',
    property_plant_equipment_net DECIMAL(28, 4) NULL COMMENT 'PropertyPlantEquipmentNet (us-gaap:PropertyPlantAndEquipmentNet)',
    accumulated_depreciation DECIMAL(28, 4) NULL COMMENT 'AccumulatedDepreciation (us-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment)',
    accounts_payable DECIMAL(28, 4) NULL COMMENT 'AccountsPayable (us-gaap:AccountsPayableCurrent)',
    accrued_liabilities_current DECIMAL(28, 4) NULL COMMENT 'AccruedLiabilitiesCurrent (us-gaap:AccruedLiabilitiesCurrent)',
    debt_current DECIMAL(28, 4) NULL COMMENT 'DebtCurrent (us-gaap:DebtCurrent)',
    long_term_debt_noncurrent DECIMAL(28, 4) NULL COMMENT 'LongTermDebtNoncurrent (us-gaap:LongTermDebtNoncurrent)',

    -- Income Statement Metrics --
    revenues DECIMAL(28, 4) NULL COMMENT 'Revenues (us-gaap:Revenues)',
    cost_of_revenue DECIMAL(28, 4) NULL COMMENT 'CostOfRevenue (us-gaap:CostOfRevenue)',
    gross_profit DECIMAL(28, 4) NULL COMMENT 'GrossProfit (us-gaap:GrossProfit)',
    operating_expenses DECIMAL(28, 4) NULL COMMENT 'OperatingExpenses (us-gaap:OperatingExpenses)',
    operating_income_loss DECIMAL(28, 4) NULL COMMENT 'OperatingIncomeLoss (us-gaap:OperatingIncomeLoss)',
    interest_expense DECIMAL(28, 4) NULL COMMENT 'InterestExpense (us-gaap:InterestExpense)',
    income_before_tax DECIMAL(28, 4) NULL COMMENT 'IncomeBeforeTax (us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxExtraordinaryItemsNoncontrollingInterest)',
    income_tax_expense_benefit DECIMAL(28, 4) NULL COMMENT 'IncomeTaxExpenseBenefit (us-gaap:IncomeTaxExpenseBenefit)',
    net_income_loss DECIMAL(28, 4) NULL COMMENT 'NetIncomeLoss (us-gaap:NetIncomeLoss)',

    -- EPS & Share Metrics --
    eps_basic DECIMAL(18, 6) NULL COMMENT 'EPSBasic (us-gaap:EarningsPerShareBasic)',
    eps_diluted DECIMAL(18, 6) NULL COMMENT 'EPSDiluted (us-gaap:EarningsPerShareDiluted)',
    shares_outstanding BIGINT UNSIGNED NULL COMMENT 'SharesOutstanding (dei:EntityCommonStockSharesOutstanding)',
    shares_basic_weighted_avg BIGINT UNSIGNED NULL COMMENT 'SharesBasicWeightedAvg (us-gaap:WeightedAverageNumberOfSharesOutstandingBasic)',
    shares_diluted_weighted_avg BIGINT UNSIGNED NULL COMMENT 'SharesDilutedWeightedAvg (us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding)',

    -- Cash Flow Statement Metrics --
    operating_cash_flow DECIMAL(28, 4) NULL COMMENT 'OperatingCashFlow (us-gaap:NetCashProvidedByUsedInOperatingActivities)',
    capital_expenditures DECIMAL(28, 4) NULL COMMENT 'CapitalExpenditures (us-gaap:PaymentsToAcquirePropertyPlantAndEquipment)',
    depreciation_and_amortization DECIMAL(28, 4) NULL COMMENT 'DepreciationAndAmortization (us-gaap:DepreciationAndAmortization)',
    dividends_paid DECIMAL(28, 4) NULL COMMENT 'DividendsPaid (us-gaap:PaymentsOfDividends)',

    -- Metadata --
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (cik, year),
    INDEX idx_sec_annual_ticker (ticker),
    INDEX idx_sec_annual_year (year)
) COMMENT 'Stores selected annual financial data derived from SEC API';
