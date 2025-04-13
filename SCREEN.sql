CREATE TABLE IF NOT EXISTS nextcloud.company_financial_periods (
    -- Core Identification & Period Info (Primary Key Candidate)
    cik INT UNSIGNED NOT NULL COMMENT 'Company Identifier (Central Index Key)',
    -- Using period_end_date as part of the key is often more reliable than fy/fp
    period_end_date DATE NOT NULL COMMENT 'The actual end date of the fiscal period (from ddate/period)',
    -- Fiscal Year/Period are useful for filtering/grouping but less reliable as unique keys
    fiscal_year INT NULL COMMENT 'Reported Fiscal Year (from sub.txt fy)',
    fiscal_period VARCHAR(2) NULL COMMENT 'Reported Fiscal Period (Q1, Q2, Q3, Q4, FY - from sub.txt fp)',
    -- Filing Info (Useful for Traceability)
    form_type VARCHAR(10) NULL COMMENT 'Type of filing (e.g., 10-K, 10-Q from sub.txt form)',
    filing_date DATE NULL COMMENT 'Date the filing was submitted (from sub.txt filed)',
    -- Use adsh if you need absolute uniqueness per *filing* instance
    source_adsh VARCHAR(20) NOT NULL COMMENT 'Accession number of the source filing',

    -- Pivoted Financial Data Columns (Based on Common Tags - ADD MORE AS NEEDED)
    -- Income Statement related
    revenue DECIMAL(28, 4) NULL,
    cost_of_revenue DECIMAL(28, 4) NULL,
    gross_profit DECIMAL(28, 4) NULL,
    operating_income_loss DECIMAL(28, 4) NULL,
    interest_expense DECIMAL(28, 4) NULL,
    income_loss_before_tax DECIMAL(28, 4) NULL,
    net_income_loss DECIMAL(28, 4) NULL,
    depreciation_amortization DECIMAL(28, 4) NULL COMMENT 'Often DepreciationDepletionAndAmortization',
    ebitda DECIMAL(28, 4) NULL COMMENT 'Often calculated or from tag EBITDA',
    eps_basic DECIMAL(10, 4) NULL,
    eps_diluted DECIMAL(10, 4) NULL,

    -- Balance Sheet related
    assets DECIMAL(28, 4) NULL COMMENT 'Usually from Assets tag',
    current_assets DECIMAL(28, 4) NULL,
    liabilities DECIMAL(28, 4) NULL COMMENT 'Usually from Liabilities tag',
    current_liabilities DECIMAL(28, 4) NULL,
    accounts_payable_current DECIMAL(28, 4) NULL, -- Requested
    total_debt DECIMAL(28, 4) NULL COMMENT 'May need calculation (ShortTermDebt + LongTermDebt)',
    total_equity DECIMAL(28, 4) NULL,
    cash_and_cash_equivalents DECIMAL(28, 4) NULL,

    -- Cash Flow related
    net_cash_ops DECIMAL(28, 4) NULL COMMENT 'NetCashProvidedByUsedInOperatingActivities',
    net_cash_investing DECIMAL(28, 4) NULL,
    net_cash_financing DECIMAL(28, 4) NULL,
    capex DECIMAL(28, 4) NULL COMMENT 'Capital Expenditures tag (e.g., PaymentsToAcquirePropertyPlantAndEquipment)',
    dividends_paid DECIMAL(28, 4) NULL,
    -- Add FCF from tag if available from FMP, but focus on calculated one
    -- free_cash_flow_reported DECIMAL(28, 4) NULL,

    -- Calculated Field(s)
    calculated_fcf DECIMAL(28, 4) NULL COMMENT 'Calculated as net_cash_ops +/- capex',

    -- Other Metadata
    currency_reported VARCHAR(10) NULL,

    -- Timestamps
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when this row was created',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp when this row was last updated',

    -- Constraints
    PRIMARY KEY (cik, period_end_date, source_adsh), -- Ensures uniqueness per company per period end date from a specific filing
    INDEX idx_cfp_cik_period (cik, period_end_date DESC),
    INDEX idx_cfp_cik_fy_fp (cik, fiscal_year, fiscal_period),
    INDEX idx_cfp_updated_at (updated_at)

) COMMENT 'Stores pivoted annual or quarterly financial data derived from SEC filings';

-- Drop table if it exists (optional, use with caution)
-- DROP TABLE IF EXISTS nextcloud.company_financial_periods;



-- nextcloud.company_financial_periods definition

CREATE TABLE `company_financial_periods` (
  `cik` int(10) unsigned NOT NULL COMMENT 'Company Identifier',
  `period_end_date` date NOT NULL COMMENT 'End date of the fiscal period (from ddate)',
  `period_duration_qtrs` int(11) NOT NULL COMMENT 'Duration in quarters (1=Q, 4=Annual)',
  `fiscal_year` int(11) DEFAULT NULL COMMENT 'Fiscal Year reported by company (from sub.fy)',
  `fiscal_period` varchar(2) DEFAULT NULL COMMENT 'Fiscal Period (Q1, Q2, Q3, FY) (from sub.fp)',
  `adsh` varchar(20) DEFAULT NULL COMMENT 'Accession number of the primary source filing for this period',
  `form_type` varchar(10) DEFAULT NULL COMMENT 'Form type of the source filing (e.g., 10-K, 10-Q)',
  `revenue` decimal(22,2) DEFAULT NULL,
  `cost_of_revenue` decimal(22,2) DEFAULT NULL,
  `gross_profit` decimal(22,2) DEFAULT NULL,
  `research_and_development_expense` decimal(22,2) DEFAULT NULL,
  `selling_general_and_administrative_expense` decimal(22,2) DEFAULT NULL,
  `operating_income_loss` decimal(22,2) DEFAULT NULL COMMENT 'Often EBIT',
  `interest_expense` decimal(22,2) DEFAULT NULL,
  `income_tax_expense_benefit` decimal(22,2) DEFAULT NULL,
  `net_income_loss` decimal(22,2) DEFAULT NULL,
  `eps_basic` decimal(10,4) DEFAULT NULL,
  `eps_diluted` decimal(10,4) DEFAULT NULL,
  `ebitda` decimal(22,2) DEFAULT NULL,
  `cash_and_cash_equivalents` decimal(22,2) DEFAULT NULL,
  `accounts_receivable_net_current` decimal(22,2) DEFAULT NULL,
  `inventory_net` decimal(22,2) DEFAULT NULL,
  `total_current_assets` decimal(22,2) DEFAULT NULL,
  `property_plant_and_equipment_net` decimal(22,2) DEFAULT NULL,
  `total_assets` decimal(22,2) DEFAULT NULL,
  `accounts_payable_current` decimal(22,2) DEFAULT NULL,
  `short_term_debt` decimal(22,2) DEFAULT NULL,
  `total_current_liabilities` decimal(22,2) DEFAULT NULL,
  `long_term_debt` decimal(22,2) DEFAULT NULL,
  `total_liabilities` decimal(22,2) DEFAULT NULL,
  `total_stockholders_equity` decimal(22,2) DEFAULT NULL,
  `net_cash_provided_by_used_in_operating_activities` decimal(22,2) DEFAULT NULL,
  `depreciation_and_amortization` decimal(22,2) DEFAULT NULL,
  `capital_expenditure` decimal(22,2) DEFAULT NULL COMMENT 'Store the value as reported (often negative)',
  `net_cash_provided_by_used_in_investing_activities` decimal(22,2) DEFAULT NULL,
  `net_cash_provided_by_used_in_financing_activities` decimal(22,2) DEFAULT NULL,
  `dividends_paid` decimal(22,2) DEFAULT NULL,
  `free_cash_flow` decimal(22,2) DEFAULT NULL,
  `calculated_fcf` decimal(28,4) DEFAULT NULL COMMENT 'FCF calculated by transformation script (e.g., CFO + CapEx)',
  `currency` varchar(10) DEFAULT NULL,
  `source_api` varchar(50) DEFAULT 'SEC_XBRL_Dataset',
  `created_at` timestamp NOT NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`cik`,`period_end_date`,`period_duration_qtrs`),
  KEY `idx_cfp_cik_period` (`cik`,`period_end_date` DESC),
  KEY `idx_cfp_adsh` (`adsh`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='Pivoted time-series financial data per company per period';

-- Create table to define unique reporting periods for companies
CREATE TABLE IF NOT EXISTS nextcloud.company_financial_periods (
    `period_id` INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'Unique identifier for the financial period',
    `ticker` VARCHAR(20) NOT NULL COMMENT 'Stock ticker symbol',
    `fiscal_year` INT NOT NULL COMMENT 'The fiscal year the data represents (e.g., 2023)',
    `fiscal_period` VARCHAR(10) NOT NULL DEFAULT 'FY' COMMENT 'Period type (e.g., FY for Fiscal Year, Q1, Q2 etc.) - Assuming Annual here',
    `calendar_year` INT NULL COMMENT 'Calendar year the period primarily falls into (useful for grouping)',
    `period_end_date` DATE NULL COMMENT 'The exact end date of the fiscal period',
    `filing_date` DATE NULL COMMENT 'Date the corresponding report (e.g., 10-K) was filed with SEC',
    `currency` VARCHAR(10) NULL COMMENT 'Reporting currency for this period (e.g., USD)',
    `source_api` VARCHAR(50) NULL COMMENT 'API source used for fetching this period info (e.g., FMP_Direct)',
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when this period record was first created',
    `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp when this period record was last updated',

    PRIMARY KEY (`period_id`),
    UNIQUE KEY `uq_company_period` (`ticker`, `fiscal_year`, `fiscal_period`) COMMENT 'Ensure only one record per company/year/period type',
    INDEX `idx_ticker_fy` (`ticker`, `fiscal_year`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Defines distinct financial reporting periods for companies.';

CREATE TABLE IF NOT EXISTS nextcloud.company_cik_map (
    cik INT UNSIGNED PRIMARY KEY NOT NULL COMMENT 'Central Index Key (Primary Key)',
    ticker VARCHAR(20) NULL COMMENT 'Stock Ticker Symbol (May be NULL or duplicated for different CIKs/classes)',
    company_name VARCHAR(255) NULL COMMENT 'Company Name/Title from SEC filing',
    -- Metadata
    source_url VARCHAR(512) NULL COMMENT 'URL from where the data was last downloaded',
    downloaded_at TIMESTAMP NULL COMMENT 'Timestamp when the data was downloaded',
    imported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when this record was inserted/updated'
    -- Removed updated_at as CIK is PK, we replace/ignore on conflict typically

    -- Indices
    -- INDEX idx_ccm_ticker (ticker) -- Add if you frequently query by ticker
) COMMENT 'Maps SEC CIK to Ticker and Company Name';

-- Optional: Add index on ticker AFTER populating if needed for lookups
-- CREATE INDEX idx_ccm_ticker ON nextcloud.company_cik_map (ticker);

-- Recreate the table allowing NULL for coreg and removing it from PK
CREATE TABLE IF NOT EXISTS nextcloud.sec_numeric_data (
    -- Linking Fields
    adsh VARCHAR(20) NOT NULL COMMENT 'Accession Number (Link to Submission)',
    tag VARCHAR(256) NOT NULL COMMENT 'XBRL Tag for the financial concept',
    version VARCHAR(20) NOT NULL COMMENT 'XBRL Taxonomy Version (e.g., us-gaap/2023)',
    ddate DATE NOT NULL COMMENT 'Date the value pertains to (Period End Date)',
    qtrs INT NOT NULL COMMENT 'Duration in quarters (0=instant, 1=Q, 4=Annual)',
    uom VARCHAR(20) NOT NULL COMMENT 'Unit of Measure (e.g., USD, shares)',

    -- Core Value
    value DECIMAL(28, 4) NULL COMMENT 'The reported numeric value',

    -- Additional Context from num.txt
    -- *** Ensure coreg allows NULL (default if not PK) ***
    coreg VARCHAR(256) NULL COMMENT 'Coregistrant, if applicable',
    footnote TEXT NULL COMMENT 'XBRL footnote associated with the value',

    -- Context from sub.txt
    cik INT UNSIGNED NOT NULL COMMENT 'Company Identifier',
    form VARCHAR(10) NULL COMMENT 'Filing type (e.g., 10-K, 10-Q)',
    period DATE NULL COMMENT 'Period end date from submission file',
    fy INT NULL COMMENT 'Fiscal Year from submission file',
    fp VARCHAR(2) NULL COMMENT 'Fiscal Period (FY, Q1, etc.) from submission file',

    -- Import Metadata
    imported_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when this specific fact row was first inserted',
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp when this specific fact row was last updated or confirmed',

    -- *** MODIFIED PRIMARY KEY (coreg removed) ***
    PRIMARY KEY (adsh, tag, version, ddate, qtrs, uom),

    -- Indices for Querying (keep others, maybe add unique index later if needed)
    INDEX idx_sec_cik_tag_date (cik, tag, ddate),
    INDEX idx_sec_adsh (adsh),
    INDEX idx_sec_tag_date (tag, ddate),
    INDEX idx_sec_updated_at (updated_at)
    -- Optional: Add back a UNIQUE index if needed for non-null coreg cases
    -- UNIQUE INDEX idx_sec_natural_key_unique (adsh, tag, version, ddate, qtrs, uom, coreg(100)),

) COMMENT 'Stores individual numeric facts from SEC XBRL filings';


-- Please write SQL on table company_financial_periods for below purpose
-- -at least 3 years positive ebitda in last 5 years's data and
-- -at least 15 % growth in ebitda compare with 5 years ago 
-- -at least 3 years in last 5 years's data with positive Free Cash Flow
-- - map with company_cik_map to get the ticker and company name but include the recode even not in company_cik_map

-- Define criteria (use user variables for flexibility)
SET @min_positive_ebitda_years = 3;
SET @min_positive_fcf_years = 3;
SET @min_ebitda_growth_pct = 15.0;
SET @screening_years = 5;
-- Determine the latest full fiscal year available for screening (e.g., based on current date)
-- Adjust this as needed, e.g., YEAR(CURDATE()) - 1 if data isn't available for the current partial year yet.
SET @latest_screening_year = (SELECT MAX(fiscal_year) FROM company_financial_periods WHERE period_duration_qtrs = 4);
SET @start_screening_year = @latest_screening_year - @screening_years + 1; -- e.g., if latest is 2023, start is 2019 for 5 years


WITH RecentAnnualData AS (
    -- Select only annual data for the relevant CIKs and recent years needed for calculation
    SELECT
        cik,
        fiscal_year,
        period_end_date,
        ebitda,
        calculated_fcf -- Or use: COALESCE(free_cash_flow, calculated_fcf) if you want to prefer reported FCF
    FROM
        nextcloud.company_financial_periods
    WHERE
        period_duration_qtrs = 4 -- Ensure we only look at annual records (10-K, 20-F etc.)
        -- Select data from the start year needed for the 5-year window up to the latest screening year
        AND fiscal_year BETWEEN (@start_screening_year - 1) AND @latest_screening_year
        -- We need year N-5's data for CAGR calc ending in year N-1, so fetch back to start_year-1
        -- Example: Screen for 2019-2023 (@latest=2023, @start=2019), need 2018 data for 2023 CAGR calc.
),
WindowCalculations AS (
    -- Use window functions to calculate counts and look back for growth
    SELECT
        cik,
        fiscal_year,
        period_end_date,
        ebitda,
        calculated_fcf,
        -- Count positive EBITDA over the last N years (including current)
        SUM(CASE WHEN ebitda > 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY cik ORDER BY fiscal_year
            ROWS BETWEEN (@screening_years - 1) PRECEDING AND CURRENT ROW
        ) as positive_ebitda_count,
        -- Count positive FCF over the last N years (including current)
        SUM(CASE WHEN calculated_fcf > 0 THEN 1 ELSE 0 END) OVER (
            PARTITION BY cik ORDER BY fiscal_year
            ROWS BETWEEN (@screening_years - 1) PRECEDING AND CURRENT ROW
        ) as positive_fcf_count,
        -- Get the EBITDA from N-1 years ago (for N year period growth, requires N-1 lag)
        LAG(ebitda, @screening_years - 1) OVER (PARTITION BY cik ORDER BY fiscal_year) as ebitda_start_period,
        -- Count number of data points in the window to ensure full coverage for CAGR
        COUNT(*) OVER (PARTITION BY cik ORDER BY fiscal_year ROWS BETWEEN (@screening_years - 1) PRECEDING AND CURRENT ROW) as window_data_points
    FROM
        RecentAnnualData
    -- Filter only years within the actual screening window AFTER window functions can access prior years
    WHERE fiscal_year BETWEEN @start_screening_year AND @latest_screening_year
),
FinalScreen AS (
    -- Filter based on the calculated window values for the LATEST year in our range
    SELECT
        wc.cik,
        wc.fiscal_year,
        wc.ebitda,
        wc.calculated_fcf,
        wc.positive_ebitda_count,
        wc.positive_fcf_count,
        wc.ebitda_start_period,
        -- Calculate CAGR (handle potential division by zero and negative start)
        CASE
            -- Turnaround: Start <= 0, End > 0
            WHEN wc.ebitda_start_period <= 0 AND wc.ebitda > 0 THEN CAST('inf' AS DOUBLE) -- Represent infinite growth
            -- Normal Growth: Start > 0, End > 0
            WHEN wc.ebitda_start_period > 0 AND wc.ebitda > 0 THEN
                 -- Use POWER for fractional exponent; requires base > 0
                 (POWER(wc.ebitda / wc.ebitda_start_period, 1.0 / (@screening_years - 1)) - 1) * 100
            -- Decline or Stagnation from positive start: Start > 0, End <= 0 -> No positive growth
            -- Non-positive to Non-positive: Start <= 0, End <= 0 -> No positive growth
            ELSE NULL -- No meaningful positive growth or calculation possible
        END as ebitda_cagr,
        -- Flag for turnaround case
        CASE WHEN wc.ebitda_start_period <= 0 AND wc.ebitda > 0 THEN TRUE ELSE FALSE END as is_turnaround
    FROM
        WindowCalculations wc
    WHERE
        wc.fiscal_year = @latest_screening_year -- Select only the results for the final year of the window
        AND wc.window_data_points = @screening_years -- Ensure we had data for ALL years in the window for reliable counts/CAGR
        AND wc.positive_ebitda_count >= @min_positive_ebitda_years
        AND wc.positive_fcf_count >= @min_positive_fcf_years
        -- Apply growth condition (Turnaround OR CAGR >= threshold)
        AND (
                (wc.ebitda_start_period <= 0 AND wc.ebitda > 0) -- Is Turnaround
                OR
                ( -- Is Normal Growth Meeting Threshold
                  wc.ebitda_start_period > 0 AND wc.ebitda > 0 AND
                  (POWER(wc.ebitda / wc.ebitda_start_period, 1.0 / (@screening_years - 1)) - 1) * 100 >= @min_ebitda_growth_pct
                )
            )
)
-- Final Selection and Join with CIK Map
SELECT
    fs.cik,
    -- Use LEFT JOIN to include records even if ticker/name is missing
    COALESCE(ccm.ticker, 'N/A') as ticker,
    COALESCE(ccm.company_name, 'N/A') as company_name,
    fs.fiscal_year as screening_end_year,
    fs.positive_ebitda_count,
    fs.positive_fcf_count,
    fs.ebitda_cagr,
    fs.is_turnaround
FROM
    FinalScreen fs
LEFT JOIN
    nextcloud.company_cik_map ccm ON fs.cik = ccm.cik
ORDER BY
    -- Sort results meaningfully (e.g., turnarounds first, then by CAGR)
    fs.is_turnaround DESC,
    fs.ebitda_cagr DESC;