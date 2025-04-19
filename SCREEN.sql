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







WITH CompanyMaxYear AS (
    -- Find the latest fiscal year available for each company (using annual data)
    SELECT
        cik,
        MAX(fiscal_year) AS max_fy
    FROM company_financial_periods
    WHERE period_duration_qtrs = 4 -- Consider only annual data for determining latest year
      AND fiscal_year IS NOT NULL
    GROUP BY cik
),
PeriodData AS (
    -- Select the relevant annual data for the last 5 fiscal years for each company
    SELECT
        cfp.cik,
        cfp.fiscal_year,
        cfp.ebitda,
        cfp.calculated_fcf, -- Or use cfp.free_cash_flow if preferred
        cmy.max_fy
    FROM company_financial_periods cfp
    JOIN CompanyMaxYear cmy ON cfp.cik = cmy.cik
    WHERE cfp.period_duration_qtrs = 4
      -- Filter for the 5-year window relative to each company's max_fy
      AND cfp.fiscal_year BETWEEN (cmy.max_fy - 4) AND cmy.max_fy
),
CompanyStats AS (
    -- Calculate the required statistics for each company over its relevant 5-year window
    SELECT
        p.cik,
        p.max_fy,
        -- Count distinct years with positive EBITDA within the 5-year window
        COUNT(DISTINCT CASE WHEN p.ebitda > 0 THEN p.fiscal_year END) AS positive_ebitda_years_count,
        -- Count distinct years with positive Free Cash Flow within the 5-year window
        COUNT(DISTINCT CASE WHEN p.calculated_fcf > 0 THEN p.fiscal_year END) AS positive_fcf_years_count,
        -- Get EBITDA for the latest year in the window (max_fy)
        MAX(CASE WHEN p.fiscal_year = p.max_fy THEN p.ebitda END) AS current_ebitda,
        -- Get EBITDA for 5 years ago (max_fy - 4)
        MAX(CASE WHEN p.fiscal_year = (p.max_fy - 4) THEN p.ebitda END) AS ebitda_5yr_ago
    FROM PeriodData p
    GROUP BY p.cik, p.max_fy
),
QualifiedCIKs AS (
    -- Filter companies based on the calculated statistics
    SELECT
        cs.cik,
        cs.max_fy,
        cs.positive_ebitda_years_count,
        cs.positive_fcf_years_count,
        cs.current_ebitda,
        cs.ebitda_5yr_ago
    FROM CompanyStats cs
    WHERE
        -- Condition 1: At least 3 years positive EBITDA
        cs.positive_ebitda_years_count >= 3
        -- Condition 2: At least 15% growth in EBITDA (Requires valid positive base 5yrs ago)
        AND cs.ebitda_5yr_ago IS NOT NULL    -- Ensure data existed 5 years ago
        AND cs.ebitda_5yr_ago > 0           -- Ensure base for growth calc is positive
        AND cs.current_ebitda IS NOT NULL   -- Ensure current year data exists
        AND cs.current_ebitda >= (cs.ebitda_5yr_ago * 1.15) -- The 15% growth check
        -- Condition 3: At least 3 years positive Free Cash Flow
        AND cs.positive_fcf_years_count >= 3
)
-- Final selection: Join qualified CIKs with the mapping table
SELECT
    q.cik,
    ccm.ticker,             -- Ticker from map (NULL if no match)
    ccm.company_name,       -- Company Name from map (NULL if no match)
    q.max_fy AS latest_fiscal_year,
    q.positive_ebitda_years_count,
    q.positive_fcf_years_count,
    q.current_ebitda,
    q.ebitda_5yr_ago,
    -- Use NULLIF to avoid division by zero if ebitda_5yr_ago is 0 (though filtered above)
    -- *** Changed alias name below ***
    ( (q.current_ebitda / NULLIF(q.ebitda_5yr_ago, 0)) - 1 ) * 100 AS ebitda_5yr_growth_pc
FROM QualifiedCIKs q
LEFT JOIN company_cik_map ccm ON q.cik = ccm.cik -- LEFT JOIN to include CIKs even if not in map
ORDER BY
    (ccm.ticker IS NULL) ASC, -- Compatible NULLS LAST for ticker
    ccm.ticker ASC,          -- Sort non-NULL tickers alphabetically
    q.cik ASC;               -- Then sort by CIK