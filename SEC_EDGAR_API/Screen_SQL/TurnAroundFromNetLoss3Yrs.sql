/* 
please make a SQL to select by the table:
- get latest 3 years data of a company
- in these 3 years, every year with net_income_loss larger than maximum of previous years
- at least 2 years of these 3 years with negative net_income_loss
- the latest years net_income_loss can be positive or negative
*/

/* 
Explanation:
CompanyMaxYear CTE: Finds the latest year for companies with at least 3 years of history.

CompanyRecent3YearsData CTE: Selects net_income_loss data for the latest 3-year window for each qualifying company.

DataWithChecks CTE: This is the core. For each company and year within the 3-year window:
    It calculates is_improvement_over_max_prev: A flag set to 1 if the current year's net_income_loss is strictly greater than the maximum net_income_loss found in all preceding years within that 3-year window. For the first year (Year N-2), this flag will always be 0. For the second year (Year N-1), it checks if it's greater than Year N-2. For the third year (Year N), it checks if it's greater than MAX(Year N-2, Year N-1).
    It calculates is_negative: A flag set to 1 if net_income_loss is less than 0.
    It counts the number of years (years_in_window_count) being processed for that company's window using COUNT(*) OVER (...) to ensure data completeness later.
CompanyAggregates CTE: Groups the results by company and sums the flags:
    negative_years_count: Total count of loss-making years in the 3-year window.
    improvement_years_count: Total count of years that showed improvement over the previous maximum.
Final SELECT and WHERE:
    Filters for actual_years_processed = 3 to ensure the 3-year window was complete.
    Filters for improvement_years_count = 2. This enforces the strict condition: Year 2 must be > Year 1, AND Year 3 must be > MAX(Year 1, Year 2).
    Filters for negative_years_count >= 2, requiring at least two loss-making years within the 3-year period.

*/


WITH CompanyMaxYear AS (
    -- Find the latest year reported for each company that has at least 3 years of data
    SELECT
        cik,
        MAX(year) as max_yr_for_cik
    FROM nextcloud.sec_annual_data
    GROUP BY cik
    HAVING COUNT(DISTINCT year) >= 3 -- Ensure company has at least 3 years history
),
CompanyRecent3YearsData AS (
    -- Get the relevant data for the latest 3 years *for each qualifying company*
    SELECT
        s.cik,
        s.year,
        s.net_income_loss,
        cmy.max_yr_for_cik
    FROM nextcloud.sec_annual_data s
    JOIN CompanyMaxYear cmy ON s.cik = cmy.cik
    -- Filter for the years within the specific company's latest 3-year window
    WHERE s.year BETWEEN cmy.max_yr_for_cik - 2 AND cmy.max_yr_for_cik
),
DataWithChecks AS (
    -- Use window functions to perform checks within the 3-year window for each company
    SELECT
        cik,
        year,
        net_income_loss,
        max_yr_for_cik,
        -- Flag if current year's income > max income of *all* preceding years in the 3-year window
        CASE
            WHEN net_income_loss > MAX(net_income_loss) OVER (
                                        PARTITION BY cik ORDER BY year
                                        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                                    ) THEN 1
            ELSE 0
        END as is_improvement_over_max_prev,
        -- Flag if the current year's income is negative
        CASE WHEN net_income_loss < 0 THEN 1 ELSE 0 END as is_negative,
        -- Count how many years are in this company's window (should be 3)
        COUNT(*) OVER (PARTITION BY cik) as years_in_window_count
    FROM CompanyRecent3YearsData
),
CompanyAggregates AS (
    -- Aggregate the checks per company
    SELECT
        cik,
        MAX(max_yr_for_cik) as latest_year_for_cik,
        MAX(years_in_window_count) as actual_years_processed, -- Should be 3 if data is complete
        -- Count years with negative income (for Condition 3)
        SUM(is_negative) as negative_years_count,
        -- Count years showing improvement over previous max (for Condition 2)
        SUM(is_improvement_over_max_prev) as improvement_years_count
    FROM DataWithChecks
    GROUP BY cik
)
-- Final Selection: Companies meeting the specific improvement and loss criteria
SELECT
    agg.cik,
    -- Retrieve the latest ticker and company name
    (SELECT ticker
     FROM nextcloud.sec_annual_data sdn_ticker
     WHERE sdn_ticker.cik = agg.cik AND sdn_ticker.year = agg.latest_year_for_cik
     LIMIT 1) as latest_ticker,
    (SELECT company_name
     FROM nextcloud.sec_annual_data sdn_name
     WHERE sdn_name.cik = agg.cik AND sdn_name.year = agg.latest_year_for_cik
     LIMIT 1) as latest_company_name,
    agg.latest_year_for_cik,
    agg.negative_years_count,
    agg.improvement_years_count -- Should be 2 if criteria met
FROM CompanyAggregates agg
WHERE
    -- Ensure we are analyzing exactly 3 years of data
    agg.actual_years_processed = 3

    -- Condition 2: "every year with net_income_loss larger than maximum of previous years"
    -- In a 3-year window (Y1, Y2, Y3), this means:
    -- Y2 > MAX(Y1)  -> Y2 > Y1
    -- Y3 > MAX(Y1, Y2)
    -- The SUM(is_improvement_over_max_prev) flag calculation counts these exact two conditions.
    -- So, the sum must equal 2.
    AND agg.improvement_years_count = 2

    -- Condition 3: At least 2 years of negative net income in the window
    AND agg.negative_years_count >= 2;