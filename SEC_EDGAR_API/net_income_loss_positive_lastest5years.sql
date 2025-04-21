-- make SQL to select net_income_loss is positive in latest 5 years

--Changes and Explanation:
-- CompanyMaxYear CTE:
-- Finds the MAX(year) separately for each cik.
-- Includes HAVING COUNT(DISTINCT year) >= 5 to ensure we only even start considering companies that have at least 5 years of history somewhere in the table.
-- CompanyRecent5YearsData CTE:
-- Joins the original data with CompanyMaxYear.
-- The WHERE s.year BETWEEN cmy.max_yr_for_cik - 4 AND cmy.max_yr_for_cik clause now filters based on the company's specific maximum year, not the overall table maximum.
-- CompanyAggregates CTE:
-- Groups by cik based on the data from the company-specific 5-year window.
-- COUNT(DISTINCT year) now checks if data exists for all 5 years within that specific window.
-- MIN(net_income_loss) finds the minimum within that window.
-- Final SELECT:
-- Filters for num_years_found_in_window = 5 to ensure completeness within the company's latest 5 years.
-- Filters for min_income_in_window > 0 to ensure positivity within that window.

WITH CompanyMaxYear AS (
    -- Find the latest year reported for each company
    SELECT
        cik,
        MAX(year) as max_yr_for_cik
    FROM nextcloud.sec_annual_data
    GROUP BY cik
    -- Ensure the company has at least 5 years of data overall to be considered
    HAVING COUNT(DISTINCT year) >= 5
),
CompanyRecent5YearsData AS (
    -- Get the data for the latest 5 years *for each company*
    SELECT
        s.cik,
        s.year,
        s.net_income_loss,
        cmy.max_yr_for_cik -- Keep the max year for this CIK
    FROM nextcloud.sec_annual_data s
    JOIN CompanyMaxYear cmy ON s.cik = cmy.cik
    -- Filter for the years within the specific company's latest 5-year window
    WHERE s.year BETWEEN cmy.max_yr_for_cik - 4 AND cmy.max_yr_for_cik
),
CompanyAggregates AS (
    -- Check the conditions over each company's latest 5 years
    SELECT
        cik,
        COUNT(DISTINCT year) as num_years_found_in_window,
        MIN(net_income_loss) as min_income_in_window,
        MAX(max_yr_for_cik) as latest_year_for_cik -- Pass through the latest year
    FROM CompanyRecent5YearsData
    GROUP BY cik
)
-- Final Selection: Companies that have exactly 5 years in their latest 5-year window
-- AND where the minimum income in that window was positive.
SELECT
    agg.cik,
    -- Optional: Get the name from the latest year for this CIK
    (SELECT company_name
     FROM nextcloud.sec_annual_data sdn
     WHERE sdn.cik = agg.cik AND sdn.year = agg.latest_year_for_cik
     LIMIT 1) as latest_company_name,
    agg.min_income_in_window -- Display the minimum income found (should be > 0)
FROM CompanyAggregates agg
WHERE
    -- Ensure data was found for all 5 years within *its* specific latest 5-year window
    agg.num_years_found_in_window = 5
    -- Ensure the minimum income in that window was positive
    AND agg.min_income_in_window > 0;
