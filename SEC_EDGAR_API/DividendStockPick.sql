/*
i want to pick dividend stock. please make SQL by this conditions:
- get latest 10 years data of a company
- at least 7 years in these 10 years with positive dividends_paid
- at least 7 years in these 10 years with dividends_paid larger than maximum of previous years
*/

/*
Explanation:
CompanyMaxYear CTE: Finds the latest year for each company, filtering for those with at least 10 years of data total.

CompanyRecent10YearsData CTE: Extracts the dividends_paid for each company's latest 10-year period.

DataWithMaxPreviousDividend CTE: Uses the MAX() OVER (...) window function to find the highest dividend amount paid in any year before the current year, but only considering years within the 10-year window.

CompanyAggregates CTE:
    Counts the distinct years found in the window (num_years_found_in_window).
    Counts years where dividends_paid > 0 (positive_dividend_years_count).
    Counts years where the current dividends_paid is strictly greater than the max_previous_dividend_in_window. It also includes a condition to count the first year a positive dividend was paid (where max_previous_dividend_in_window would be NULL) as an "increase" (increasing_dividend_years_count). This captures a strong signal of consistently growing dividends.

Final SELECT and WHERE:
    Requires num_years_found_in_window = 10 to ensure a full 10 years of data for the comparison.
    Applies positive_dividend_years_count >= 7.
    Applies increasing_dividend_years_count >= 7. This is a demanding condition implying dividends were raised (above any previous peak within the window) in at least 7 out of the 10 years.
*/

WITH CompanyMaxYear AS (
    -- Find the latest year reported for each company that has at least 10 years of data
    SELECT
        cik,
        MAX(year) as max_yr_for_cik
    FROM nextcloud.sec_annual_data
    GROUP BY cik
    HAVING COUNT(DISTINCT year) >= 10 -- Ensure company has enough history overall
),
CompanyRecent10YearsData AS (
    -- Get the relevant data for the latest 10 years *for each qualifying company*
    SELECT
        s.cik,
        s.year,
        s.dividends_paid, -- Focus on dividends_paid
        cmy.max_yr_for_cik -- Keep the max year for this CIK
    FROM nextcloud.sec_annual_data s
    JOIN CompanyMaxYear cmy ON s.cik = cmy.cik
    -- Filter for the years within the specific company's latest 10-year window
    WHERE s.year BETWEEN cmy.max_yr_for_cik - 9 AND cmy.max_yr_for_cik
),
DataWithMaxPreviousDividend AS (
    -- Calculate the maximum dividends_paid from previous years WITHIN the 10-year window
    SELECT
        cik,
        year,
        dividends_paid,
        max_yr_for_cik,
        -- Calculate Max dividend from all preceding rows in the partition (within the 10yr window)
        MAX(dividends_paid) OVER (
            PARTITION BY cik
            ORDER BY year
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING -- Rows up to the one before current
        ) as max_previous_dividend_in_window
    FROM CompanyRecent10YearsData
),
CompanyAggregates AS (
    -- Aggregate results per company based on the 10-year window
    SELECT
        cik,
        COUNT(DISTINCT year) as num_years_found_in_window,
        -- Condition 1: Count years with positive dividends paid (value > 0)
        SUM(CASE WHEN dividends_paid > 0 THEN 1 ELSE 0 END) as positive_dividend_years_count,
        -- Condition 2: Count years where dividend > max of ALL preceding years in the window
        -- Comparison with NULL (first year or if all previous were NULL/0) results in false/0
        SUM(CASE
                WHEN dividends_paid IS NOT NULL AND max_previous_dividend_in_window IS NOT NULL AND dividends_paid > max_previous_dividend_in_window THEN 1
                WHEN dividends_paid IS NOT NULL AND dividends_paid > 0 AND max_previous_dividend_in_window IS NULL THEN 1 -- Count first positive dividend as an 'increase' from nothing
                ELSE 0
            END) as increasing_dividend_years_count,
        MAX(max_yr_for_cik) as latest_year_for_cik
    FROM DataWithMaxPreviousDividend
    GROUP BY cik
)
-- Final Selection: Companies meeting the dividend criteria
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
    agg.positive_dividend_years_count,
    agg.increasing_dividend_years_count
FROM CompanyAggregates agg
WHERE
    -- Ensure data was found for all 10 years within *its* specific latest 10-year window
    agg.num_years_found_in_window = 10

    -- Condition 1: At least 7 years with positive dividends paid
    AND agg.positive_dividend_years_count >= 7

    -- Condition 2: At least 7 years where dividend > max of previous years in window
    AND agg.increasing_dividend_years_count >= 7;
    