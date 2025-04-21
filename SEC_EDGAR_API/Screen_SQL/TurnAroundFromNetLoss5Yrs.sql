/*
maybe i add some conditions:
- get latest 5 years data of a company
- in these 5 years, at lease 3 years with net_income_loss larger than maximum of previous years
- at least 4 years of these 5 years with negative net_income_loss
- the latest years net_income_loss can be positive or negative
*/


/*
Explanation:
CompanyMaxYear CTE: Same as before, finds the latest year for each company with at least 5 years of data overall.

CompanyRecent5YearsData CTE: Same as before, gets the cik, year, net_income_loss for each company's latest 5-year window.

DataWithMaxPrevious CTE: This is the key step for Condition 2.
    It uses the MAX() OVER (...) window function.
    PARTITION BY cik: Ensures the calculation restarts for each company.
    ORDER BY year: Defines the order within the company's data.
    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING: This crucial part tells the MAX function to look at all rows before the current row within the partition (i.e., within that company's 5-year window) and find the maximum net_income_loss among them. For the very first year in the window, this will result in NULL because there are no preceding rows.
CompanyAggregates CTE:
    Groups the results by cik.
    COUNT(DISTINCT year): Checks if 5 years were actually processed (important for window function validity).
    SUM(CASE WHEN net_income_loss < 0 ...): Counts the number of years with negative income (Condition 3).
    SUM(CASE WHEN net_income_loss > max_previous_income_in_window ...): Counts the years where the current income was strictly greater than the maximum income seen in any of the previous years within that 5-year window (Condition 2). When max_previous_income_in_window is NULL (the first year), the comparison net_income_loss > NULL evaluates to unknown/false, so the first year is never counted here, which makes sense.
Final SELECT and WHERE:
    Requires num_years_found_in_window = 5 for data integrity.
    Applies improvement_years_count >= 3 (Condition 2).
    Applies negative_years_count >= 4 (Condition 3).

*/

WITH CompanyMaxYear AS (
    -- Find the latest year reported for each company that has at least 5 years of data
    SELECT
        cik,
        MAX(year) as max_yr_for_cik
    FROM nextcloud.sec_annual_data
    GROUP BY cik
    HAVING COUNT(DISTINCT year) >= 5 -- Ensure company has enough history overall
),
CompanyRecent5YearsData AS (
    -- Get the relevant data for the latest 5 years *for each qualifying company*
    SELECT
        s.cik,
        s.year,
        s.net_income_loss,
        cmy.max_yr_for_cik
    FROM nextcloud.sec_annual_data s
    JOIN CompanyMaxYear cmy ON s.cik = cmy.cik
    -- Filter for the years within the specific company's latest 5-year window
    WHERE s.year BETWEEN cmy.max_yr_for_cik - 4 AND cmy.max_yr_for_cik
),
DataWithMaxPrevious AS (
    -- Calculate the maximum net_income_loss from previous years WITHIN the 5-year window
    SELECT
        cik,
        year,
        net_income_loss,
        max_yr_for_cik,
        -- Calculate Max income from all preceding rows in the partition (within the 5yr window)
        MAX(net_income_loss) OVER (
            PARTITION BY cik
            ORDER BY year
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING -- Rows up to the one before current
        ) as max_previous_income_in_window
    FROM CompanyRecent5YearsData
),
CompanyAggregates AS (
    -- Aggregate results per company based on the 5-year window
    SELECT
        cik,
        COUNT(DISTINCT year) as num_years_found_in_window,
        -- Condition 3: Count years with negative net income
        SUM(CASE WHEN net_income_loss < 0 THEN 1 ELSE 0 END) as negative_years_count,
        -- Condition 2: Count years where income > max of all preceding years in the window
        -- Comparison with NULL (first year) results in false/0, which is correct
        SUM(CASE WHEN net_income_loss > max_previous_income_in_window THEN 1 ELSE 0 END) as improvement_years_count,
        MAX(max_yr_for_cik) as latest_year_for_cik
    FROM DataWithMaxPrevious
    GROUP BY cik
)
-- Final Selection: Companies meeting the turnaround/improvement criteria
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
    agg.improvement_years_count
FROM CompanyAggregates agg
WHERE
    -- Ensure data was found for all 5 years within *its* specific latest 5-year window
    agg.num_years_found_in_window = 5
    -- Condition 2: At least 3 years where income beat the max of all previous years in the window
    AND agg.improvement_years_count >= 3
    -- Condition 3: At least 4 years of negative net income in the window
    AND agg.negative_years_count >= 4;
