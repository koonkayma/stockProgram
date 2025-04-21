/*
in sum, i want to make SQL to 
- get latest 5 years data of a company
- net_income_loss is positive in these 5 years
- at lease 15 % growth in net_income_loss compare with 5 years ago. 5 years ago means 5 years before latest year in availabe data
- at least 3 years in these 5 years's data with positive Free Cash Flow
*/

/*
Explanation of the Combined Logic:
CompanyMaxYear: Filters out companies that don't even have 5 years of data recorded in total. Then, identifies the most recent year (max_yr_for_cik) for each remaining company.

CompanyRecent5YearsData: Selects the net_income_loss and calculated_free_cash_flow for the 5-year period ending on each company's specific max_yr_for_cik.

CompanyAggregates: For each company, based only on the data from its latest 5-year window:
    Counts how many distinct years were actually found in that window (num_years_found_in_window).
    Finds the minimum net income (min_income_in_window).
    Pulls out the net income for the latest year (net_income_latest) and 5 years prior (net_income_5_ago).
    Counts the number of years with positive FCF (positive_fcf_years_count).
Final SELECT and WHERE: Filters the aggregated results:
    num_years_found_in_window = 5: Ensures the company has a complete dataset for its most recent 5 years, making the checks valid.
    min_income_in_window > 0: Checks that all 5 years had positive net income.
    net_income_5_ago > 0 AND net_income_latest >= (net_income_5_ago * 1.15): Checks for >= 15% growth from a positive base 5 years ago.
    positive_fcf_years_count >= 3: Checks for sufficient positive free cash flow years.
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
        s.calculated_free_cash_flow,
        cmy.max_yr_for_cik -- Keep the max year for this CIK
    FROM nextcloud.sec_annual_data s
    JOIN CompanyMaxYear cmy ON s.cik = cmy.cik
    -- Filter for the years within the specific company's latest 5-year window
    WHERE s.year BETWEEN cmy.max_yr_for_cik - 4 AND cmy.max_yr_for_cik
),
CompanyAggregates AS (
    -- Calculate all necessary aggregates over each company's latest 5 years
    SELECT
        cik,
        COUNT(DISTINCT year) as num_years_found_in_window, -- Check if exactly 5 years were found
        MIN(net_income_loss) as min_income_in_window,      -- For Condition 1 (positivity)
        MAX(CASE WHEN year = max_yr_for_cik THEN net_income_loss END) as net_income_latest, -- For Condition 2 (growth)
        MAX(CASE WHEN year = max_yr_for_cik - 4 THEN net_income_loss END) as net_income_5_ago, -- For Condition 2 (growth)
        SUM(CASE WHEN calculated_free_cash_flow > 0 THEN 1 ELSE 0 END) as positive_fcf_years_count, -- For Condition 3 (FCF)
        MAX(max_yr_for_cik) as latest_year_for_cik -- Pass through the latest year
    FROM CompanyRecent5YearsData
    GROUP BY cik
)
-- Final Selection: Companies meeting ALL criteria
SELECT
    agg.cik,
    -- Retrieve the latest ticker and company name using a subquery
    (SELECT ticker
     FROM nextcloud.sec_annual_data sdn_ticker
     WHERE sdn_ticker.cik = agg.cik AND sdn_ticker.year = agg.latest_year_for_cik
     LIMIT 1) as latest_ticker, -- Added ticker selection
    (SELECT company_name
     FROM nextcloud.sec_annual_data sdn_name
     WHERE sdn_name.cik = agg.cik AND sdn_name.year = agg.latest_year_for_cik
     LIMIT 1) as latest_company_name,
    agg.latest_year_for_cik,
    agg.min_income_in_window,      -- To verify condition 1
    agg.net_income_latest,         -- To verify condition 2
    agg.net_income_5_ago,          -- To verify condition 2
    agg.positive_fcf_years_count   -- To verify condition 3
FROM CompanyAggregates agg
WHERE
    -- Ensure data was found for all 5 years within *its* specific latest 5-year window
    -- This is crucial for the validity of the min check and the 5-year growth comparison
    agg.num_years_found_in_window = 5

    -- Condition 1: Net income loss is positive in ALL past 5 years
    AND agg.min_income_in_window > 0

    -- Condition 2: At least 15% growth compared to 5 years ago (base year must also be positive)
    AND agg.net_income_5_ago > 0 -- Base year income must be positive
    AND agg.net_income_latest >= (agg.net_income_5_ago * 1.15)

    -- Condition 3: At least 3 years in last 5 years' data with positive Free Cash Flow
    AND agg.positive_fcf_years_count >= 3;
    
    