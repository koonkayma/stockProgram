-- make SQL to select at least 3 years in latest 5 years's data with positive Free Cash Flow

/*
CompanyMaxYear CTE: Finds the latest year (max_yr_for_cik) available for each company (cik).
CompanyRecent5YearsFCF CTE:
Joins the main table with CompanyMaxYear.
Filters the data to include only rows within the company's specific latest 5-year period (max_yr_for_cik - 4 to max_yr_for_cik).
Selects the calculated_free_cash_flow for these years.
PositiveFCFCount CTE:
Groups the results from the previous CTE by company (cik).
Uses SUM(CASE WHEN calculated_free_cash_flow > 0 THEN 1 ELSE 0 END) to count how many rows within that 5-year window had a positive calculated_free_cash_flow.
(Optional commented-out line shows how you could also count years that simply have FCF data, regardless of sign, if needed).
Final SELECT:
Selects the cik and other useful information from the PositiveFCFCount results.
The WHERE pfc.positive_fcf_years_count >= 3 clause filters to include only those companies that met the condition of having 3 or more years with positive Free Cash Flow in their most recent 5-year reporting period.
*/

WITH CompanyMaxYear AS (
    -- Find the latest year reported for each company
    SELECT
        cik,
        MAX(year) as max_yr_for_cik
    FROM nextcloud.sec_annual_data
    GROUP BY cik
),
CompanyRecent5YearsFCF AS (
    -- Get the FCF data for the latest 5 years *for each company*
    SELECT
        s.cik,
        s.year,
        s.calculated_free_cash_flow,
        cmy.max_yr_for_cik -- Keep the max year for this CIK
    FROM nextcloud.sec_annual_data s
    JOIN CompanyMaxYear cmy ON s.cik = cmy.cik
    -- Filter for the years within the specific company's latest 5-year window
    WHERE s.year BETWEEN cmy.max_yr_for_cik - 4 AND cmy.max_yr_for_cik
),
PositiveFCFCount AS (
    -- Count the number of years with positive FCF within that 5-year window
    SELECT
        cik,
        SUM(CASE WHEN calculated_free_cash_flow > 0 THEN 1 ELSE 0 END) as positive_fcf_years_count,
        -- Optional: Count how many years actually had FCF data (not NULL) in the window
        -- COUNT(calculated_free_cash_flow) as fcf_data_years_count,
        MAX(max_yr_for_cik) as latest_year_for_cik -- Carry forward the latest year
    FROM CompanyRecent5YearsFCF
    GROUP BY cik
)
-- Final Selection: Companies meeting the positive FCF count threshold
SELECT
    pfc.cik,
    -- Optional: Get the name from the latest year for this CIK
    (SELECT company_name
     FROM nextcloud.sec_annual_data sdn
     WHERE sdn.cik = pfc.cik AND sdn.year = pfc.latest_year_for_cik
     LIMIT 1) as latest_company_name,
    pfc.latest_year_for_cik,
    pfc.positive_fcf_years_count -- Show the count found
    -- Optional: Display fcf_data_years_count if calculated above
FROM PositiveFCFCount pfc
WHERE
    -- Condition: At least 3 years in the latest 5-year window had positive FCF
    pfc.positive_fcf_years_count >= 3;
