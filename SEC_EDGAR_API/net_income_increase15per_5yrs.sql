-- make SQL at to select least 15 % growth in net_income_loss compare with 5 years ago. 5 years ago start means 5 years before latest year in availabe data

/*
CompanyMaxYear CTE: Finds the latest year (max_yr_for_cik) for each company (cik) present in the table.
CompanyIncomeComparison CTE:
Takes the results from CompanyMaxYear.
Uses a JOIN to connect back to sec_annual_data (aliased as latest) to get the net_income_loss for the company's max_yr_for_cik.
Uses a second JOIN to connect back to sec_annual_data (aliased as past) to get the net_income_loss for the year max_yr_for_cik - 4 (which is 5 years prior, inclusive of the start year).
Crucially: Because these are JOINs (equivalent to INNER JOIN), a company will only appear in this CTE if it has data recorded in the sec_annual_data table for both its latest year and the year 5 years before that.
Final SELECT:
Selects the CIK, optional name, the years being compared, and the corresponding net income values.
Includes an optional growth_percentage calculation for context.
WHERE Clause:
Includes IS NOT NULL checks (though usually redundant because of the JOINs).
cic.net_income_5_ago > 0: This ensures we are calculating growth based on a positive starting income. If the income 5 years ago was zero or negative, the concept of "15% growth" becomes less straightforward, and this condition excludes those cases.
cic.net_income_latest >= (cic.net_income_5_ago * 1.15): This is the core condition, checking if the latest income is at least 1.15 times the income from 5 years ago.
*/

WITH CompanyMaxYear AS (
    -- Find the latest year reported for each company
    SELECT
        cik,
        MAX(year) as max_yr_for_cik
    FROM nextcloud.sec_annual_data
    GROUP BY cik
),
CompanyIncomeComparison AS (
    -- Get the net income for the latest year and the year 5 years prior for each company
    -- This implicitly requires data to exist for both years due to the JOINs
    SELECT
        cy.cik,
        cy.max_yr_for_cik,
        latest.net_income_loss as net_income_latest,
        past.net_income_loss as net_income_5_ago
    FROM CompanyMaxYear cy
    -- Join to get latest year's data
    JOIN nextcloud.sec_annual_data latest
      ON cy.cik = latest.cik AND cy.max_yr_for_cik = latest.year
    -- Join to get data from 5 years prior (max_yr - 4 includes the max year in the 5-year count)
    JOIN nextcloud.sec_annual_data past
      ON cy.cik = past.cik AND (cy.max_yr_for_cik - 4) = past.year
)
-- Final Selection: Companies meeting the growth criteria
SELECT
    cic.cik,
    -- Optional: Get the name from the latest year for this CIK
    (SELECT company_name
     FROM nextcloud.sec_annual_data sdn
     WHERE sdn.cik = cic.cik AND sdn.year = cic.max_yr_for_cik
     LIMIT 1) as latest_company_name,
    cic.max_yr_for_cik as latest_year,
    cic.net_income_latest,
    cic.max_yr_for_cik - 4 as year_5_ago,
    cic.net_income_5_ago,
    -- Optional: Calculate the actual growth percentage for display
    CASE
        WHEN cic.net_income_5_ago IS NOT NULL AND cic.net_income_5_ago <> 0 THEN -- Avoid division by zero
             ((cic.net_income_latest - cic.net_income_5_ago) / ABS(cic.net_income_5_ago)) * 100 -- Use ABS for % change from negative
        ELSE NULL -- Or 0 or some other indicator if division by zero or NULL base
    END as growth_percentage
FROM CompanyIncomeComparison cic
WHERE
    -- Ensure we have values to compare (already handled by JOINs, but good practice)
    cic.net_income_latest IS NOT NULL
    AND cic.net_income_5_ago IS NOT NULL
    -- Apply the 15% growth condition
    -- We check if net_income_5_ago is positive for a standard growth comparison.
    -- If you want to include growth from negative to positive, this condition needs adjustment.
    AND cic.net_income_5_ago > 0 -- Only compare growth from a positive base
    AND cic.net_income_latest >= (cic.net_income_5_ago * 1.15);
