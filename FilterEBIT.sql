-- Define screening criteria constants (adjust as needed)
SET @min_positive_ebitda_years = 3;
SET @min_positive_fcf_years = 3;
SET @min_ebitda_growth_pct = 15.0;
SET @target_period_years = 5; -- Should match YEARS_HISTORY used in Python script

-- Select stocks meeting the criteria
SELECT
    ticker,
    latest_data_year,
    positive_ebitda_years_count,
    positive_fcf_years_count,
    ebitda_cagr_percent,
    is_ebitda_turnaround,
    updated_at
FROM
    nextcloud.stock_financial_summary -- Use your actual database name if different
WHERE
    -- Ensure we are looking at data calculated for the correct period
    data_period_years = @target_period_years
    AND
    -- Ensure the last fetch attempt was successful
    data_fetch_error = FALSE
    AND
    -- Criterion 1: Positive EBITDA years
    positive_ebitda_years_count >= @min_positive_ebitda_years
    AND
    -- Criterion 2: Positive FCF years
    positive_fcf_years_count >= @min_positive_fcf_years
    AND
    -- Criterion 3: EBITDA Growth (Turnaround OR CAGR threshold)
    (
        is_ebitda_turnaround = TRUE
        OR
        (ebitda_cagr_percent IS NOT NULL AND ebitda_cagr_percent >= @min_ebitda_growth_pct)
    )
ORDER BY
    -- Optional: Sort results meaningfully
    is_ebitda_turnaround DESC,  -- Show turnarounds first
    ebitda_cagr_percent DESC;   -- Then by growth rate