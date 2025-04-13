
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
