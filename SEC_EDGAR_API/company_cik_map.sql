-- nextcloud.company_cik_map definition

CREATE TABLE `company_cik_map` (
  `cik` int(10) unsigned NOT NULL COMMENT 'Central Index Key (Primary Key)',
  `ticker` varchar(20) DEFAULT NULL COMMENT 'Stock Ticker Symbol (May be NULL or duplicated for different CIKs/classes)',
  `company_name` varchar(255) DEFAULT NULL COMMENT 'Company Name/Title from SEC filing',
  `source_url` varchar(512) DEFAULT NULL COMMENT 'URL from where the data was last downloaded',
  `downloaded_at` timestamp NULL DEFAULT NULL COMMENT 'Timestamp when the data was downloaded',
  `imported_at` timestamp NOT NULL DEFAULT current_timestamp() COMMENT 'Timestamp when this record was inserted/updated',
  PRIMARY KEY (`cik`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci COMMENT='Maps SEC CIK to Ticker and Company Name';