/*
This data was sourced from the NHS Business Service Authority Open Data Portal.
This data is used under the Open Government Licence 3.0 (United Kingdom).
This data is available at: https://opendata.nhsbsa.net/dataset/pharmacy-and-appliance-contractor-dispensing-data/resource/6b374a23-d1b5-4e2b-9314-cdbb4581a94f
It is Pharmacy and appliance contractor dispensing data for September 2025.
This dataset represents a single reporting month. 
All analyses are cross-sectional. 
Temporal trends, seasonality, and longitudinal inference are out of scope

Objective: 
- Find trends/patterns in the pharmacy dispensing during September 2025
- Highlights geographic inequities in service utilisation/.
- local population health needs.
- Understand how dispensing varies across NHS geographies.

Steps:
- Create a database and table to store the dispensing data
- Load the data from a CSV file into the table
- Validate the data load by querying the table
- Clean the data:
    * Convert data types where necessary
    * Handle missing values
- Explore how NHS dispensing varies by geography (IVB,LPC,Postcode)
- Explore how NHS dispensing varies by type of pharmacy
- Explore NHS dispensing by Content Type, Content and Value
*/

-- Create database for NHS Pharmacy Dispensing Data
CREATE DATABASE nhs_pharmacy_dispensing;

-- Create schema for September-2025 NHS Pharmacy Dispensing Data
CREATE SCHEMA september_2025;

-- Create table for September-2025 NHS Pharmacy Dispensing Data
CREATE TABLE september_2025.dispensing_data (
    year_month TEXT,
    icb_code TEXT,
    icb_name TEXT,
    hwb_code TEXT,
    hwb_name TEXT,
    lpc_code TEXT,
    lpc_name TEXT,
    pharmacy_account_type TEXT,
    contractor_code TEXT,
    contractor_name TEXT,
    address_1 TEXT,
    address_2 TEXT,
    address_3 TEXT,
    address_4 TEXT,
    postcode TEXT,
    content_group TEXT,
    content TEXT,
    value TEXT
);

-- Load data from CSV file into the table
COPY september_2025.dispensing_data
FROM 'E:/Site/Data Analysis/Datasets/NHSPharm925/dispensing_data_202509.csv'
DELIMITER ','
CSV HEADER;

-- Create backup of the original table before data cleaning
CREATE TABLE september_2025.dispensing_data_backup AS
SELECT * FROM september_2025.dispensing_data;

-- Validate data load by querying the table
SELECT COUNT(*) AS total_records FROM september_2025.dispensing_data;
-- 183387 records 

-- Update month data type from TEXT to DATE 
/*
*create a new column with DATE type
*populate the new column by converting from TEXT to DATE
*validate the new column
*drop the old TEXT column
*rename the new DATE column to the old column name
*/
ALTER TABLE september_2025.dispensing_data
ADD COLUMN month_year DATE;

UPDATE september_2025.dispensing_data 
SET month_year = (year_month || '-01')::DATE;

SELECT COUNT(month_year) FROM september_2025.dispensing_data;

ALTER TABLE september_2025.dispensing_data
DROP COLUMN year_month;

ALTER TABLE september_2025.dispensing_data
RENAME COLUMN month_year TO year_month;

-- Update value column from TEXT to NUMERIC
/*
*create a new column with NUMERIC type
*populate the new column by converting from TEXT to NUMERIC (using REPLACE to remove commas)
*validate the new column 
*drop the old TEXT column
*/
ALTER TABLE september_2025.dispensing_data
ADD COLUMN products_dispensed INT;

UPDATE september_2025.dispensing_data
SET products_dispensed = value::INT;

SELECT COUNT(products_dispensed) FROM september_2025.dispensing_data;

ALTER TABLE september_2025.dispensing_data
DROP COLUMN value;

-- Check for missing values in columns
SELECT 
    COUNT(*) AS total_records,
    COUNT (CASE WHEN icb_code IS NULL THEN 1 END) AS missing_icb_code,
    COUNT (CASE WHEN icb_name IS NULL THEN 1 END) AS missing_icb_name,
    COUNT (CASE WHEN hwb_code IS NULL THEN 1 END) AS missing_hwb_code,
    COUNT (CASE WHEN hwb_name IS NULL THEN 1 END) AS missing_hwb_name,
    COUNT (CASE WHEN lpc_code IS NULL THEN 1 END) AS missing_lpc_code,
    COUNT (CASE WHEN lpc_name IS NULL THEN 1 END) AS missing_lpc_name,
    COUNT (CASE WHEN pharmacy_account_type IS NULL THEN 1 END) AS missing_pharmacy_account_type,
    COUNT (CASE WHEN contractor_code IS NULL THEN 1 END) AS missing_contractor_code,
    COUNT (CASE WHEN contractor_name IS NULL THEN 1 END) AS missing_contractor_name,
    COUNT (CASE WHEN postcode IS NULL THEN 1 END) AS missing_postcode, 
    COUNT (CASE WHEN content_group IS NULL THEN 1 END) AS missing_content_group,
    COUNT (CASE WHEN content IS NULL THEN 1 END) AS missing_content,
    COUNT (CASE WHEN products_dispensed IS NULL THEN 1 END) AS missing_products_dispensed
FROM september_2025.dispensing_data;

/*
*Results:
*missing_hwb_code = 812
*missing_hwb_name = 812
*missing_lpc_code = 2116
*missing_lpc_name = 2116
*/

-- Handle missing values in hwb_code and hwb_name
SELECT icb_name, hwb_code, hwb_name
FROM september_2025.dispensing_data
WHERE hwb_code IS NULL AND hwb_name IS NULL;

UPDATE TABLE september_2025.dispensing_data
SET hwb_code = 'Unknown',
    hwb_name = 'Unknown'
WHERE hwb_code IS NULL AND hwb_name IS NULL;

-- Handle missing values in lpc_code and lpc_name
SELECT icb_name, lpc_code, lpc_name 
FROM september_2025.dispensing_data
WHERE lpc_code IS NULL AND lpc_name IS NULL;

UPDATE TABLE september_2025.dispensing_data
SET lpc_code = 'Unknown',
    lpc_name = 'Unknown'
WHERE lpc_code IS NULL AND lpc_name IS NULL;

-- Get the distinct counts of categorical columns
SELECT 
COUNT(DISTINCT icb_code) AS icb_code_count,                                 
COUNT(DISTINCT icb_name) AS icb_name_count,                                 
COUNT(DISTINCT hwb_code) AS hwb_code_count,                                  
COUNT(DISTINCT hwb_name) AS hwb_name_count,                                 
COUNT(DISTINCT lpc_code) AS lpc_code_count,                                 
COUNT(DISTINCT lpc_name) AS lpc_name_count,                                 
COUNT(DISTINCT pharmacy_account_type) AS pharmacy_account_type_count,
COUNT(DISTINCT contractor_code) AS contractor_code_count,            
COUNT(DISTINCT contractor_name) AS contractor_name_count,            
COUNT(DISTINCT postcode) AS postcode_count,                          
COUNT(DISTINCT content_group) AS content_group_count,             
COUNT(DISTINCT content) AS content_count                                   
FROM september_2025.dispensing_data

-- What is the max, min, avg and total products dispensed in September 2025?
SELECT 
    MAX(products_dispensed) AS max_products_dispensed,
    MIN(products_dispensed) AS min_products_dispensed,
    AVG(products_dispensed) AS avg_products_dispensed,
    SUM(products_dispensed) AS total_products_dispensed 
FROM september_2025.dispensing_data;

-- What are the different Integrated Care Boards (ICBs) and the number of LPCs and pharmacies they oversee?
SELECT DISTINCT icb_name, 
COUNT(DISTINCT lpc_name) AS lpc_count,
COUNT(DISTINCT contractor_name) AS pharmacy_count
FROM september_2025.dispensing_data
GROUP BY icb_name
ORDER BY pharmacy_count DESC;

-- What is the total number of products dispensed by each ICB in September 2025?
SELECT icb_name, SUM(products_dispensed) AS total_products_dispensed
FROM september_2025.dispensing_data 
GROUP BY icb_name
ORDER BY total_products_dispensed DESC;

-- What percentage of the products dispensed does each ICB contribute in September 2025?
SELECT
    icb_name,
    SUM(products_dispensed) AS icb_items,
    ROUND (100 * SUM(products_dispensed) / SUM(SUM(products_dispensed)) OVER (),2)
    AS pct_of_national_total
FROM september_2025.dispensing_data
GROUP BY icb_name
ORDER BY pct_of_national_total DESC;

-- What are the different Local Pharmaceutical Committees (LPCs) and the number of pharmacies in each?
SELECT DISTINCT lpc_name, lpc_code, COUNT(DISTINCT contractor_name) AS pharmacy_count
FROM september_2025.dispensing_data 
GROUP BY lpc_name, lpc_code
ORDER BY pharmacy_count DESC;

-- What are the different contractors and their pharmacy account types?
SELECT DISTINCT contractor_name, pharmacy_account_type
FROM september_2025.dispensing_data
ORDER BY contractor_name;

-- How many pharmacies are there for each pharmacy account type? 
-- How many products were dispensed by each type?
SELECT DISTINCT pharmacy_account_type, 
COUNT(DISTINCT contractor_name) AS pharmacy_count,
SUM(products_dispensed) AS total_products_dispensed
FROM september_2025.dispensing_data
GROUP BY pharmacy_account_type
ORDER BY pharmacy_count DESC;

-- How many contents are there in each content group?
SELECT DISTINCT content_group, 
COUNT(content) AS content_count
FROM september_2025.dispensing_data
GROUP BY content_group
ORDER BY content_count DESC;

-- Get the unique contents and their occurrence counts
SELECT DISTINCT content, COUNT(*) AS occurrence_count
FROM september_2025.dispensing_data
GROUP BY content
ORDER BY occurrence_count DESC;

-- What are the top 10 contents by total products dispensed?
SELECT content, SUM(products_dispensed) AS total_products_dispensed
FROM september_2025.dispensing_data
GROUP BY content
ORDER BY total_products_dispensed DESC
LIMIT 10;

-- What are the top 10 contractors by total products dispensed?
WITH item_totals AS (
	SELECT contractor_name, pharmacy_account_type, 
    SUM(products_dispensed) AS total_products_dispensed
	FROM september_2025.dispensing_data
	GROUP BY contractor_name, pharmacy_account_type
)
SELECT contractor_name, pharmacy_account_type, 
    total_products_dispensed,
    ROUND (100 * total_products_dispensed / SUM(total_products_dispensed) OVER (),2)
    AS pct_of_national_total
FROM item_totals
ORDER BY total_products_dispensed DESC
LIMIT 10;

-- What are the top 10 applicance contractors by total products dispensed?
SELECT contractor_name, pharmacy_account_type, SUM(products_dispensed) AS total_products_dispensed
FROM september_2025.dispensing_data
WHERE pharmacy_account_type = 'Appliance'
GROUP BY contractor_name, pharmacy_account_type
ORDER BY total_products_dispensed DESC
LIMIT 10;

-- What are the top 10 postcodes by total products dispensed?
SELECT icb_name, postcode, SUM(products_dispensed) AS total_products_dispensed,
ROUND (100 * SUM(products_dispensed) / SUM(SUM(products_dispensed)) OVER (),2)
FROM september_2025.dispensing_data
GROUP BY postcode, icb_name
ORDER BY total_products_dispensed DESC

-- Do appliance contractors focus on specific content groups or contents?
SELECT content_group, COUNT(content) as content_count
FROM september_2025.dispensing_data
WHERE pharmacy_account_type = 'Appliance'
GROUP BY content_group
ORDER BY 2 DESC;

SELECT content, COUNT(content) as content_count
FROM september_2025.dispensing_data
WHERE pharmacy_account_type = 'Appliance'
GROUP BY content
ORDER BY 2 DESC;

-- How many products were dispensed at BOOTS pharmacies by content_group in September 2025?
SELECT content_group,content, SUM(products_dispensed) AS total_products_dispensed
FROM september_2025.dispensing_data
WHERE contractor_name IN ('BOOTS', 'BOOTS THE CHEMIST', 'YOUR LOCAL BOOTS PHARMACY')
GROUP BY content_group, content;