
/*
The raw csv file(Canadian_climate_history.csv) used in this project was sourced from https://www.kaggle.com/datasets/aturner374/eighty-years-of-canadian-climate-data.
It consists of daily temperatures and precipitation from 13 Canadian centres. This project focuses only on the data queried from 2 weather
stations in Winnipeg: WINNIPEG RICHARDSON INT'L Airport, and WINNIPEG THE FORKS
*/
-- After viewing the downloaded csv file in a text editor, We start by creating the required table in PostgreSQL database:
CREATE TABLE public.canada_climate
(
    local_date character varying, -- the date field is imported as a string to reduce errors during import
    calgary_temp real,
    calgary_precip real,
    ottawa_temp real,
    ottawa_precip real,
    toronto_temp real,
    toronto_precip real,
    vancouver_temp real,
    vancouver_precip real,
    winnipeg_temp real,
    winnipeg_precip real
)
TABLESPACE pg_default;
ALTER TABLE public.canada_climate
    OWNER to postgres;

-- Using psql, import the data from csv file into the table:
C:\>psql -U postgres -d postgres -h localhost -p 5432 -- Connect to psql
 COPY canada_climate FROM 'D:/E-Resource/Data_and_AI/Datasets/Canadian_Weather/Canadian_climate_history_cleaned.csv' 
 DELIMITER ',' CSV HEADER;
 
 -- View the first few records ofo the table:
 SELECT *
 FROM canada_climate
 LIMIT 5;
 
 -- Alter the datatype of the local_date column to convert from string to date:
 ALTER TABLE canada_climate
 ALTER COLUMN local_date TYPE DATE USING TO_DATE(local_date, 'MM/DD/YYYY');
 
  -- View the first few records of the table:
 SELECT *
 FROM canada_climate
 LIMIT 5;
 
-- From the master table canada_climate, create a separate table for Winnipeg:

CREATE TABLE winnipeg AS 
(
	SELECT 
	local_date, 
	winnipeg_temp,
	winnipeg_precip
	FROM 
	canada_climate
);

COMMIT; -- Save changes

-- Clean up winnipeg table to replace/get rid of missing data:

-- Count the total number of records in winnipag table:
SELECT COUNT(*)
FROM winnipeg;
-- 29221


-- Count how many records in the winnipeg table contain a missing value for either temperature or precipitation:
SELECT COUNT(*)
FROM winnipeg
WHERE winnipeg_temp is null OR winnipeg_precip is null;
-- 254


-- Count how many records in the winnipeg table contain missing values for both temperature and precipitation:
SELECT COUNT(*)
FROM winnipeg
WHERE winnipeg_temp is null AND winnipeg_precip is null;
-- 117

-- Query the number of days for each year on which there were no records for both temperature and precipitation:
WITH winn_miss_data AS
(
	SELECT 
	local_date,
	EXTRACT(YEAR FROM local_date) AS year_recorded,
	winnipeg_temp,
	winnipeg_precip
	FROM winnipeg
	WHERE winnipeg_temp is null AND winnipeg_precip is null
	ORDER BY local_date
)
SELECT 
year_recorded,
COUNT(*) AS records_missing
FROM winn_miss_data
GROUP BY year_recorded
ORDER BY records_missing DESC;
-- 1993 has the highest number of missing records - 62

-- Export the output of the query above to a csv file:
COPY
(
	WITH winn_miss_data AS
	(
		SELECT 
		local_date,
		EXTRACT(YEAR FROM local_date) AS year_recorded,
		winnipeg_temp,
		winnipeg_precip
		FROM winnipeg
		WHERE winnipeg_temp is null AND winnipeg_precip is null
		ORDER BY local_date
	)
	SELECT 
	year_recorded,
	COUNT(*) AS records_missing
	FROM winn_miss_data
	GROUP BY year_recorded
	ORDER BY records_missing DESC
)
TO 'D:/E-Resource/Data_and_AI/Datasets/Canadian_Weather/winn_records_deleted_L1.csv' 
WITH DELIMITER ',' CSV HEADER; -- winn_records_deleted_L1.csv file uploaded to GitHub project repository


-- Delete all records in the winnipeg table containing missing values for both temperature and precipitation:
DELETE FROM winnipeg
WHERE winnipeg_temp is null AND winnipeg_precip is null;
-- 117 missing records deleted

COMMIT; -- Save changes

-- Get the new count of how many records in the winnipeg table contain a missing value for either temperature or precipitation:
SELECT COUNT(*)
FROM winnipeg
WHERE winnipeg_temp is null OR winnipeg_precip is null;
-- 137

-- Count how many records are missing a temperature value only:
SELECT COUNT(*)
FROM winnipeg
WHERE winnipeg_temp is null;
-- 7 records returned. These missing values will be replaced with the average for each respective month.


-- Create a modified winnipeg table with a year_month column added:
CREATE TABLE winnipeg_modified AS
SELECT 
	local_date,
	CONCAT(EXTRACT(YEAR FROM local_date), '-', EXTRACT(MONTH FROM local_date)) AS year_month,
	winnipeg_temp,
	winnipeg_precip
FROM winnipeg;



WITH null_periods AS -- Retrieve the year-month groupings that contain nulls for winnipeg_temp
(
	SELECT 
	CONCAT(EXTRACT(YEAR FROM local_date), '-', EXTRACT(MONTH FROM local_date)) AS year_month
	FROM winnipeg
	WHERE winnipeg_temp is null	
	GROUP BY year_month
),
winn_modified AS -- Create modified temporary table based on original winnipeg table with year_month column
(
	SELECT 
	CONCAT(EXTRACT(YEAR FROM local_date), '-', EXTRACT(MONTH FROM local_date)) AS year_month,
	winnipeg_temp,
	winnipeg_precip
	FROM winnipeg 
),
winn_temp_avgs AS -- Create a temporary table from winn_modified with year-month groups and average temperatures
(
	SELECT 
	year_month,
	ROUND(AVG(winnipeg_temp)::numeric, 2) AS average_temp
	FROM winn_modified
	GROUP BY year_month
	HAVING year_month IN (SELECT year_month FROM null_periods)
	ORDER BY year_month
)
-- Replace the nulls in the temperature column of winnipeg_modified with the average temperatures from winn_temp_avgs.
UPDATE winnipeg_modified
SET winnipeg_temp = winn_temp_avgs.average_temp
FROM winn_temp_avgs
WHERE winnipeg_modified.winnipeg_temp is null AND winnipeg_modified.year_month = winn_temp_avgs.year_month;
-- 7 Records updated

-- Confirm the non-existence of nulls in winnipeg_temp column:
SELECT *
FROM winnipeg_modified
WHERE winnipeg_temp is null;
-- No records returned


-- Count how many records are missing a precipitation value:
SELECT COUNT(*)
FROM winnipeg_modified
WHERE winnipeg_precip is null;
-- 130 records returned. These missing values will be replaced with the average for each respective month as shown below:


WITH null_periods AS -- Retrieve the year-month groupings that contain nulls for winnipeg_precip
(
	SELECT 
	CONCAT(EXTRACT(YEAR FROM local_date), '-', EXTRACT(MONTH FROM local_date)) AS year_month
	FROM winnipeg
	WHERE winnipeg_precip is null	
	GROUP BY year_month
),
winn_modified AS -- Create modified temporary table based on original winnipeg table with year_month column
(
	SELECT 
	CONCAT(EXTRACT(YEAR FROM local_date), '-', EXTRACT(MONTH FROM local_date)) AS year_month,
	winnipeg_temp,
	winnipeg_precip
	FROM winnipeg 
),
winn_precip_avgs AS -- Create a temporary table from winn_modified with year-month groups and average precipitation for each month
(
	SELECT 
	year_month,
	ROUND(AVG(winnipeg_precip)::numeric, 2) AS average_precip
	FROM winn_modified
	GROUP BY year_month
	HAVING year_month IN (SELECT year_month FROM null_periods)
	ORDER BY year_month
)
-- Replace the nulls in the precipitation column of winnipeg_modified with the average precipitation from winn_precip_avgs.
UPDATE winnipeg_modified
SET winnipeg_precip = winn_precip_avgs.average_precip
FROM winn_precip_avgs
WHERE winnipeg_modified.winnipeg_precip is null AND winnipeg_modified.year_month = winn_precip_avgs.year_month;
-- 130 Records updated 

-- Confirm the non-existence of nulls in winnipeg_precip column:
SELECT *
FROM winnipeg_modified
WHERE winnipeg_precip is null;
-- No records returned

COMMIT; -- Save changes


-- Rename the original winnipeg table to winnipeg_backup, and winnipeg_modified to winnipeg:
ALTER TABLE winnipeg
RENAME TO winnipeg_backup;

ALTER TABLE winnipeg_modified
RENAME TO winnipeg;

COMMIT; -- Save Changes

SELECT * 
FROM winnipeg
LIMIT 10;



-- Create a new table, winnipeg_climate_rating, with columns for temperature and precipitation categorizations:
CREATE TABLE winnipeg_climate_rating AS
SELECT 
local_date,
year_month,
winnipeg_temp,
CASE
	WHEN winnipeg_temp < -15 THEN 'Extremely Cold'
	WHEN winnipeg_temp >= -15 AND winnipeg_temp < 0 THEN 'Very Cold'
	WHEN winnipeg_temp >= 0 AND winnipeg_temp < 15 THEN 'Cold'
	WHEN winnipeg_temp >= 15 AND winnipeg_temp < 25 THEN 'Fair'
	WHEN winnipeg_temp >= 25 AND winnipeg_temp < 35 THEN 'Hot'
	WHEN winnipeg_temp >= 35 AND winnipeg_temp < 40 THEN 'Very Hot'
	WHEN winnipeg_temp >= 40  THEN 'Extremely Hot'
END AS temp_category,
winnipeg_precip,
CASE
	WHEN winnipeg_precip = 0 THEN 'No Precipitation'
	WHEN winnipeg_precip > 0 AND winnipeg_precip < 2.5 THEN 'Light Precipitation'
	WHEN winnipeg_precip >=2.5 AND winnipeg_precip < 7.5 THEN 'Moderate Precipitation'
	WHEN winnipeg_precip >= 7.5 AND winnipeg_precip < 50 THEN 'Heavy Precipitation'
	WHEN winnipeg_precip >= 50 AND winnipeg_precip < 100 THEN 'Very Heavy Precipitation'
END AS precip_category
FROM winnipeg;

COMMIT; -- Save changes

SELECT * FROM winnipeg_climate_rating
ORDER BY local_date DESC
LIMIT 5; -- View first 5 records

--Export the newly created and cleaned winnipeg_climate_rating table to the project folder and upload to GitHub:
COPY (SELECT * FROM winnipeg_climate_rating) TO 'D:/E-Resource/Data_and_AI/Datasets/Canadian_Weather/winnipeg_climate_rating.csv' 
WITH DELIMITER ',' CSV HEADER; -- winnipeg_climate_rating.csv file uploaded to GitHub





	
	



 
 
 
 
 
 