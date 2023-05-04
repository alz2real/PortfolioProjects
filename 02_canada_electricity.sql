/*
The raw csv file - electricity-exports-and-imports-monthly.csv, renamed to energy_exp_imp.csv for simplicity - used in 
this project was sourced from https://open.canada.ca/data/en/dataset/5c358f51-bc8c-4565-854d-9d2e35e6b178. The dataset is 
published by Canada Energy Regulator, under the Open Government License.
The dataset consist of electricity export and import volumes, values, and prices(by year and month) broken out by source and 
destination from 1990 t0 2023.
The integrated Canada-US Power Grid allows for bi-directional flows of electricity to meet fluctuating regional supply and demand.
*/

/*
After briefly viewing the downloaded csv file in a text editor and opening it in spreadsheet, the following pre-processing steps 
were carried out:
1 - Filters applied to the attributes/columns of the data and summary briefly viewed.
2 - Careful observation showed that for both the source and destination columns, Quebec is wrongly spelt as Qu閎ec.
3 - Find and Replace tool is used to change all occurences of Qu閎ec in source and destination columns to Quebec.
4 - All column names changed to lowercase.
5 - Energy (MW.h) column renamed to energy.
6 - Total Value (CAN$) column renamed to total_value.
7 - Price (CAN$/MW.h) changed to price.
8 - The price column has 642 values as 'Confidential'. We use the find and replace tool to change this to blanks
9 - Filters used to identify missing/blank values, as well as incorrect values:
	* period - No missing values
	* source - No missing values
	* destination - 6 missing values
	* energy - No missing values, negative values found
	* total_value - No missing values, negative values found
	* price - 642 missing values, negative values found
  - The above missing and negative values will be dealt with using SQL
*/

-- After preliminary pre-processing is done in spreadsheet, We create the required table in PostgreSQL database:
CREATE TABLE public.energy_exp_imp
(
    period character varying,-- the period date field is imported as a string to reduce errors during import
    activity character varying,
    source character varying,
    destination character varying,
    energy real,
    total_value real,
    price real
)
TABLESPACE pg_default;
ALTER TABLE public.energy_exp_imp
    OWNER to postgres;
	
commit; -- Save changes
	
-- Using psql, import the data from energy_exp_imp.csv file into the table:
C:\>psql -U postgres -d postgres -h localhost -p 5432 -- Connect to psql
C:\>COPY energy_exp_imp FROM 'D:/E-Resource/Data_and_AI/Datasets/Canada-Electricity_Imp_Exp/energy_exp_imp.csv' 
DELIMITER ',' CSV HEADER;
-- 39461 records copied

 -- View the first few records of the table:
 SELECT *
 FROM energy_exp_imp
 LIMIT 5;
 
 -- Alter the datatype of the period column to convert from string to date:
 ALTER TABLE energy_exp_imp
 ALTER COLUMN period TYPE DATE USING TO_DATE(period, 'MM/DD/YYYY');
 
 COMMIT; -- Save changes


-- Count the number of negative values in the energy column:
SELECT COUNT(*)
FROM energy_exp_imp
WHERE energy < 0;
-- 4

-- Set the negative values in the energy column to non-negative values:
UPDATE energy_exp_imp
SET energy = energy * -1
WHERE energy < 0;
-- 4 records updated

-- Count the number of negative values in the total_value column:
SELECT COUNT(*)
FROM energy_exp_imp
WHERE total_value < 0;
-- 307

-- Set the negative values in the total_value column to non-negative values:
UPDATE energy_exp_imp
SET total_value = total_value * -1
WHERE total_value < 0;
-- 307 records updated

COMMIT; -- Save changes

-- Query the table for all records that have a value of 0 for energy, total_value, and price. Remove these records from the table
SELECT COUNT(*)
FROM energy_exp_imp
WHERE energy = 0 AND total_value = 0 AND price = 0;
-- 993

-- Delete the records from above:
DELETE FROM energy_exp_imp
WHERE energy = 0 AND total_value = 0 AND price = 0;
-- 993 records deleted

-- Delete records from the table where destination is null:
DELETE FROM energy_exp_imp
WHERE destination is null;
-- 6 records deleted
COMMIT; -- Save changes

/*
The price column in the table is a calculated field: price = total_value / energy.
We can deal with the nulls/blanks in this column by updating all nulls to equal total_value / energy.
We also want to ensure there are no zeros for total_value or energy in the calculation to avoid exceptions:
*/
-- Query the table to get the number of records with nulls in the price column:
SELECT COUNT(*)
FROM energy_exp_imp
WHERE price is null;
-- 642

-- Query the the table for records where the price is null and either total_value or energy is equal to zero:
SELECT COUNT(*)
FROM energy_exp_imp
WHERE price is null AND (energy = 0 OR total_value = 0);
-- 0

-- Update the nulls in price column:
UPDATE energy_exp_imp
SET price = total_value / energy
WHERE price is null;
-- 642 updated

COMMIT; -- Save changes


-- Export the cleaned database table energy_exp_imp to energy_exp_imp.csv and upload to GitHub:
COPY
(
	SELECT 
	period,
	activity,
	source,
	destination,
	ROUND(energy::numeric, 2) AS energy,
	ROUND(total_value::numeric, 2) AS total_value,
	ROUND(price::numeric, 2) AS price
	FROM energy_exp_imp
	ORDER BY period
)
TO 'D:/E-Resource/Data_and_AI/Datasets/Canada-Electricity_Imp_Exp/energy_exp_imp_cleaned.csv' 
WITH DELIMITER ',' CSV HEADER; -- energy_exp_imp_cleaned.csv uploaded to GitHub	



-- Create a new table from energy_exp_imp with records of all energy exports from Manitoba:
CREATE TABLE manitoba_energy_exp AS
SELECT 
period,
activity,
source,
SUM(ROUND(energy::numeric, 2)) AS total_energy,
SUM(ROUND(total_value::numeric, 2)) sum_total_value,
SUM(ROUND(price::numeric, 2)) AS total_price
FROM energy_exp_imp
WHERE source = 'Manitoba'
GROUP BY period, activity, source
ORDER BY period;

-- Export manitoba_energy_exp to manitoba_energy_exp.csv and upload to GitiHub:
COPY (SELECT * FROM manitoba_energy_exp) TO 'D:/E-Resource/Data_and_AI/Datasets/Canada-Electricity_Imp_Exp/manitoba_energy_exp.csv' 
WITH DELIMITER ',' CSV HEADER; 

-- Create a new table from energy_exp_imp with records of all energy imports from Manitoba:
CREATE TABLE manitoba_energy_imp AS
SELECT 
period,
activity,
destination,
SUM(ROUND(energy::numeric, 2)) AS total_energy,
SUM(ROUND(total_value::numeric, 2)) sum_total_value,
SUM(ROUND(price::numeric, 2)) AS total_price
FROM energy_exp_imp
WHERE destination = 'Manitoba'
GROUP BY period, activity, destination
ORDER BY period;

-- Export manitoba_energy_imp to manitoba_energy_imp.csv and upload to GitiHub:
COPY (SELECT * FROM manitoba_energy_imp) TO 'D:/E-Resource/Data_and_AI/Datasets/Canada-Electricity_Imp_Exp/manitoba_energy_imp.csv' 
WITH DELIMITER ',' CSV HEADER;
