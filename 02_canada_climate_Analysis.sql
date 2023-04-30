/* In this section of the project, we use the information in 
the winnipeg_climate_rating table to derive important insights 
that could helpmake better informed decisions 
*/

/*
Let us identify trends in temperature over time: By calculating the 
average, maximum, and minimum temperature for each year, we can identify 
trends in temperature over time. This information can be used to
make decisions around climate change mitigation and adaptation strategies.
*/
-- The results of the query is exported to the system folder and upload to GitHub
COPY
(
	SELECT 
	EXTRACT(YEAR FROM local_date) AS date_year,
	COUNT(local_date) AS record_count, -- Counts the number of observations for each year
	ROUND(AVG(winnipeg_temp::numeric), 2) AS average_year_temp,
	ROUND(MAX(winnipeg_temp::numeric), 2) AS maximum_year_temp,
	ROUND(MIN(winnipeg_temp::numeric), 2) AS minimum_year_temp
	FROM winnipeg_climate_rating
	GROUP BY date_year
	HAVING COUNT(local_date) >= 300 -- Filters out group results with with less than 300 days of observation 
	ORDER BY date_year
)
TO 'D:/E-Resource/Data_and_AI/Datasets/Canadian_Weather/average_temp_80yrs.csv' 
WITH DELIMITER ',' CSV HEADER; -- average_temp_80yrs.csv uploaded to GitHub

/*
We can identify periods of extreme weather: By analyzing the temperature data, we 
can identify periods of extreme weather, such as heat waves and extreme cold weather events.
This information can be used to make decisions around emergency preparedness and response.
*/
-- The results of the query is exported to the system folder and upload to GitHub
COPY
(
	SELECT 
	EXTRACT(MONTH FROM local_date) AS date_month,
	TO_CHAR(local_date, 'Month') AS month_name,
	COUNT(local_date) AS record_count,
	ROUND(AVG(winnipeg_temp::numeric), 2) AS average_temp
	FROM winnipeg_climate_rating
	WHERE winnipeg_temp < -30 OR winnipeg_temp >= 30
	GROUP BY 1, 2 -- Group by month
	ORDER BY 3 DESC -- Order by number of records for each group
)
TO 'D:/E-Resource/Data_and_AI/Datasets/Canadian_Weather/extreme_temp_80yrs.csv' 
WITH DELIMITER ',' CSV HEADER; -- average_temp_80yrs.csv uploaded to GitHub
/*
The result of the above query shows that the coldest periods in Winnipeg occur between December and February, with
January typically being the coldest period. June to August are the hottest months in Winnipeg.
*/

SELECT *
FROM winnipeg_climate_rating
LIMIT 5;


-- The query below summarizes the total annual precipitation from 1940 to 2019:
COPY
(
	SELECT 
	EXTRACT(YEAR FROM local_date) AS date_year,
	ROUND(SUM(winnipeg_precip::numeric), 2) AS total_precipitation
	FROM winnipeg_climate_rating
	GROUP BY date_year
	HAVING SUM(winnipeg_precip) > 0
	ORDER BY date_year
)
TO 'D:/E-Resource/Data_and_AI/Datasets/Canadian_Weather/total_annual_precip_80yrs.csv' 
WITH DELIMITER ',' CSV HEADER; -- total_annual_precip_80yrs.csv uploaded to GitHub


-- The query below summarizes the total monthly precipitation from 1940 to 2019. 
COPY
(
	SELECT 
	EXTRACT(MONTH FROM local_date) AS date_month,
	TO_CHAR(local_date, 'Month') AS month_name,
	COUNT(local_date) AS record_count,
	ROUND(SUM(winnipeg_precip::numeric), 2) AS total_precipitation
	FROM winnipeg_climate_rating
	GROUP BY 1, 2 -- Group by month
	ORDER BY 1 -- Order by month
)
TO 'D:/E-Resource/Data_and_AI/Datasets/Canadian_Weather/total_monthly_precip_80yrs.csv' 
WITH DELIMITER ',' CSV HEADER; -- total_monthly_precip_80yrs.csv uploaded to GitHub




