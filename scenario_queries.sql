--TASK 1: How many stores does the business have and in which countries?
SELECT country_code, COUNT(store_code) as total_no_stores
FROM dim_store_details
GROUP BY country_code 
ORDER BY total_no_stores DESC;


--TASK 2: Which locations currently have the most stores?
SELECT locality, COUNT(store_code) as total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;


--TASK 3: Which months produce the average highest cost of sales typically?
SELECT ROUND(SUM(dim_product.product_price * order_table.product_quantity)::NUMERIC,2) as total_sales, dim_date_times.month 
FROM dim_product
JOIN order_table
ON order_table.product_code = dim_product.product_code
JOIN dim_date_times
ON dim_date_times.date_uuid = order_table.date_uuid
GROUP BY dim_date_times.month
ORDER BY total_sales DESC;


--TASK 4: How many sales are coming from online?
SELECT COUNT(dim_product.product_code) as number_of_sales, SUM(order_table.product_quantity) as product_quantity_count, 
CASE
WHEN dim_store_details.store_type IN ('Super Store', 'Local', 'Outlet', 'Mall Kiosk') THEN 'Offline'
ELSE 'Web'
END as location
FROM dim_product
JOIN order_table
ON order_table.product_code = dim_product.product_code
JOIN dim_store_details
ON dim_store_details.store_code = order_table.store_code
GROUP BY location 
ORDER BY number_of_sales ASC;


--TASK 5: What percentage of sales come through each type of store
SELECT dim_store_details.store_type, ROUND(SUM(dim_product.product_price * order_table.product_quantity)::NUMERIC,2) as total_sales, 
ROUND(SUM(100 * dim_product.product_price * order_table.product_quantity)::NUMERIC/ SUM(SUM(dim_product.product_price * order_table.product_quantity)::NUMERIC) OVER (), 2) as "percentage_total(%)"
FROM dim_product
JOIN order_table
ON order_table.product_code = dim_product.product_code
JOIN dim_store_details
ON dim_store_details.store_code = order_table.store_code
GROUP BY dim_store_details.store_type
ORDER BY "percentage_total(%)" DESC;


--TASK 6: Which month in each year produced the highest cost of sales?
SELECT ROUND(SUM(dim_product.product_price * order_table.product_quantity)::NUMERIC ,2) as total_sales,
dim_date_times.year, dim_date_times.month
FROM dim_product
JOIN order_table
ON order_table.product_code = dim_product.product_code
JOIN dim_date_times
ON dim_date_times.date_uuid = order_table.date_uuid
GROUP BY dim_date_times.year, dim_date_times.month
ORDER BY total_sales DESC;


--TASK 7: What is our staff headcount?
SELECT SUM(staff_numbers) as total_staff_numbers, country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;


--TASK 8: Which German store type is selling the most?
SELECT ROUND(SUM(dim_product.product_price * order_table.product_quantity)::NUMERIC ,2) as total_sales,
dim_store_details.store_type, dim_store_details.country_code
FROM dim_product
JOIN order_table
ON order_table.product_code = dim_product.product_code
JOIN dim_store_details
ON dim_store_details.store_code = order_table.store_code
WHERE country_code = 'DE'
GROUP BY dim_store_details.store_type, dim_store_details.country_code
ORDER BY total_sales;


--TASK 9: How quickly is the company making sales?
WITH time_table(hour, minutes, seconds, day, month, year, date_uuid) as (
	SELECT 
		EXTRACT(hour from CAST(timestamp as time)) as hour,
		EXTRACT(minute from CAST(timestamp as time)) as minutes,
		EXTRACT(second from CAST(timestamp as time)) as seconds,
		day as day,
		month as month,
		year as year,
		date_uuid
	FROM dim_date_times),
	
	timestamp_table(timestamp, date_uuid, year) as (
		SELECT MAKE_TIMESTAMP(CAST(time_table.year as int), CAST(time_table.month as int),
							  CAST(time_table.day as int), CAST(time_table.hour as int),	
							  CAST(time_table.minutes as int), CAST(time_table.seconds as float)) as order_timestamp,
			time_table.date_uuid as date_uuid, 
			time_table.year as year
		FROM time_table),
	
	time_stamp_diffs(year, time_diff) as (
		SELECT timestamp_table.year, timestamp_table.timestamp - LAG(timestamp_table.timestamp) OVER (ORDER BY timestamp_table.timestamp asc) as time_diff
		FROM order_table
		JOIN timestamp_table ON order_table.date_uuid = timestamp_table.date_uuid),

	year_time_diffs(year, average_time_diff) as (
		SELECT year, AVG(time_diff) as average_time_diff
		FROM time_stamp_diffs
		GROUP BY year
		ORDER BY average_time_diff desc)
		
SELECT 
	year, 
	CONCAT('hours: ', EXTRACT(HOUR FROM average_time_diff),
					'  minutes: ', EXTRACT(MINUTE FROM average_time_diff),
				   '  seconds: ', CAST(EXTRACT(SECOND FROM average_time_diff) as int),
				   '  milliseconds: ', CAST(EXTRACT(MILLISECOND FROM average_time_diff) as int))
FROM year_time_diffs;


