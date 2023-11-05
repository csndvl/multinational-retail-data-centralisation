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
ORDER BY total_sales DESC
LIMIT 6;


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
