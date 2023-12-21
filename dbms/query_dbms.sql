-- Perform a query to get the number of stores by country
SELECT 
    country_code, COUNT(*) 
FROM
    dim_store_details 
GROUP BY
    country_code 
ORDER BY
    COUNT(*) DESC;

-- Perform a query to get the location that has the highest number of stores
SELECT
    locality, COUNT(*) 
FROM
    dim_store_details 
GROUP BY
    locality 
ORDER BY
    COUNT(*) DESC
LIMIT 7;

-- Perform a query to find out which months typically have the most sales
SELECT
    ROUND(SUM(product_price * product_quantity)::numeric, 0) AS total_sales,
    month
FROM
    orders_table
    JOIN dim_products ON orders_table.product_code = dim_products.product_code
    JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    month
ORDER BY
    total_sales DESC;

-- Perform a query to find out how many sales are coming from online
SELECT
    COUNT(*) AS numbers_of_sales,
	ROUND(SUM(product_quantity)::numeric, 0) AS product_quantity_count,
    CASE
        WHEN store_type = 'Web Portal' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM
    orders_table
    JOIN dim_products ON orders_table.product_code = dim_products.product_code
    JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
    location
ORDER BY
    numbers_of_sales ASC;

-- Perform a query to find out what percentage of sales come through each type of store
SELECT 
    store_type,
    ROUND(SUM(product_price * product_quantity)::numeric, 0) AS total_sales,
    ROUND(((SUM(product_price * product_quantity) / SUM(SUM(product_price * product_quantity)) OVER ()) * 100)::numeric, 2) AS "percentage_total(%)"
FROM
    orders_table
    JOIN dim_products ON orders_table.product_code = dim_products.product_code
    JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
    store_type
ORDER BY
    total_sales DESC;

-- Perform a query to find out which months in which years have had the most sales historically
SELECT 
    ROUND(SUM(product_price * product_quantity)::numeric, 0) AS total_sales,
    year,
    month
FROM
    orders_table
    JOIN dim_products ON orders_table.product_code = dim_products.product_code
    JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    year,
    month
ORDER BY
    total_sales DESC
LIMIT 5;

-- Perform a query to find out the overall staff numbers in each location around the world
ALTER TABLE dim_store_details
    ALTER COLUMN country_code TYPE VARCHAR(3) USING country_code::VARCHAR(3);

UPDATE dim_store_details
    SET country_code = COALESCE(country_code, 'Web');

SELECT
    SUM(staff_numbers) AS total_staff_numbers,
    country_code
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY
    total_staff_numbers DESC;

-- Perform a query to find out which type of store is generating the most sales in Germany
SELECT
    COUNT(*) AS total_sales,
    store_type,
    country_code
FROM
    orders_table
    JOIN dim_products ON orders_table.product_code = dim_products.product_code
    JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
    store_type,
    country_code
HAVING
    country_code = 'DE'
ORDER BY
    total_sales ASC;

-- Perform a query to find out how quickly the company is making sales
SELECT
	year, 
	AVG(next_sale - previous_sale) AS actual_time_taken
FROM (
	-- Create a common table expression (CTE)
	WITH cte AS (
		SELECT
			year,
			year || '-' || month || '-' || day || ' ' || timestamp 
			AS previous_sale
		FROM dim_date_times
		ORDER BY previous_sale
	)
	-- Select the year, previous_sale, and next_sale from CTE using a subquery
	SELECT 
		year, 
		TO_TIMESTAMP(previous_sale, 'YYYY-MM-DD HH24:MI:SS:MS') AS previous_sale,
		TO_TIMESTAMP(LEAD(previous_sale, 1) OVER (ORDER BY previous_sale), 'YYYY-MM-DD HH24:MI:SS:MS') AS next_sale
	FROM cte
) AS subquery
GROUP BY
	year
ORDER BY
	actual_time_taken DESC
LIMIT 5;
