-- Select all the columns of "orders_table"
SELECT * FROM orders_table;

-- Get the maximum length of the values in a column
SELECT
    MAX(LENGTH(card_number)) AS card_max_length,
    MAX(LENGTH(store_code)) AS store_max_length,
	MAX(LENGTH(product_code)) AS product_max_length
FROM
	orders_table;

-- Set a new data types for each colum of the table
ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN card_number TYPE VARCHAR(19) USING card_number::VARCHAR(19),
	ALTER COLUMN store_code TYPE VARCHAR(12) USING store_code::VARCHAR(12),
	ALTER COLUMN product_code TYPE VARCHAR(11) USING product_code::VARCHAR(11),
	ALTER COLUMN product_quantity TYPE SMALLINT USING product_quantity::SMALLINT;

-- Select all the columns of "dim_users"
SELECT * FROM dim_users;

-- Get the maximum length of the values in a column
SELECT
    MAX(LENGTH(country_code)) AS country_max_length
FROM
	dim_users;

-- Set a new data types for each colum of the table
ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255) USING first_name::VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255) USING last_name::VARCHAR(255),
    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
	ALTER COLUMN country_code TYPE VARCHAR(2) USING country_code::VARCHAR(2),
	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
	ALTER COLUMN join_date TYPE DATE USING join_date::DATE;

-- Select all the columns of "dim_store_details"
SELECT * FROM dim_store_details;

-- Merge "latitude" column into "lat" column
UPDATE dim_store_details
    SET lat = latitude
    DROP COLUMN latitude
    RENAME COLUMN lat TO latitude;

-- Get the maximum length of the values in a column
SELECT
	MAX(LENGTH(store_code)) AS store_max_length,
    MAX(LENGTH(country_code)) AS country_max_length
FROM
	dim_store_details;

-- Set a new data types for each colum of the table
ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
    ALTER COLUMN locality TYPE VARCHAR(255) USING locality::VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(11) USING store_code::VARCHAR(11),
	ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
	ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
	ALTER COLUMN store_type TYPE VARCHAR(255) USING store_type::VARCHAR(255),
	ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
	ALTER COLUMN country_code TYPE VARCHAR(2) USING country_code::VARCHAR(2),
	ALTER COLUMN continent TYPE VARCHAR(255) USING continent::VARCHAR(255);

-- Replace null values with "N/A" in column "locality"
SELECT 
  CASE
    WHEN locality IS NULL
    THEN 'N/A' 
    ELSE locality
  END AS locality
FROM
  dim_store_details;

  SELECT * FROM dim_store_details;

-- Select all the columns of "dim_products"
SELECT * FROM dim_products;

-- Remove the "£" symbol from the "product_price" column
UPDATE dim_products
	SET product_price = REPLACE(product_price, '£', '');

-- Add a new column "weight_class"
ALTER TABLE dim_products
	ADD weight_class varchar(255);

-- This contains human-readable values based on the weight range of the product
UPDATE dim_products
	SET weight_class =
	  CASE 
	    WHEN weight <= 2 THEN 'Light'
		WHEN weight > 3 AND weight <= 40 THEN 'Mid Sized'
		WHEN weight > 41 AND weight <= 140 THEN 'Heavy'
		WHEN weight > 141 THEN 'Truck Required'
		END;

-- Rename the "removed" column to "still_available"
ALTER TABLE dim_products RENAME COLUMN removed TO still_available;

-- Adjust "still_available" by assigning boolean values
UPDATE dim_products
	SET still_available =
	  CASE 
	    WHEN still_available = 'Available' THEN 'True'
		WHEN still_available = 'Removed' THEN 'False'
		END;

-- Get the maximum length of the values in a column
SELECT
	MAX(LENGTH("EAN")) AS ean_max_length,
    MAX(LENGTH(weight_class)) AS weight_class_max_length
FROM
	dim_products;

-- Set a new data types for each colum of the table
ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
    ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
    ALTER COLUMN "EAN" TYPE VARCHAR(17) USING "EAN"::VARCHAR(17),
	ALTER COLUMN product_code TYPE VARCHAR(11) USING product_code::VARCHAR(11),
	ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
	ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
	ALTER COLUMN still_available TYPE BOOLEAN USING still_available::BOOLEAN,
	ALTER COLUMN weight_class TYPE VARCHAR(14) USING weight_class::VARCHAR(14);

-- Select all the columns of "dim_date_times"
SELECT * FROM dim_date_times;

-- Get the maximum length of the values in a column
SELECT
	MAX(LENGTH(month)) AS month_max_length,
    MAX(LENGTH(year)) AS year_max_length,
	MAX(LENGTH(day)) AS day_max_length,
	MAX(LENGTH(time_period)) AS time_period_max_length
FROM
	dim_date_times;
	
-- Set a new data types for each colum of the table
ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE CHAR(2) USING month::CHAR(2),
    ALTER COLUMN year TYPE CHAR(4) USING year::CHAR(4),
    ALTER COLUMN day TYPE CHAR(2) USING day::CHAR(2),
	ALTER COLUMN time_period TYPE VARCHAR(10) USING time_period::VARCHAR(10),
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

-- Select all the columns of "dim_card_details"
SELECT * FROM dim_card_details;

-- Get the maximum length of the values in a column
SELECT
	MAX(LENGTH(card_number)) AS card_number_max_length,
    MAX(LENGTH(expiry_date)) AS expiry_date_max_length
FROM
	dim_card_details;
	
-- Set a new data types for each colum of the table
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(22) USING card_number::VARCHAR(22),
    ALTER COLUMN expiry_date TYPE VARCHAR(5) USING expiry_date::VARCHAR(5),
    ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

-- Remove the "?" symbol from the "card_number" column
SELECT card_number FROM dim_card_details WHERE card_number LIKE '%?%';

UPDATE dim_card_details
	SET card_number = REPLACE(card_number, '?', '');

-- Find the primary key of a table
SELECT
    pg_attribute.attname as column_name,
    format_type(pg_attribute.atttypid, pg_attribute.atttypmod) as data_type
FROM
    pg_index, pg_class, pg_attribute
WHERE
    pg_class.oid = 'table_name'::regclass AND
    indrelid = pg_class.oid AND
    pg_attribute.attrelid = pg_class.oid AND
    pg_attribute.attnum = any(pg_index.indkey)
    AND indisprimary;

-- Create the primary keys in the dimension tables
ALTER TABLE dim_card_details
	ADD CONSTRAINT card_number PRIMARY KEY (card_number);

ALTER TABLE dim_date_times
	ADD CONSTRAINT date_uuid PRIMARY KEY (date_uuid);

ALTER TABLE dim_products
	ADD CONSTRAINT product_code PRIMARY KEY (product_code);

ALTER TABLE dim_store_details
	ADD CONSTRAINT store_code PRIMARY KEY (store_code);

ALTER TABLE dim_users
	ADD CONSTRAINT user_uuid PRIMARY KEY (user_uuid);

-- Add a new row of data into the table 
INSERT INTO dim_store_details (store_code, staff_numbers, opening_date, store_type)
VALUES ('WEB-1388012W', '325', '2010-06-12', 'Web Portal');

-- Delete all orders that have product codes not present in the table
DELETE FROM orders_table
	WHERE product_code NOT IN (SELECT product_code FROM dim_products);

DELETE FROM orders_table
	WHERE store_code NOT IN (SELECT store_code FROM dim_store_details);

-- Add the foreign keys constraints
ALTER TABLE orders_table
	ADD CONSTRAINT card_number
	FOREIGN KEY (card_number)
	REFERENCES dim_card_details (card_number),
	
	ADD CONSTRAINT date_uuid
	FOREIGN KEY (date_uuid)
	REFERENCES dim_date_times (date_uuid),

	ADD CONSTRAINT product_code
	FOREIGN KEY (product_code)
	REFERENCES dim_products (product_code),
	
	ADD CONSTRAINT store_code
	FOREIGN KEY (store_code)
	REFERENCES dim_store_details (store_code),
	
	ADD CONSTRAINT user_uuid
	FOREIGN KEY (user_uuid)
	REFERENCES dim_users (user_uuid);
