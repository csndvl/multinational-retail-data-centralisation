--TASK 1: Change order table columns into the correct data types

-- Find the longest card number length
SELECT MAX(LENGTH(card_number::TEXT)) FROM order_table
SET LIMIT 1; --19

-- Find the longest store code length
SELECT MAX(LENGTH(store_code::TEXT)) FROM order_table
SET LIMIT 1; --12

-- Find the longest product code length
SELECT MAX(LENGTH(product_code::TEXT)) FROM order_table
SET LIMIT 1; --11

-- Alter column data types
ALTER TABLE order_table
ALTER COLUMN date_uuid TYPE UUID
USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE UUID
USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN product_quantity TYPE SMALLINT;









