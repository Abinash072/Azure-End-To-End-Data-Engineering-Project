-- =====================================================================
-- 04 · External tables (CETAS): persist each Gold view as Parquet in the
--      gold container, still queryable through SQL.
-- Requires: 01_external_data_sources.sql, 02_external_file_formats.sql,
--           03_gold_views.sql
-- Note: CETAS fails if the table or target folder already exists. To re-run,
--       DROP EXTERNAL TABLE <name> and delete its folder in the gold container.
-- =====================================================================

-- Calendar
CREATE EXTERNAL TABLE ext_calendar
WITH (
    LOCATION    = 'calendar',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS
SELECT * FROM gold.calendar;
GO

-- Customers
CREATE EXTERNAL TABLE ext_customer
WITH (
    LOCATION    = 'customer',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS
SELECT * FROM gold.customers;
GO

-- Product categories
CREATE EXTERNAL TABLE ext_product_catagory
WITH (
    LOCATION    = 'product_catagory',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS
SELECT * FROM gold.product_categories;
GO

-- Product subcategories
CREATE EXTERNAL TABLE ext_products_subcat
WITH (
    LOCATION    = 'product_sub_catagory',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS
SELECT * FROM gold.product_subcategories;
GO

-- Products
CREATE EXTERNAL TABLE ext_products
WITH (
    LOCATION    = 'products',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS
SELECT * FROM gold.products;
GO

-- Returns
CREATE EXTERNAL TABLE ext_return
WITH (
    LOCATION    = 'returns',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS
SELECT * FROM gold.returns;
GO

-- Sales 2015
CREATE EXTERNAL TABLE sales_2015
WITH (
    LOCATION    = 'sales_2015',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS
SELECT * FROM gold.sales_2015;
GO

-- Sales 2016
CREATE EXTERNAL TABLE sales_2016
WITH (
    LOCATION    = 'sales_2016',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS
SELECT * FROM gold.sales_2016;
GO

-- Sales 2017
CREATE EXTERNAL TABLE sales_2017
WITH (
    LOCATION    = 'sales_2017',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS
SELECT * FROM gold.sales_2017;
GO

-- Territories
CREATE EXTERNAL TABLE Territories
WITH (
    LOCATION    = 'Territories',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS
SELECT * FROM gold.territories;
GO
