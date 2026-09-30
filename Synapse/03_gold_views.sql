-- =====================================================================
-- 03 · Gold views over the Silver Parquet files (serverless OPENROWSET)
-- CREATE OR ALTER makes this script safe to re-run.
-- Replace <storage-account> with your ADLS Gen2 account name.
-- =====================================================================

-- Calendar
CREATE OR ALTER VIEW gold.calendar
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.blob.core.windows.net/silver/AdventureWorks_Calendar/*.parquet',
    FORMAT = 'PARQUET'
) AS QUERY2;
GO

-- Customers
CREATE OR ALTER VIEW gold.customers
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.blob.core.windows.net/silver/AdventureWorks_Customers/*.parquet',
    FORMAT = 'PARQUET'
) AS QUERY2;
GO

-- Product Categories
CREATE OR ALTER VIEW gold.product_categories
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.blob.core.windows.net/silver/AdventureWorks_Product_Categories/*.parquet',
    FORMAT = 'PARQUET'
) AS QUERY2;
GO

-- Product Subcategories
CREATE OR ALTER VIEW gold.product_subcategories
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.blob.core.windows.net/silver/AdventureWorks_Product_Subcategories/*.parquet',
    FORMAT = 'PARQUET'
) AS QUERY2;
GO

-- Products
CREATE OR ALTER VIEW gold.products
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.blob.core.windows.net/silver/AdventureWorks_Products/*.parquet',
    FORMAT = 'PARQUET'
) AS QUERY2;
GO

-- Returns
CREATE OR ALTER VIEW gold.returns
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.blob.core.windows.net/silver/AdventureWorks_Returns/*.parquet',
    FORMAT = 'PARQUET'
) AS QUERY2;
GO

-- Sales 2015
CREATE OR ALTER VIEW gold.sales_2015
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.blob.core.windows.net/silver/AdventureWorks_Sales_2015/*.parquet',
    FORMAT = 'PARQUET'
) AS QUERY2;
GO

-- Sales 2016
CREATE OR ALTER VIEW gold.sales_2016
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.blob.core.windows.net/silver/AdventureWorks_Sales_2016/*.parquet',
    FORMAT = 'PARQUET'
) AS QUERY2;
GO

-- Sales 2017
CREATE OR ALTER VIEW gold.sales_2017
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.blob.core.windows.net/silver/AdventureWorks_Sales_2017/*.parquet',
    FORMAT = 'PARQUET'
) AS QUERY2;
GO

-- Territories
CREATE OR ALTER VIEW gold.territories
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<storage-account>.blob.core.windows.net/silver/AdventureWorks_Territories/*.parquet',
    FORMAT = 'PARQUET'
) AS QUERY2;
GO
