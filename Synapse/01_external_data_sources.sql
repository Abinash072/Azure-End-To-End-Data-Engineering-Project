-- =====================================================================
-- 01 · Credential and external data sources
-- Replace <storage-account> with your ADLS Gen2 account name.
-- =====================================================================

-- A database master key must exist before creating a scoped credential.
-- Run once per database if you have not already:
-- CREATE MASTER KEY ENCRYPTION BY PASSWORD = '<strong-password>';

CREATE DATABASE SCOPED CREDENTIAL cred_papu
WITH IDENTITY = 'Managed Identity';
GO

-- Silver: cleaned Parquet written by Databricks
CREATE EXTERNAL DATA SOURCE source_silver
WITH (
    LOCATION   = 'https://<storage-account>.dfs.core.windows.net/silver/',
    CREDENTIAL = cred_papu
);
GO

-- Gold: output location for the external tables (CETAS)
CREATE EXTERNAL DATA SOURCE source_gold
WITH (
    LOCATION   = 'https://<storage-account>.dfs.core.windows.net/gold/',
    CREDENTIAL = cred_papu
);
GO
