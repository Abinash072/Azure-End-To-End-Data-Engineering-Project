-- =====================================================================
-- 02 · External file format (Parquet + Snappy compression)
-- =====================================================================
CREATE EXTERNAL FILE FORMAT format_parquet
WITH (
    FORMAT_TYPE      = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO
