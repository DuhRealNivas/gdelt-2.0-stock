CREATE OR REPLACE EXTERNAL TABLE `stocks-gdelt.gdelt_analysis.sp100_constituents`
(
  Symbol STRING,
  Name   STRING,
  Sector STRING
)
WITH CONNECTION `projects/stocks-gdelt/locations/us/connections/bigquery-gcs-bucket`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://sp-100-gdelt-query/sp100_tickers.parquet'],
  metadata_cache_mode = 'AUTOMATIC',
  max_staleness = INTERVAL 4 HOUR
);
