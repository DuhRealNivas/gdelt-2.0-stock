CREATE OR REPLACE TABLE `stocks-gdelt.gdelt_analysis.sp100_universe_history` AS
SELECT DISTINCT
  UPPER(Symbol) AS ticker,
  DATE '2019-01-01' AS start_date,
  DATE '9999-12-31' AS end_date
FROM `stocks-gdelt.gdelt_analysis.sp100_constituents`;
