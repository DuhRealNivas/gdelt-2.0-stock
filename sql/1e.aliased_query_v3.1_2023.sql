-- A) Slice 2023
CREATE OR REPLACE TABLE `stocks-gdelt.gdelt_analysis.gdelt_events_2023` AS
SELECT
  GLOBALEVENTID, SQLDATE,
  Actor1Name, Actor2Name,
  Actor1Code, Actor2Code,
  AvgTone, GoldsteinScale,
  `stocks-gdelt.gdelt_analysis`.NORMALIZE(Actor1Name) AS n1,
  `stocks-gdelt.gdelt_analysis`.NORMALIZE(Actor2Name) AS n2
FROM `gdelt-bq.gdeltv2.events`
WHERE SQLDATE BETWEEN 20230101 AND 20231231;