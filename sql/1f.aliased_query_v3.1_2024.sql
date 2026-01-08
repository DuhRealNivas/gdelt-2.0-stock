-- A) Slice 2024
CREATE OR REPLACE TABLE `stocks-gdelt.gdelt_analysis.gdelt_events_2024` AS
SELECT
  GLOBALEVENTID, SQLDATE,
  Actor1Name, Actor2Name,
  Actor1Code, Actor2Code,
  AvgTone, GoldsteinScale,
  `stocks-gdelt.gdelt_analysis`.NORMALIZE(Actor1Name) AS n1,
  `stocks-gdelt.gdelt_analysis`.NORMALIZE(Actor2Name) AS n2
FROM `gdelt-bq.gdeltv2.events`
WHERE SQLDATE BETWEEN 20240101 AND 20241231;