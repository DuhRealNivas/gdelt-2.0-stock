-- 2) Daily features 2021
CREATE OR REPLACE TABLE `stocks-gdelt.gdelt_analysis.gdelt_company_features_daily_2021`
PARTITION BY observation_date
CLUSTER BY ticker AS
WITH ev AS (SELECT * FROM `stocks-gdelt.gdelt_analysis.gdelt_events_2021`), 
code_hits AS (
  SELECT PARSE_DATE('%Y%m%d', CAST(SQLDATE AS STRING)) AS observation_date,
         GLOBALEVENTID, a.symbol AS ticker, AvgTone, GoldsteinScale, n1, n2
  FROM ev JOIN `stocks-gdelt.gdelt_analysis.company_aliases` a
  ON Actor1Code = a.symbol OR Actor2Code = a.symbol
),
name_hits AS (
  SELECT PARSE_DATE('%Y%m%d', CAST(e.SQLDATE AS STRING)) AS observation_date,
         e.GLOBALEVENTID, a.symbol AS ticker, e.AvgTone, e.GoldsteinScale, e.n1, e.n2
  FROM ev e JOIN `stocks-gdelt.gdelt_analysis.company_aliases` a
    ON REGEXP_CONTAINS(e.n1, r'\b' || a.alias || r'\b')
    OR REGEXP_CONTAINS(e.n2, r'\b' || a.alias || r'\b')
  LEFT JOIN code_hits ch ON ch.GLOBALEVENTID = e.GLOBALEVENTID AND ch.ticker = a.symbol
  WHERE ch.GLOBALEVENTID IS NULL
),
mentions AS (SELECT * FROM code_hits UNION ALL SELECT * FROM name_hits),
-- BLOCKLIST FILTER --
mentions_with_blocklist AS (
  SELECT
    m.*,
    bl.phrase
  FROM mentions m
  LEFT JOIN `stocks-gdelt.gdelt_analysis.company_aliases_blocklist` bl
    ON bl.symbol = m.ticker
   AND (REGEXP_CONTAINS(m.n1, bl.phrase) OR REGEXP_CONTAINS(m.n2, bl.phrase))
)
SELECT
  ticker, observation_date,
  AVG(AvgTone) AS avg_daily_tone,
  COUNT(*)   AS news_volume,
  AVG(GoldsteinScale) AS avg_goldstein_scale
FROM mentions_with_blocklist
WHERE phrase IS NULL 
GROUP BY ticker, observation_date;