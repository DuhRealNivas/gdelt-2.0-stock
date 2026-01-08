-- Ambiguous tickers (idempotent)
CREATE TABLE IF NOT EXISTS `stocks-gdelt.gdelt_analysis.ambiguous_tickers` (symbol STRING);

MERGE `stocks-gdelt.gdelt_analysis.ambiguous_tickers` T
USING (
  SELECT symbol
  FROM UNNEST(['COP','CRM','CAT','LOW','ALL','SO','GE','A','C','T','X']) AS symbol
) S
ON T.symbol = S.symbol
WHEN NOT MATCHED THEN INSERT (symbol) VALUES (S.symbol);

-- Canonicalize Alphabet once (GOOG -> GOOGL), then refresh normalized aliases
UPDATE `stocks-gdelt.gdelt_analysis.gdelt_company_aliases`
SET symbol = 'GOOGL'
WHERE symbol IN ('GOOG','GOOGL');

CREATE OR REPLACE TABLE `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_norm` AS
SELECT
  UPPER(symbol) AS symbol,
  `stocks-gdelt.gdelt_analysis`.NORMALIZE(alias) AS alias_norm,
  SPLIT(`stocks-gdelt.gdelt_analysis`.NORMALIZE(alias), ' ')[OFFSET(0)] AS first_token
FROM `stocks-gdelt.gdelt_analysis.gdelt_company_aliases`
WHERE alias IS NOT NULL AND LENGTH(alias) >= 2;

-- Ensure blocklist table exists (idempotent guard)
CREATE TABLE IF NOT EXISTS `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_blocklist` (
  symbol STRING,
  phrase STRING
);

-- COP blocklist patch
DELETE FROM `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_blocklist`
WHERE symbol='COP' AND phrase = r'(?i)\bofficers?\b';

MERGE `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_blocklist` T
USING (
  SELECT 'COP' AS symbol, r'(?i)\bpolice\s+officer(s)?\b' AS phrase UNION ALL
  SELECT 'COP' AS symbol, r'(?i)\barrest(s|ed|ing)?\b'    AS phrase
) S
ON T.symbol = S.symbol AND T.phrase = S.phrase
WHEN NOT MATCHED THEN INSERT (symbol, phrase) VALUES (S.symbol, S.phrase);
