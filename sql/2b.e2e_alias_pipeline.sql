
-- 1) Canonicalize Alphabet once: GOOG → GOOGL (prevents double counting)
UPDATE `stocks-gdelt.gdelt_analysis.gdelt_company_aliases`
SET symbol = 'GOOGL'
WHERE symbol IN ('GOOG','GOOGL');

-- Refresh normalized aliases after canonicalization
CREATE OR REPLACE TABLE `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_norm` AS
SELECT
  symbol,
  `stocks-gdelt.gdelt_analysis`.NORMALIZE(alias) AS alias_norm,
  SPLIT(`stocks-gdelt.gdelt_analysis`.NORMALIZE(alias), ' ')[OFFSET(0)] AS first_token
FROM `stocks-gdelt.gdelt_analysis.gdelt_company_aliases`
WHERE alias IS NOT NULL AND LENGTH(`stocks-gdelt.gdelt_analysis`.NORMALIZE(alias)) >= 2;

-- 2) Ambiguous tickers (words, acronyms, CAMEO overlaps)
CREATE TABLE IF NOT EXISTS `stocks-gdelt.gdelt_analysis.ambiguous_tickers` (symbol STRING);

MERGE `stocks-gdelt.gdelt_analysis.ambiguous_tickers` T
USING (
  SELECT symbol
  FROM UNNEST(['COP','CRM','CAT','LOW','ALL','SO','GE','A','C','T','X']) AS symbol
) S
ON T.symbol = S.symbol
WHEN NOT MATCHED THEN INSERT (symbol) VALUES (S.symbol);

-- 3) Blocklist table + minimal safe seeds
CREATE TABLE IF NOT EXISTS `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_blocklist` (
  symbol STRING,
  phrase STRING  -- case-insensitive regex
);

-- Seed (idempotent)
MERGE `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_blocklist` T
USING (
  SELECT 'AAPL' AS symbol, r'(?i)\bapple(s)?\s+(orchard|sauce|blossom|tree|trees|cider|juice|harvest)\b' AS phrase UNION ALL
  SELECT 'COP'  AS symbol, r'(?i)\bpolice\b' AS phrase UNION ALL
  SELECT 'COP'  AS symbol, r'(?i)\bcops?\b' AS phrase UNION ALL
  SELECT 'COP'  AS symbol, r'(?i)\blaw\s*enforcement\b' AS phrase UNION ALL
  -- narrower police-only text 
  SELECT 'COP'  AS symbol, r'(?i)\bpolice\s+officer(s)?\b' AS phrase UNION ALL
  SELECT 'COP'  AS symbol, r'(?i)\barrest(s|ed|ing)?\b'    AS phrase
) S
ON T.symbol = S.symbol AND T.phrase = S.phrase
WHEN NOT MATCHED THEN INSERT (symbol, phrase) VALUES (S.symbol, S.phrase);
