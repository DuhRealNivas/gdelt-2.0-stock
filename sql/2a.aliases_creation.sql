DECLARE old_exists BOOL DEFAULT FALSE;

-- 1) NORMALIZE UDF (idempotent)
CREATE OR REPLACE FUNCTION `stocks-gdelt.gdelt_analysis`.NORMALIZE(s STRING)
RETURNS STRING AS (
  REGEXP_REPLACE(
    REGEXP_REPLACE(
      REPLACE(REPLACE(LOWER(IFNULL(s, '')), '&', ' and '), "'", ''),
      r'[,\.!/:\-\(\)\[\]"]', ''
    ),
    r'\s+', ' '
  )
);

-- 2) MANUAL aliases table 
CREATE OR REPLACE TABLE `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_manual` (
  symbol STRING,
  alias  STRING
);

INSERT INTO `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_manual` (symbol, alias)
VALUES
  
  ('AAPL','apple'), ('AAPL','apple inc'), ('AAPL','apple computer'),
  ('MSFT','microsoft'), ('MSFT','microsoft corp'),
  ('GOOGL','alphabet'), ('GOOGL','alphabet inc'), ('GOOGL','google'), ('GOOGL','google parent'),
  ('GOOG','alphabet'), ('GOOG','google'), ('GOOG','alphabet inc'), ('GOOG','google parent'),
  ('AMZN','amazon'), ('AMZN','amazon.com'), ('AMZN','amazon inc'),
  ('META','meta'), ('META','meta platforms'), ('META','facebook'), ('META','fb'),
  ('NVDA','nvidia'), ('NVDA','nvidia corp'),
  ('TSLA','tesla'), ('TSLA','tesla inc'),
  ('NFLX','netflix'),
  ('ADBE','adobe'), ('ADBE','adobe systems'),
  ('CRM','salesforce'), ('CRM','salesforce.com'),
  ('INTC','intel'), ('INTC','intel corp'),
  ('AMD','amd'), ('AMD','advanced micro devices'),
  ('CSCO','cisco'), ('CSCO','cisco systems'),
  ('ORCL','oracle'), ('ORCL','oracle corp'),
  ('IBM','ibm'), ('IBM','international business machines'),
  ('TXN','texas instruments'), ('QCOM','qualcomm'), ('AMAT','applied materials'),

  -- PAYMENTS / FINTECH
  ('V','visa'), ('V','visa inc'),
  ('MA','mastercard'), ('MA','mastercard inc'),
  ('PYPL','paypal'), ('PYPL','paypal holdings'),
  ('AXP','american express'), ('AXP','amex'),

  -- BANKS / BROKERS / AM
  ('JPM','jpmorgan'), ('JPM','jp morgan'), ('JPM','jpmorgan chase'),
  ('BAC','bank of america'),
  ('C','citigroup'), ('C','citi'),
  ('WFC','wells fargo'),
  ('GS','goldman sachs'),
  ('MS','morgan stanley'),
  ('BLK','blackrock'),
  ('SCHW','charles schwab'),
  ('BK','bnymellon'), ('BK','bank of new york mellon'),
  ('PNC','pnc'), ('PNC','pnc financial'),

  -- MEDIA / TELCO
  ('DIS','disney'), ('DIS','walt disney'),
  ('CMCSA','comcast'), ('CMCSA','nbcuniversal'),
  ('T','at&t'), ('T','att'),
  ('VZ','verizon'),
  ('TMUS','t mobile'), ('TMUS','t-mobile'),

  -- STAPLES
  ('PG','procter and gamble'), ('PG','p&g'),
  ('KO','coca cola'), ('KO','coke'),
  ('PEP','pepsico'), ('PEP','pepsi'),
  ('COST','costco'),
  ('WMT','walmart'), ('WMT','wal-mart'),
  ('MDLZ','mondelez'), ('MDLZ','mondelez international'),
  ('MO','altria'),
  ('PM','philip morris'),
  ('CL','colgate'), ('CL','colgate palmolive'),
  ('KMB','kimberly clark'), ('KMB','kimberly-clark'),

  -- DISCRETIONARY / RETAIL / RESTAURANTS
  ('MCD','mcdonalds'), ('MCD','mc donalds'),
  ('NKE','nike'), ('NKE','nike inc'),
  ('SBUX','starbucks'),
  ('HD','home depot'), ('HD','the home depot'),
  ('LOW','lowes'), ('LOW','lowe’s'),
  ('TGT','target'), ('TGT','target corp'),

  -- INDUSTRIALS / AERO
  ('BA','boeing'), ('BA','boeing co'),
  ('CAT','caterpillar'), ('CAT','caterpillar inc'),
  ('HON','honeywell'), ('HON','honeywell international'),
  ('UPS','ups'), ('UPS','united parcel service'),
  ('UNP','union pacific'),
  ('MMM','3m'),
  ('GE','ge'), ('GE','general electric'),
  ('LMT','lockheed'), ('LMT','lockheed martin'),

  -- ENERGY
  ('XOM','exxon'), ('XOM','exxonmobil'), ('XOM','exxon mobil'),
  ('CVX','chevron'), ('CVX','chevron corp'),
  ('COP','conocophillips'),
  ('SLB','schlumberger'), ('SLB','slb'),
  ('EOG','eog resources'),

  -- HEALTHCARE
  ('UNH','unitedhealth'), ('UNH','united health'),
  ('JNJ','johnson and johnson'),
  ('PFE','pfizer'),
  ('MRK','merck'),
  ('ABBV','abbvie'),
  ('ABT','abbott'),
  ('AMGN','amgen'),
  ('GILD','gilead'),
  ('LLY','eli lilly'), ('LLY','lilly'),
  ('BMY','bristol myers'), ('BMY','bristol-myers squibb'),
  ('TMO','thermo fisher'),
  ('DHR','danaher'),
  ('ISRG','intuitive surgical'),
  ('MDT','medtronic'),
  ('SYK','stryker'),
  ('CVS','cvs'), ('CVS','cvs health'),

  -- UTILITIES / RE
  ('NEE','nextera'), ('NEE','next era energy'),
  ('SO','southern company'),
  ('DUK','duke energy'),
  ('PLD','prologis'),
  ('AMT','american tower'),

  -- MATERIALS
  ('LIN','linde'),
  ('APD','air products'),
  ('SHW','sherwin williams'), ('SHW','sherwin-williams'),

  -- BERKSHIRE
  ('BRK.B','berkshire hathaway'), ('BRK.B','berkshire'), ('BRK.B','berkshire hathaway class b');


-- 3) Auto-generated aliases from S&P-100 constituents 
CREATE OR REPLACE TABLE `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_automatic_new`
CLUSTER BY symbol AS
WITH base AS (
  SELECT
    UPPER(Symbol) AS symbol,
    Name AS official_name,
    `stocks-gdelt.gdelt_analysis`.NORMALIZE(Name) AS base_norm
  FROM `stocks-gdelt.gdelt_analysis.sp100_constituents`
  WHERE Symbol IS NOT NULL AND Name IS NOT NULL
),
suffixes AS (
  SELECT suffix FROM UNNEST([
    '', ' inc', ' inc.', ' incorporated',
    ' corp', ' corp.', ' corporation',
    ' co', ' co.', ' company', ' companies',
    ' plc', ' limited', ' ltd', ' ltd.',
    ' llc', ' llp', ' lp',
    ' nv', ' ag', ' sa', ' se',
    ' group', ' holdings', ' holding', ' hldgs', ' hldg',
    ' services', ' technologies', ' technology', ' tech', ' systems', ' solutions'
  ]) AS suffix
),
prefices AS ( SELECT pref FROM UNNEST(['', 'the ']) AS pref ),
expanded AS (
  SELECT b.symbol, b.base_norm AS alias FROM base b
  UNION ALL
  SELECT b.symbol,
         `stocks-gdelt.gdelt_analysis`.NORMALIZE(CONCAT(p.pref, b.official_name, s.suffix)) AS alias
  FROM base b CROSS JOIN prefices p CROSS JOIN suffixes s
  UNION ALL
  SELECT b.symbol, TRIM(CONCAT(b.base_norm, s.suffix)) AS alias
  FROM base b CROSS JOIN suffixes s
),
amp_variants AS (
  SELECT symbol, alias FROM expanded
  UNION ALL
  SELECT symbol, REPLACE(alias, ' and ', ' & ') FROM expanded WHERE alias LIKE '% and %'
  UNION ALL
  SELECT symbol, REPLACE(alias, ' & ', ' and ') FROM expanded WHERE alias LIKE '% & %'
),
collapsed AS (
  SELECT symbol, alias FROM amp_variants
  UNION ALL
  SELECT symbol, REGEXP_REPLACE(alias, r'[\s\-]+', ' ')  FROM amp_variants
  UNION ALL
  SELECT symbol, REGEXP_REPLACE(alias, r'[\s\-]+', '')   FROM amp_variants
)
SELECT DISTINCT symbol, alias
FROM collapsed
WHERE LENGTH(alias) >= 4;

-- 4) If OLD automatic exists, SNAPSHOT it and MERGE old+new; else publish new
SET old_exists = (
  SELECT COUNT(*) > 0
  FROM `stocks-gdelt.gdelt_analysis`.INFORMATION_SCHEMA.TABLES
  WHERE table_name = 'gdelt_company_aliases_automatic'
);  

IF old_exists THEN
  -- Snapshot the old automatic table (timestamped)
  EXECUTE IMMEDIATE FORMAT("""
    CREATE SNAPSHOT TABLE `stocks-gdelt.gdelt_analysis.bak_gdelt_company_aliases_automatic_%s`
    CLONE `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_automatic`
  """, FORMAT_TIMESTAMP('%Y%m%d_%H%M%S', CURRENT_TIMESTAMP()));

  -- Merge (UNION DISTINCT) old + new back into the main automatic table
  CREATE OR REPLACE TABLE `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_automatic`
  CLUSTER BY symbol AS
  SELECT symbol, alias FROM `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_automatic`
  UNION DISTINCT
  SELECT symbol, alias FROM `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_automatic_new`;
ELSE
  -- No old table; promote the NEW staging as the first automatic table
  CREATE OR REPLACE TABLE `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_automatic`
  CLUSTER BY symbol AS
  SELECT * FROM `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_automatic_new`;
END IF;

-- Drop staging
DROP TABLE `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_automatic_new`;

-- 5) Final union: auto ∪ manual 
CREATE OR REPLACE TABLE `stocks-gdelt.gdelt_analysis.gdelt_company_aliases`
CLUSTER BY symbol AS
SELECT symbol, alias FROM `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_automatic`
UNION DISTINCT
SELECT UPPER(symbol) AS symbol, alias FROM `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_manual`;

-- 6) Normalized alias table for matching
CREATE OR REPLACE TABLE `stocks-gdelt.gdelt_analysis.gdelt_company_aliases_norm` AS
SELECT
  symbol,
  `stocks-gdelt.gdelt_analysis`.NORMALIZE(alias) AS alias_norm,
  SPLIT(`stocks-gdelt.gdelt_analysis`.NORMALIZE(alias), ' ')[OFFSET(0)] AS first_token
FROM `stocks-gdelt.gdelt_analysis.gdelt_company_aliases`
WHERE alias IS NOT NULL
  AND LENGTH(`stocks-gdelt.gdelt_analysis`.NORMALIZE(alias)) >= 2;
