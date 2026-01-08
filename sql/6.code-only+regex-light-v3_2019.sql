-- PARAMETERS
DECLARE proj STRING DEFAULT 'stocks-gdelt';
DECLARE ds   STRING DEFAULT 'gdelt_analysis';

DECLARE use_checkpoint BOOL DEFAULT FALSE;                  
DECLARE start_date   DATE DEFAULT DATE '2019-01-01';        -- start of range
DECLARE end_date     DATE DEFAULT DATE '2019-12-31';        -- end of range
DECLARE batch_days   INT64 DEFAULT 200;                   

-- VARIABLES
DECLARE cur DATE;
DECLARE ym  STRING;
DECLARE loop_end DATE;
DECLARE hard_end DATE DEFAULT DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 1 DAY);
DECLARE master_tbl STRING DEFAULT FORMAT('`%s.%s.gdelt_company_features_daily_master_v3`', proj, ds);

-- PICK START
SET cur = start_date;

-- VALIDATE / SET LOOP END
SET end_date = LEAST(end_date, hard_end);
SET loop_end = LEAST(end_date, DATE_ADD(cur, INTERVAL batch_days-1 DAY));

IF cur > end_date THEN
  SELECT 'DONE' AS status, cur AS at_date, end_date AS end_date;
ELSE
  
  EXECUTE IMMEDIATE FORMAT("""
    CREATE TABLE IF NOT EXISTS %s (
      ticker STRING,
      observation_date DATE,
      news_volume INT64,
      avg_daily_tone FLOAT64,
      avg_goldstein_scale FLOAT64
    )
    PARTITION BY observation_date
    CLUSTER BY ticker
  """, master_tbl);

  -- MAIN LOOP
  WHILE cur <= loop_end DO
    SET ym = FORMAT_DATE('%Y%m', cur);

    -- Clear today's rows (idempotent)
    EXECUTE IMMEDIATE FORMAT(
      "DELETE FROM %s WHERE observation_date = DATE('%s')",
      master_tbl, FORMAT_DATE('%F', cur)
    );

    -- Build day text
    EXECUTE IMMEDIATE FORMAT("""
      CREATE OR REPLACE TEMP TABLE tmp_txt AS
      SELECT
        e.GLOBALEVENTID,
        e.effective_date AS observation_date,
        e.AvgTone, e.GoldsteinScale,
        e.n1, e.n2,
        CONCAT(' ', COALESCE(e.n1,''), ' ', COALESCE(e.n2,''), ' ') AS bag
      FROM `%s.%s.gdelt_events_%s` e
      WHERE e.effective_date = DATE('%s')
    """, proj, ds, ym, FORMAT_DATE('%F', cur));

    -- Canonical aliases / norms / ambiguous
    EXECUTE IMMEDIATE FORMAT("""
      CREATE OR REPLACE TEMP TABLE tmp_aliases AS
      SELECT DISTINCT IF(symbol IN ('GOOG','GOOGL'), 'GOOGL', symbol) AS symbol
      FROM `%s.%s.gdelt_company_aliases`
    """, proj, ds);

    EXECUTE IMMEDIATE FORMAT("""
      CREATE OR REPLACE TEMP TABLE tmp_aliases_norm AS
      SELECT IF(symbol IN ('GOOG','GOOGL'), 'GOOGL', symbol) AS symbol,
             alias_norm, first_token
      FROM `%s.%s.gdelt_company_aliases_norm`
    """, proj, ds);

    EXECUTE IMMEDIATE FORMAT("""
      CREATE OR REPLACE TEMP TABLE tmp_ambig AS
      SELECT symbol FROM `%s.%s.ambiguous_tickers`
    """, proj, ds);

    -- PASS 1: FAST
    EXECUTE IMMEDIATE """
      CREATE OR REPLACE TEMP TABLE tmp_fast_hits AS
      WITH safe_code_hits AS (
        SELECT t.GLOBALEVENTID, t.observation_date, a.symbol AS ticker,
               t.AvgTone, t.GoldsteinScale, t.n1, t.n2, t.bag
        FROM tmp_txt t
        JOIN tmp_aliases a ON REGEXP_CONTAINS(t.bag, r'\\b' || a.symbol || r'\\b')
        LEFT JOIN tmp_ambig amb ON amb.symbol = a.symbol
        WHERE amb.symbol IS NULL
      ),
      ambig_code_hits AS (
        SELECT t.GLOBALEVENTID, t.observation_date, a.symbol AS ticker,
               t.AvgTone, t.GoldsteinScale, t.n1, t.n2, t.bag
        FROM tmp_txt t
        JOIN tmp_ambig amb ON TRUE
        JOIN tmp_aliases a ON a.symbol = amb.symbol
        JOIN tmp_aliases_norm an ON an.symbol = a.symbol
        WHERE REGEXP_CONTAINS(t.bag, r'\\b' || a.symbol || r'\\b')
          AND REGEXP_CONTAINS(t.bag, r'\\b' || an.alias_norm || r'\\b')
      ),
      finance_context_hits AS (
        SELECT t.GLOBALEVENTID, t.observation_date, a.symbol AS ticker,
               t.AvgTone, t.GoldsteinScale, t.n1, t.n2, t.bag
        FROM tmp_txt t
        JOIN tmp_ambig amb ON TRUE
        JOIN tmp_aliases a ON a.symbol = amb.symbol
        WHERE REGEXP_CONTAINS(t.bag, r'\\b(?:nyse|nasdaq|lse|tsx)\\W*' || a.symbol || r'\\b')
           OR REGEXP_CONTAINS(t.bag, r'\\$' || LOWER(a.symbol) || r'\\b')
           OR REGEXP_CONTAINS(t.bag, r'\\b' || LOWER(a.symbol) || r'\\.(us|n)\\b')
      ),
      fast_raw AS (
        SELECT * FROM safe_code_hits
        UNION ALL SELECT * FROM ambig_code_hits
        UNION ALL SELECT * FROM finance_context_hits
      ),
      fast_guarded AS (
        SELECT m.*
        FROM fast_raw m
        WHERE NOT (
          m.ticker = 'COP'
          AND REGEXP_CONTAINS(m.bag, r'(?i)\\bpolice\\b|\\bcops?\\b|\\blaw[[:space:]]*enforcement\\b|\\bpolice[[:space:]]+officer(s)?\\b')
          AND NOT REGEXP_CONTAINS(m.bag, r'(?i)\\bconoco[[:space:]]?phillips\\b|\\bconoco\\b')
        )
        AND NOT (
          m.ticker = 'CRM'
          AND REGEXP_CONTAINS(m.bag, r'(?i)\\bcustomer[[:space:]]+relationship[[:space:]]+management\\b')
          AND NOT REGEXP_CONTAINS(m.bag, r'(?i)\\bsalesforce\\b')
        )
      ),
      fast_clean AS (
        SELECT m.*
        FROM fast_guarded m
        LEFT JOIN `""" || proj || "." || ds || """`.gdelt_company_aliases_blocklist bl
          ON bl.symbol = m.ticker
         AND REGEXP_CONTAINS(m.bag, bl.phrase)
        WHERE bl.phrase IS NULL
      )
      SELECT * FROM fast_clean
    """;

    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO %s (ticker, observation_date, news_volume, avg_daily_tone, avg_goldstein_scale)
      WITH universe AS (
        SELECT ticker, start_date, end_date FROM `%s.%s.sp100_universe_history`
      )
      SELECT
        f.ticker,
        DATE('%s') AS observation_date,
        COUNT(DISTINCT f.GLOBALEVENTID) AS news_volume,
        AVG(f.AvgTone) AS avg_daily_tone,
        AVG(f.GoldsteinScale) AS avg_goldstein_scale
      FROM tmp_fast_hits f
      JOIN universe u ON u.ticker = f.ticker AND DATE('%s') BETWEEN u.start_date AND u.end_date
      WHERE f.GoldsteinScale IS NOT NULL AND f.AvgTone IS NOT NULL
      GROUP BY f.ticker
    """, master_tbl, proj, ds, FORMAT_DATE('%F', cur), FORMAT_DATE('%F', cur));

    -- PASS 2: REGEX 
    EXECUTE IMMEDIATE """
      CREATE OR REPLACE TEMP TABLE tmp_fast_keys AS
      SELECT DISTINCT GLOBALEVENTID, ticker FROM tmp_fast_hits
    """;

    EXECUTE IMMEDIATE """
      CREATE OR REPLACE TEMP TABLE tmp_regex_hits AS
      WITH cand AS (
        SELECT t.*, an.symbol AS ticker, an.alias_norm, an.first_token
        FROM tmp_txt t
        JOIN tmp_aliases_norm an
          ON ( (t.n1 IS NOT NULL AND STRPOS(t.n1, an.first_token) > 0)
            OR (t.n2 IS NOT NULL AND STRPOS(t.n2, an.first_token) > 0) )
        LEFT JOIN tmp_fast_keys k
          ON k.GLOBALEVENTID = t.GLOBALEVENTID AND k.ticker = an.symbol
        WHERE k.GLOBALEVENTID IS NULL
      ),
      regex_raw AS (
        SELECT
          c.GLOBALEVENTID, c.observation_date, c.ticker,
          c.AvgTone, c.GoldsteinScale, c.n1, c.n2, c.bag
        FROM cand c
        WHERE (c.n1 IS NOT NULL AND REGEXP_CONTAINS(c.n1, r'\\b' || c.alias_norm || r'\\b'))
           OR (c.n2 IS NOT NULL AND REGEXP_CONTAINS(c.n2, r'\\b' || c.alias_norm || r'\\b'))
      ),
      regex_guarded AS (
        SELECT m.*
        FROM regex_raw m
        WHERE NOT (
          m.ticker = 'COP'
          AND REGEXP_CONTAINS(m.bag, r'(?i)\\bpolice\\b|\\bcops?\\b|\\blaw[[:space:]]*enforcement\\b|\\bpolice[[:space:]]+officer(s)?\\b')
          AND NOT REGEXP_CONTAINS(m.bag, r'(?i)\\bconoco[[:space:]]?phillips\\b|\\bconoco\\b')
        )
        AND NOT (
          m.ticker = 'CRM'
          AND REGEXP_CONTAINS(m.bag, r'(?i)\\bcustomer[[:space:]]+relationship[[:space:]]+management\\b')
          AND NOT REGEXP_CONTAINS(m.bag, r'(?i)\\bsalesforce\\b')
        )
      ),
      regex_clean AS (
        SELECT m.*
        FROM regex_guarded m
        LEFT JOIN `""" || proj || "." || ds || """`.gdelt_company_aliases_blocklist bl
          ON bl.symbol = m.ticker
         AND REGEXP_CONTAINS(m.bag, bl.phrase)
        WHERE bl.phrase IS NULL
      )
      SELECT * FROM regex_clean
    """;

    EXECUTE IMMEDIATE FORMAT("""
      INSERT INTO %s (ticker, observation_date, news_volume, avg_daily_tone, avg_goldstein_scale)
      WITH universe AS (
        SELECT ticker, start_date, end_date FROM `%s.%s.sp100_universe_history`
      )
      SELECT
        r.ticker,
        DATE('%s') AS observation_date,
        COUNT(DISTINCT r.GLOBALEVENTID) AS news_volume,
        AVG(r.AvgTone) AS avg_daily_tone,
        AVG(r.GoldsteinScale) AS avg_goldstein_scale
      FROM tmp_regex_hits r
      JOIN universe u ON u.ticker = r.ticker AND DATE('%s') BETWEEN u.start_date AND u.end_date
      WHERE r.GoldsteinScale IS NOT NULL AND r.AvgTone IS NOT NULL
      GROUP BY r.ticker
    """, master_tbl, proj, ds, FORMAT_DATE('%F', cur), FORMAT_DATE('%F', cur));

    -- Advance one day
    SET cur = DATE_ADD(cur, INTERVAL 1 DAY);

    -- No checkpoint updates in range mode
    SELECT 'OK' AS status, DATE_SUB(cur, INTERVAL 1 DAY) AS processed_day;
  END WHILE;

  SELECT 'DONE (range)' AS status, DATE_SUB(cur, INTERVAL 1 DAY) AS last_processed, loop_end AS loop_end;
END IF;
