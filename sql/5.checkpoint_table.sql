CREATE TABLE IF NOT EXISTS `stocks-gdelt.gdelt_analysis.gdelt_daily_checkpoint` (
  next_date DATE
);

-- Seed 
DELETE FROM `stocks-gdelt.gdelt_analysis.gdelt_daily_checkpoint` WHERE TRUE;
INSERT INTO `stocks-gdelt.gdelt_analysis.gdelt_daily_checkpoint` (next_date)
VALUES (DATE '2019-01-01');