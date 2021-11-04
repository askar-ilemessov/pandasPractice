CREATE VIEW vtwo AS
SELECT v.month,
    v.region,
    min(v.low) AS low
   FROM v_processed v
  WHERE v.region = 'Germany'::text
  GROUP BY v.month, v.region