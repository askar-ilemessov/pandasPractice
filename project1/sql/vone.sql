
CREATE VIEW vone AS  SELECT v.month,
    v.region,
    max(v.max) AS max
   FROM v_processed v
  WHERE v.region = 'Germany'::text
  GROUP BY v.month, v.region;
