CREATE VIEW V_PROCESSED as
 SELECT EXTRACT(year FROM p.date) AS year,
    TO_CHAR(TO_DATE(EXTRACT(MONTH FROM p.Date)::text, 'MM'), 'Month') AS month,
    max(p.open) AS max,
    min(p.low) AS low,
    i.region,
    i.exchange,
    i.currency
   FROM processed p
     JOIN info i ON i.index = p.index
  GROUP BY i.region, (EXTRACT(year FROM p.date)), (EXTRACT(month FROM p.date)), i.exchange, i.currency;