CREATE VIEW V_NOTPROCESSED as 
SELECT DISTINCT d.key, d.date, d.index, i.region 
FROM data d
LEFT JOIN processed p ON (d.date = p.date) AND (d.index = p.index)
JOIN info i on (d.index = i.index)
	WHERE p.date IS NULL