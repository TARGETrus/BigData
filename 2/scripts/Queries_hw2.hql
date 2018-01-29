-- q1. Find all carriers who canceled more than 1 flights during 2007, order them from biggest to lowest by number 
-- of canceled flights and list in each record all departure cities where cancellation happened.
SELECT c.description, count(*) AS cnt, collect_set(a.city)
FROM flights f
LEFT JOIN carriers c
	ON (c.code = f.unique_carrier)
LEFT JOIN airports a
	ON (a.iata = f.origin)
WHERE f.canceled = 1
GROUP BY c.description
HAVING cnt > 1
ORDER BY cnt DESC;
