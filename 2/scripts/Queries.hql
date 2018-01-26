-- Total number of flights per carrier in 2007:
SELECT unique_carrier, count(*)
FROM flights
GROUP BY unique_carrier;

-- Total number of flights served in Jun 2007 by NYC (all airports):
SELECT count(*) AS cnt
FROM flights f 
LEFT JOIN airports o
	ON (o.iata = f.origin)
LEFT JOIN airports d
	ON (d.iata = f.dest)
WHERE f.month = 6 AND (o.city = 'New York' OR d.city = 'New York')
GROUP BY f.month;

-- Find five most busy airports in US during Jun 01 - Aug 31.
SELECT airport, sum(cnt) as summ
FROM (
	SELECT airport, count(*) AS cnt
	FROM flights f 
	LEFT JOIN airports
		ON (country = 'USA' AND iata = origin)
	WHERE f.month >5 and f.month <9
	GROUP BY airport
	UNION
	SELECT airport, count(*) AS cnt
	FROM flights f 
	LEFT JOIN airports
		ON (country = 'USA' AND iata = f.dest)
	WHERE f.month > 5 and f.month < 9
	GROUP BY airport
) ports
GROUP BY airport
ORDER BY summ DESC LIMIT 5;

-- Find the carrier who served the biggest number of flights.
-- LEFT SEMI JOIN can be used to boost performance. However, LEFT JOIN is used to provide improved readability.
SELECT c.description, count(*) AS cnt
FROM flights f
LEFT JOIN carriers c
	ON (c.code = f.unique_carrier)
GROUP BY c.description
ORDER BY cnt DESC LIMIT 1;

-- Find all carriers who canceled more than 1 flights during 2007, order them from biggest to lowest by number 
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

