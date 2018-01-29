-- q1. Total number of flights per carrier in 2007:
SELECT c.description, count(*) cnt
FROM flights f
LEFT JOIN carriers c
	ON (c.code = f.unique_carrier)
GROUP BY c.description;

-- q2. Total number of flights served in Jun 2007 by NYC (all airports):
SELECT count(*) AS cnt
FROM flights f 
LEFT JOIN airports o
	ON (o.iata = f.origin)
LEFT JOIN airports d
	ON (d.iata = f.dest)
WHERE f.month = 6 AND (o.city = 'New York' OR d.city = 'New York')
GROUP BY f.month;

-- q3. Find five most busy airports in US during Jun 01 - Aug 31.
SELECT airport, sum(cnt) as summ
FROM (
	SELECT airport, count(*) AS cnt
	FROM flights f 
	LEFT JOIN airports
		ON (country = 'USA' AND iata = f.origin)
	WHERE f.month > 5 and f.month < 9
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

-- q4. Find the carrier who served the biggest number of flights.
-- LEFT SEMI JOIN can be used to boost performance. However, LEFT JOIN is used to provide improved readability.
SELECT c.description, count(*) AS cnt
FROM flights f
LEFT JOIN carriers c
	ON (c.code = f.unique_carrier)
GROUP BY c.description
ORDER BY cnt DESC LIMIT 1;
