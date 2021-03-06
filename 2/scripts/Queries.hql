-- Total number of flights per carrier in 2007:
SELECT c.description, count(*) cnt
FROM flights f
LEFT JOIN carriers c
	ON (c.code = f.unique_carrier)
GROUP BY c.description;

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
-- V.2
SELECT aps.airport AS airport, count(*) AS workload
FROM (SELECT airlines.dest AS airport, airlines.month FROM airlines
	UNION ALL
	SELECT airlines.origin AS airport, airlines.month FROM airlines) aps, airports
WHERE 
	(aps.airport = airports.iata) 
	AND aps.month > 5 
	AND aps.month < 9
	AND airports.country = 'USA'
GROUP BY aps.airport, airports.airport
ORDER BY workload DESC
LIMIT 5;

-- Find the carrier who served the biggest number of flights.
-- LEFT SEMI JOIN can be used to boost performance. However, LEFT JOIN is used to provide improved readability.
SELECT c.description, count(*) AS cnt
FROM flights f
LEFT JOIN carriers c
	ON (c.code = f.unique_carrier)
GROUP BY c.description
ORDER BY cnt DESC LIMIT 1;
-- V.2
SELECT flights.unique_carrier, carriers.description, count(*) AS cnt
FROM flights, carriers
WHERE (flights.unique_carrier = carriers.code)
GROUP BY flights.unique_carrier, carriers.description
ORDER BY cnt DESC
LIMIT 1;

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

-- Find most popular device, browser, OS for each city.
SELECT c.city_name, r.key, r.value
FROM (
	SELECT p.city_id, p.key, p.value, row_number()
  	OVER (partition by p.city_id, p.key ORDER BY cnt DESC) rowNo
  	FROM (
	  	SELECT b.city_id, key, value, count(*) cnt
	  	FROM bids b
		LATERAL VIEW explode(user_agent_to_map(b.user_agent)) u AS key, value
  		WHERE key != 'UA'
	  	GROUP BY b.city_id, key, value
	) p
) r
LEFT JOIN cities c
	ON (r.city_id = c.city_id)
WHERE r.rowNo = 1
ORDER BY c.city_name DESC;

!connect jdbc:hive2://
!connect jdbc:hive2://localhost:10000 org.apache.hive.jdbc.HiveDriver

CREATE FUNCTION user_agent_to_map AS 'parsers.UserAgentParser' USING JAR 'hdfs:///user/maria_dev/module2-1.0.jar';
DROP FUNCTION user_agent_to_map;
