-- q1. Find most popular device, browser, OS for each city.
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
