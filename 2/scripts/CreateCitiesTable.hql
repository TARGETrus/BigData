CREATE EXTERNAL TABLE IF NOT EXISTS cities
(
	city_id INT,
	city_name STRING
)
COMMENT 'Data about cities'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/user/maria_dev/input/hw3_cache/'
