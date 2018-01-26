CREATE EXTERNAL TABLE IF NOT EXISTS airports
(
	iata STRING,
	airport STRING,
	city STRING,
	state STRING,
	country STRING,
	latitude DECIMAL,
	longitude DECIMAL
)
COMMENT 'Data about airports'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
) 
STORED AS TEXTFILE
LOCATION '/user/maria_dev/input_hive/airports/'
TBLPROPERTIES("skip.header.line.count"="1");
