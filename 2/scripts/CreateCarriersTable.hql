CREATE EXTERNAL TABLE IF NOT EXISTS carriers
(
	code STRING,
	description STRING
)
COMMENT 'Data about carriers'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
) 
STORED AS TEXTFILE
LOCATION '/user/maria_dev/input_hive/carriers/'
TBLPROPERTIES("skip.header.line.count"="1");
