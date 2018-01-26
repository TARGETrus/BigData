CREATE EXTERNAL TABLE IF NOT EXISTS flights_i
(
	year INT,
	month INT,
    day_of_month INT,
    day_of_week INT,
    dep_time INT,
    crs_deptime INT,
    arr_time INT,
    crs_arr_time INT,
    unique_carrier STRING,
    flight_num INT,
    tail_num STRING,
    actual_elapsed_time INT,
    crs_elapsed_time INT,
    airtime INT,
    arr_delay INT,
    dep_delay INT,
    origin STRING,
    dest STRING,
    distance INT,
    taxi_in INT,
    taxi_out INT,
    canceled INT,
    cancellation_code STRING,
    diverted INT,
    carrier_delay INT,
    weather_delay INT,
    nas_delay INT,
    security_delay INT,
    late_aircraft_delay INT
)
COMMENT 'Intermediate external teable for flights data'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/maria_dev/input_hive/flights/'
TBLPROPERTIES("skip.header.line.count"="1");
