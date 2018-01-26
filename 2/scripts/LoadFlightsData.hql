set hive.exec.dynamic.partition.mode = nonstrict;
set hive.enforce.bucketing = true;  
INSERT OVERWRITE TABLE flights PARTITION (year, month) 
SELECT
	day_of_month, day_of_week, dep_time, crs_deptime, arr_time, crs_arr_time, unique_carrier,
    flight_num, tail_num, actual_elapsed_time, crs_elapsed_time, airtime, arr_delay, dep_delay, origin,
    dest, distance, taxi_in, taxi_out, canceled, cancellation_code, diverted, carrier_delay, weather_delay,
    nas_delay, security_delay, late_aircraft_delay, year, month
FROM flights_i;
set hive.exec.dynamic.partition.mode = strict;
set hive.enforce.bucketing = flase;
