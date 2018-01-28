set hive.enforce.bucketing = true;  
INSERT OVERWRITE TABLE bids
SELECT * FROM bids_i;
set hive.enforce.bucketing = flase;
