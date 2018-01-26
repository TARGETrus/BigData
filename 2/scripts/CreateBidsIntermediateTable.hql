CREATE EXTERNAL TABLE IF NOT EXISTS bids_i
(
	bid_id STRING,
	timestmp STRING,
    log_type STRING,
    i_pin_you_id STRING,
    user_agent STRING,
    ip STRING,
    region_id STRING,
    city_id STRING,
    ad_exchange STRING,
    domain STRING,
    url STRING,
    anonymous_url STRING,
    ad_slot_id STRING,
	ad_slot_width STRING,
	ad_slot_height STRING,
	ad_slot_visibility STRING,
	ad_slot_format STRING,
	ad_slot_floor_price STRING,
	creative_id STRING,
	bidding_price STRING,
	paying_price STRING,
	landing_page_url STRING,
	advertiser_id STRING,
	user_profile_ids STRING
)
COMMENT 'Intermediate external teable for bids data'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
STORED AS TEXTFILE
LOCATION '/user/maria_dev/input/hw3/';
