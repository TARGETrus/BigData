CREATE TABLE IF NOT EXISTS bids_i
(
	bid_id STRING,
	timestmp TIMESTAMP,
    log_type INT,
    i_pin_you_id BIGINT,
    user_agent STRING,
    ip STRING,
    region_id INT,
    city_id INT,
    ad_exchange INT,
    domain STRING,
    url STRING,
    anonymous_url STRING,
    ad_slot_id BIGINT,
	ad_slot_width INT,
	ad_slot_height INT,
	ad_slot_visibility STRING,
	ad_slot_format STRING,
	ad_slot_floor_price INT,
	creative_id STRING,
	bidding_price INT,
	paying_price INT,
	landing_page_url STRING,
	advertiser_id INT,
	user_profile_ids STRING
)
COMMENT 'Intermediate external teable for bids data'
PARTITIONED BY (year INT, month INT)
CLUSTERED BY (unique_carrier) INTO 3 BUCKETS
STORED AS ORC;