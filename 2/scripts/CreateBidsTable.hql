CREATE TABLE IF NOT EXISTS bids
(
	bid_id STRING,
	timestmp STRING,
    log_type INT,
    i_pin_you_id STRING,
    user_agent STRING,
    ip STRING,
    region_id INT,
    city_id INT,
    ad_exchange STRING,
    domain STRING,
    url STRING,
    anonymous_url STRING,
    ad_slot_id STRING,
	ad_slot_width INT,
	ad_slot_height INT,
	ad_slot_visibility STRING,
	ad_slot_format STRING,
	ad_slot_floor_price BIGINT,
	creative_id INT,
	bidding_price BIGINT,
	paying_price BIGINT,
	landing_page_url STRING,
	advertiser_id INT,
	user_profile_ids array<STRING>
)
COMMENT 'Data about bids'
CLUSTERED BY (bid_id) INTO 4 BUCKETS
STORED AS ORC;
