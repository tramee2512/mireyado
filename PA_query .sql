WITH events AS (
  SELECT
    user_pseudo_id,
    event_name,
    event_timestamp,                                 
    PARSE_DATE('%Y%m%d', event_date) AS event_day,
    category, 
    mobile_brand_name, 
    mobile_model_name, 
    operating_system,
    language, 
    browser, 
    medium, 
    campaign
  FROM `turing_data_analytics.raw_events`
),
first_arrival_row AS (
  SELECT
    user_pseudo_id,
    event_day,
    event_timestamp AS first_arrival_ts,
    event_name      AS daily_first_event_name,   
    category        AS device,
    mobile_brand_name,
    mobile_model_name,
    operating_system,
    language,
    browser,
    medium,
    campaign,
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id, event_day
      ORDER BY event_timestamp
    ) AS rn
  FROM events
),
first_arrival AS (
  SELECT * EXCEPT(rn)
  FROM first_arrival_row
  WHERE rn = 1
),
first_purchase AS (
  SELECT
    user_pseudo_id,
    event_day,
    MIN(event_timestamp) AS first_purchase_ts
  FROM events
  WHERE event_name = 'purchase'
  GROUP BY user_pseudo_id, event_day
)
  SELECT
    fa.user_pseudo_id,
    fa.event_day,
    fa.daily_first_event_name,                        
    TIMESTAMP_MICROS(fa.first_arrival_ts)  AS arrival_time,
    TIMESTAMP_MICROS(fp.first_purchase_ts) AS purchase_time,
    ROUND( (fp.first_purchase_ts - fa.first_arrival_ts) / 1000000.0 / 60.0, 2 ) AS duration_minutes,
    fa.device, 
    fa.mobile_brand_name, 
    fa.mobile_model_name, 
    fa.operating_system,
    fa.language, 
    fa.browser, 
    fa.browser_version, 
    fa.country, 
    fa.medium, 
    fa.campaign
  FROM first_arrival fa
  JOIN first_purchase fp
    ON fa.user_pseudo_id = fp.user_pseudo_id
   AND fa.event_day      = fp.event_day
  WHERE fp.first_purchase_ts > fa.first_arrival_ts  

