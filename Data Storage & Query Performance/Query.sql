SELECT
  c.campaign_id,
  COUNT(DISTINCT ai.user_id) AS total_users,
  COUNT(DISTINCT ai.ad_creative_id) AS total_ads_shown,
  COUNT(CASE WHEN cc.conversion_type = 'click' THEN cc.user_id END) AS total_clicks,
  COUNT(CASE WHEN cc.conversion_type = 'purchase' THEN cc.user_id END) AS total_purchases
FROM
  correlated_data ai
  LEFT JOIN clicks_conversions cc ON ai.user_id = cc.user_id
                                  AND ai.timestamp < cc.event_timestamp
                                  AND cc.event_timestamp < ai.timestamp + INTERVAL '1 HOUR'
  JOIN campaign_details c ON cc.campaign_id = c.campaign_id
WHERE
  ai.timestamp >= '2023-04-30'
  AND ai.timestamp < '2023-05-01'
GROUP BY
  c.campaign_id;