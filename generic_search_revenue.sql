WITH session_data AS (
  SELECT
    event_date AS date,
    CONCAT(CAST(user_pseudo_id AS STRING), CAST(session_id AS STRING)) AS session_id,
    CONCAT(
      COALESCE(ARRAY_AGG((CASE WHEN gclid IS NOT NULL THEN 'google' ELSE manual_source END) IGNORE NULLS ORDER BY event_timestamp)[SAFE_OFFSET(0)],'(direct)'),
      ' / ',
      COALESCE(ARRAY_AGG((CASE WHEN gclid IS NOT NULL THEN 'cpc' ELSE manual_medium END) IGNORE NULLS ORDER BY event_timestamp)[SAFE_OFFSET(0)], '(none)')
    )
     AS source_medium,
    CASE WHEN event_name = 'page_view' AND ep_entrances = 1 THEN ep_page_location END AS landing_page,
    hostname,
    country
  FROM `tough-healer-395417.BI.LAW_flat_events`
  WHERE event_date BETWEEN '20240824' AND '20240830'
    AND hostname = 'www.laithwaites.com'
    AND country != 'India'
  GROUP BY ALL
)
  SELECT
    sd.date,
    sd.session_id,
    sd.source_medium,
    sd.landing_page,
    td.transaction_id,
    SUM(td.purchase_revenue) AS revenue
  FROM session_data sd
  LEFT JOIN `tough-healer-395417.BI.LAW_flat_events` td
  ON CONCAT(td.user_pseudo_id, td.session_id) = sd.session_id
  WHERE td.event_date BETWEEN '20240824' AND '20240830'
    AND td.purchase_revenue IS NOT NULL
    AND REGEXP_CONTAINS(sd.source_medium, r'.* \/ organic$|\(direct\) \/ \(none\)|\(not set\)')
    AND REGEXP_CONTAINS(sd.landing_page, r'.*\/wine-blog.*|.*\/product.*|.*\/wines.*')
    AND NOT REGEXP_CONTAINS(sd.landing_page, r'promoCode=')
  GROUP BY ALL
