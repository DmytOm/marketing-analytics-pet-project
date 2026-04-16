

SELECT
    date,
    channel,
    spend
FROM {{ ref('stg_ad_spend') }}
WHERE spend <= 0
