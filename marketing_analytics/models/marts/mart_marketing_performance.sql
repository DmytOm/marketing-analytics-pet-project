WITH ad_spend AS (

    SELECT * FROM {{ ref('stg_ad_spend') }}

),

sessions AS (

    SELECT * FROM {{ ref('stg_sessions') }}

),

orders AS (

    SELECT * FROM {{ ref('stg_orders') }}

),

sessions_by_channel AS (

    SELECT
        channel,
        DATE(DATE_TRUNC('month', started_at))    AS month,
        COUNT(session_id)                  AS total_sessions
    FROM sessions
    GROUP BY 1, 2

),

orders_by_channel AS (

    SELECT
        sessions.channel,
        DATE(DATE_TRUNC('month', orders.ordered_at))    AS month,
        COUNT(orders.order_id)                    AS total_orders,
        SUM(orders.amount)                        AS total_revenue
    FROM orders
    LEFT JOIN sessions
        ON orders.session_id = sessions.session_id
    GROUP BY 1, 2

),

ad_spend_by_channel AS (

    SELECT
        channel,
        DATE(DATE_TRUNC('month', date))    AS month,
        SUM(spend)                   AS total_spend,
        SUM(impressions)             AS total_impressions,
        SUM(clicks)                  AS total_clicks
    FROM ad_spend
    GROUP BY 1, 2

),

final AS (

    SELECT
        ad_spend_by_channel.channel,
        ad_spend_by_channel.month,
        ad_spend_by_channel.total_spend,
        ad_spend_by_channel.total_impressions,
        ad_spend_by_channel.total_clicks,
        sessions_by_channel.total_sessions,
        orders_by_channel.total_orders,
        orders_by_channel.total_revenue,
        ROUND(
            orders_by_channel.total_revenue / NULLIF(ad_spend_by_channel.total_spend, 0), 2
        )                                                                AS roas,
        ROUND(
            ad_spend_by_channel.total_spend / NULLIF(orders_by_channel.total_orders, 0), 2
        )                                                                AS cac
    FROM ad_spend_by_channel
    LEFT JOIN sessions_by_channel
        ON ad_spend_by_channel.channel = sessions_by_channel.channel
        AND ad_spend_by_channel.month = sessions_by_channel.month
    LEFT JOIN orders_by_channel
        ON ad_spend_by_channel.channel = orders_by_channel.channel
        AND ad_spend_by_channel.month = orders_by_channel.month

)

SELECT * FROM final
