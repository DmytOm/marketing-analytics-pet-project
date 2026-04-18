WITH source AS (

    SELECT * FROM {{ source('raw', 'email_events') }}

),

renamed AS (

    SELECT
        event_id,
        customer_id,
        campaign,
        sent_at,
        CAST(sent_at as DATE) AS sent_date,
        is_opened,
        is_clicked
    FROM source

)

SELECT * FROM renamed
