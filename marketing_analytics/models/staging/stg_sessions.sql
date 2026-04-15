WITH source AS (

    SELECT * FROM {{ source('raw', 'sessions') }}

),

renamed AS (

    SELECT
        session_id,
        customer_id,
        channel,
        started_at,
        duration_seconds,
        pages_viewed,
        device,
        country
    FROM source

)

SELECT * FROM renamed
