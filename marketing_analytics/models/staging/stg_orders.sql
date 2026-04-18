WITH source AS (

    SELECT * FROM {{ source('raw', 'orders') }}

),

renamed AS (

    SELECT
        order_id,
        customer_id,
        session_id,
        status,
        amount,
        ordered_at,
        CAST(ordered_at AS DATE) AS ordered_date
    FROM source

)

SELECT * FROM renamed
