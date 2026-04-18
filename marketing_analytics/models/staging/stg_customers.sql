WITH source AS (

    SELECT * FROM {{ source('raw', 'customers') }}

),

renamed AS (

    SELECT
        customer_id,
        first_name,
        last_name,
        LOWER(email)                    AS email,
        country,
        city,
        age,
        LOWER(gender)                   AS gender,
        CAST(created_at AS DATE)        AS created_date,
        created_at
    FROM source

)

SELECT * FROM renamed