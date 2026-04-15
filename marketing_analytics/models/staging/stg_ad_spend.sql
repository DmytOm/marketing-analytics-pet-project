WITH source AS (

    SELECT * FROM {{ source('raw', 'ad_spend') }}

),

renamed AS (

    SELECT
        date,
        channel,
        spend,
        impressions,
        clicks
    FROM source

)

SELECT * FROM renamed
