{% snapshot customers_snapshot %}

    {{
        config(
            target_schema='snapshots',
            strategy='check',
            unique_key='customer_id',
        check_cols=['country', 'city', 'email']
   )
}}

    select * from {{ source('raw', 'customers') }}

{% endsnapshot %}
