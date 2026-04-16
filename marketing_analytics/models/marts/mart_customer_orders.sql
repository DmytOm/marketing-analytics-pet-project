WITH customers AS (

    SELECT * FROM {{ ref('stg_customers') }}

),

orders AS (

    SELECT * FROM {{ ref('stg_orders') }}

),

customer_orders AS (

    SELECT
        orders.customer_id,
        COUNT(orders.order_id)                                            AS total_orders,
        SUM(orders.amount)                                                AS lifetime_value,
        ROUND(AVG(orders.amount), 2)                                      AS avg_order_value,
        MIN(orders.ordered_date)                                          AS first_order_date,
        MAX(orders.ordered_date)                                          AS last_order_date,
        COUNT(CASE WHEN orders.status = 'completed' THEN 1 END)          AS completed_orders,
        COUNT(CASE WHEN orders.status = 'refunded' THEN 1 END)           AS refunded_orders
    FROM orders
    GROUP BY 1

),

final AS (

    SELECT
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customers.email,
        customers.country,
        customers.gender,
        customers.age,
        customers.created_date,
        customer_orders.total_orders,
        customer_orders.lifetime_value,
        customer_orders.avg_order_value,
        customer_orders.first_order_date,
        customer_orders.last_order_date,
        customer_orders.completed_orders,
        customer_orders.refunded_orders
    FROM customers
    LEFT JOIN customer_orders
        ON customers.customer_id = customer_orders.customer_id

)

SELECT * FROM final
