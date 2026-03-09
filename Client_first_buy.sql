# Primeira compra de cada cliente
WITH ranked_orders AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY customer_id
            ORDER BY order_date
        ) AS rn
    FROM orders
)

SELECT *
FROM ranked_orders
WHERE rn = 1;
