# Clientes inativos (não compram há 90 dias)
WITH last_purchase AS (
    SELECT
        customer_id,
        MAX(order_date) AS last_order
    FROM orders
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    last_order,
    CURRENT_DATE - last_order AS days_inactive
FROM customers c
JOIN last_purchase lp 
    ON c.customer_id = lp.customer_id
WHERE CURRENT_DATE - last_order > 90
ORDER BY days_inactive DESC;
