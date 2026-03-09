# Identificar clientes em risco de churn
WITH purchase_gaps AS (
    SELECT
        customer_id,
        order_date,
        LAG(order_date) OVER (
            PARTITION BY customer_id
            ORDER BY order_date
        ) AS previous_order
    FROM orders
),
gap_analysis AS (
    SELECT
        customer_id,
        AVG(order_date - previous_order) AS avg_gap
    FROM purchase_gaps
    WHERE previous_order IS NOT NULL
    GROUP BY customer_id
),
last_purchase AS (
    SELECT
        customer_id,
        MAX(order_date) AS last_order
    FROM orders
    GROUP BY customer_id
)

SELECT
    g.customer_id,
    avg_gap,
    CURRENT_DATE - last_order AS days_since_last_order
FROM gap_analysis g
JOIN last_purchase l
    ON g.customer_id = l.customer_id
WHERE CURRENT_DATE - last_order > avg_gap * 2;
