# Classifica clientes em tiers de valor
WITH customer_revenue AS (
    SELECT
        customer_id,
        SUM(amount) AS total_revenue
    FROM orders
    GROUP BY customer_id
),
revenue_percentiles AS (
    SELECT
        customer_id,
        total_revenue,
        NTILE(3) OVER (ORDER BY total_revenue DESC) AS revenue_tier
    FROM customer_revenue
)

SELECT
    customer_id,
    total_revenue,
    CASE
        WHEN revenue_tier = 1 THEN 'High Value'
        WHEN revenue_tier = 2 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment
FROM revenue_percentiles;
