# Top 5 clientes por gasto por mês
WITH monthly_customer_spending AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        customer_id,
        SUM(amount) AS total_spent
    FROM orders
    GROUP BY 1,2
),
ranked_customers AS (
    SELECT
        month,
        customer_id,
        total_spent,
        RANK() OVER (
            PARTITION BY month
            ORDER BY total_spent DESC
        ) AS rank
    FROM monthly_customer_spending
)

SELECT *
FROM ranked_customers
WHERE rank <= 5
ORDER BY month, rank;
