# Problema de ranking com empate
WITH ranked_sales AS (
    SELECT
        region,
        salesperson,
        revenue,
        DENSE_RANK() OVER (
            PARTITION BY region
            ORDER BY revenue DESC
        ) AS rank
    FROM sales
)

SELECT *
FROM ranked_sales
WHERE rank <= 3;
