# Top 3 produtos mais vendidos por categoria
WITH product_sales AS (
    SELECT
        p.category,
        oi.product_id,
        SUM(oi.quantity) AS total_sold
    FROM order_items oi
    JOIN products p 
        ON oi.product_id = p.product_id
    GROUP BY p.category, oi.product_id
),
ranked_products AS (
    SELECT *,
           RANK() OVER(
               PARTITION BY category
               ORDER BY total_sold DESC
           ) AS rank_in_category
    FROM product_sales
)

SELECT *
FROM ranked_products
WHERE rank_in_category <= 3;
