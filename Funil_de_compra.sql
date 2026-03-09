# Funil de compra (visualização de conversão)
WITH visits AS (
    SELECT COUNT(DISTINCT user_id) AS total_visits
    FROM website_visits
),
add_to_cart AS (
    SELECT COUNT(DISTINCT user_id) AS total_cart
    FROM cart_events
),
purchases AS (
    SELECT COUNT(DISTINCT customer_id) AS total_purchases
    FROM orders
)

SELECT
    v.total_visits,
    c.total_cart,
    p.total_purchases,
    c.total_cart * 1.0 / v.total_visits AS visit_to_cart_rate,
    p.total_purchases * 1.0 / c.total_cart AS cart_to_purchase_rate
FROM visits v
CROSS JOIN add_to_cart c
CROSS JOIN purchases p;
