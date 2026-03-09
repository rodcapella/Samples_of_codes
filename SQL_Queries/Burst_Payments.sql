# Detectar “burst payments” (fraude financeira)
WITH payment_windows AS (
    SELECT
        user_id,
        payment_time,
        COUNT(*) OVER (
            PARTITION BY user_id
            ORDER BY payment_time
            RANGE BETWEEN INTERVAL '1 minute' PRECEDING AND CURRENT ROW
        ) AS payments_last_minute
    FROM payments
)

SELECT *
FROM payment_windows
WHERE payments_last_minute >= 5;
