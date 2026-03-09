# Tempo entre compras do usuário
WITH ordered_transactions AS (
    SELECT
        user_id,
        transaction_date,
        LAG(transaction_date) OVER(
            PARTITION BY user_id
            ORDER BY transaction_date
        ) AS previous_purchase
    FROM transactions
)

SELECT
    user_id,
    transaction_date,
    previous_purchase,
    transaction_date - previous_purchase AS days_between
FROM ordered_transactions;
