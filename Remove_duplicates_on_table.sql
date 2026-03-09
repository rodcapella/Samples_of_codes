# Tabelas pequenas (< 10 milhões de linhas)
BEGIN;

-- Verificar duplicados
WITH ranked_transactions AS (
    SELECT
        id,
        transaction_id,
        timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY timestamp ASC
        ) AS rn
    FROM transactions
)

SELECT *
FROM ranked_transactions
WHERE rn > 1;

-- Remover duplicados
WITH ranked_transactions AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY timestamp ASC
        ) AS rn
    FROM transactions
)

DELETE FROM transactions
WHERE id IN (
    SELECT id
    FROM ranked_transactions
    WHERE rn > 1
);

COMMIT;

# Tabelas médias (10M – 1B linhas)
BEGIN;

-- Criar nova tabela deduplicada
CREATE TABLE transactions_dedup AS
SELECT
    id,
    transaction_id,
    amount,
    timestamp,
    transaction_date
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY timestamp ASC
        ) AS rn
    FROM transactions
) t
WHERE rn = 1;

-- Criar índices novamente
CREATE INDEX idx_transaction_id
ON transactions_dedup(transaction_id);

CREATE INDEX idx_transaction_date
ON transactions_dedup(transaction_date);

-- Trocar tabelas
ALTER TABLE transactions RENAME TO transactions_backup;
ALTER TABLE transactions_dedup RENAME TO transactions;

COMMIT;

-- Remover backup se tudo estiver ok
DROP TABLE transactions_backup;

# Tabelas gigantes (> 1 bilhão de linhas)
BEGIN;

-- Criar tabela temporária com partição deduplicada
CREATE TEMP TABLE temp_dedup AS
SELECT
    id,
    transaction_id,
    amount,
    timestamp,
    transaction_date
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY timestamp ASC
        ) AS rn
    FROM transactions
    WHERE transaction_date = '2026-03-01'
) t
WHERE rn = 1;

-- Remover partição antiga
DELETE FROM transactions
WHERE transaction_date = '2026-03-01';

-- Inserir partição limpa
INSERT INTO transactions
SELECT *
FROM temp_dedup;

COMMIT;

# Lakehouses
MERGE INTO transactions t
USING (
    SELECT
        id,
        transaction_id,
        amount,
        timestamp,
        transaction_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY transaction_id
                ORDER BY timestamp ASC
            ) AS rn
        FROM transactions_staging
    ) s
    WHERE rn = 1
) src
ON t.transaction_id = src.transaction_id

WHEN NOT MATCHED THEN
INSERT (
    id,
    transaction_id,
    amount,
    timestamp,
    transaction_date
)
VALUES (
    src.id,
    src.transaction_id,
    src.amount,
    src.timestamp,
    src.transaction_date
);
