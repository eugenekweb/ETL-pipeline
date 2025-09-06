-- Построение витрины мошенничества

-- 1. Операции при просроченном/заблокированном паспорте
-- Проверяем транзакции, которые произошли ПОСЛЕ истечения срока действия паспорта
INSERT INTO bank.rep_fraud (event_dt, passport, fio, phone, event_type)
SELECT DISTINCT
    t.transaction_date::TIMESTAMP,
    c.passport_num,
    CONCAT(c.last_name, ' ', c.first_name, ' ', c.patronymic) as fio,
    c.phone,
    'Просроченный/заблокированный паспорт' as event_type
FROM bank.stg_transactions_temp t
JOIN bank.cards card ON t.card_num = card.card_num
JOIN bank.accounts acc ON card.account= acc.account
JOIN bank.clients c ON acc.client = c.client_id 
LEFT JOIN bank.dwh_fact_passport_blacklist bl ON c.passport_num = bl.passport
WHERE (
    -- Просроченный паспорт: транзакция ПОСЛЕ истечения срока действия
    (c.passport_valid_to IS NOT NULL AND c.passport_valid_to < t.transaction_date::DATE)
    OR 
    -- Заблокированный паспорт: паспорт в черном списке
    (bl.passport IS NOT NULL AND bl.entry_dt <= DATE(t.transaction_date))
    OR
    -- Подозрительный случай: NULL для людей младше 45 лет на дату транзакции
    (c.passport_valid_to IS NULL AND EXTRACT(YEAR FROM AGE(t.transaction_date::DATE, c.date_of_birth)) < 45)
  )
ON CONFLICT (event_dt, passport, event_type) DO NOTHING;

-- 2. Операции при недействующем договоре
-- Проверяем транзакции, которые произошли ПОСЛЕ истечения срока действия договора
INSERT INTO bank.rep_fraud (event_dt, passport, fio, phone, event_type)
SELECT DISTINCT
    t.transaction_date::TIMESTAMP,
    c.passport_num,
    CONCAT(c.last_name, ' ', c.first_name, ' ', c.patronymic) as fio,
    c.phone,
    'Недействующий договор' as event_type
FROM bank.stg_transactions_temp t
JOIN bank.cards card ON t.card_num = card.card_num
JOIN bank.accounts acc ON card.account = acc.account
JOIN bank.clients c ON acc.client = c.client_id
WHERE acc.valid_to::DATE < t.transaction_date::DATE
ON CONFLICT (event_dt, passport, event_type) DO NOTHING;
  
-- 3. Операции в разных городах в течение часа
INSERT INTO bank.rep_fraud (event_dt, passport, fio, phone, event_type)
SELECT DISTINCT
    t1.transaction_date::TIMESTAMP,
    c.passport_num,
    CONCAT(c.last_name, ' ', c.first_name, ' ', c.patronymic) as fio,
    c.phone,
    'Операции в разных городах в течение часа' as event_type
FROM bank.stg_transactions_temp t1
JOIN bank.stg_transactions_temp t2 ON t1.card_num = t2.card_num
JOIN bank.cards card ON t1.card_num = card.card_num
JOIN bank.accounts acc ON card.account = acc.account
JOIN bank.clients c ON acc.client = c.client_id
JOIN bank.dwh_dim_terminals_hist term1 ON t1.terminal = term1.terminal_id
JOIN bank.dwh_dim_terminals_hist term2 ON t2.terminal = term2.terminal_id
WHERE t1.transaction_date::TIMESTAMP < t2.transaction_date::TIMESTAMP
  AND t2.transaction_date::TIMESTAMP <= (t1.transaction_date::TIMESTAMP + INTERVAL '1 hour')
  AND term1.terminal_city <> term2.terminal_city
  AND t1.transaction_date::TIMESTAMP BETWEEN term1.effective_from AND term1.effective_to
  AND term1.deleted_flg = 0
  AND t2.transaction_date::TIMESTAMP BETWEEN term2.effective_from AND term2.effective_to
  AND term2.deleted_flg = 0
ON CONFLICT (event_dt, passport, event_type) DO NOTHING;

-- 4. Попытка подбора суммы
INSERT INTO bank.rep_fraud (event_dt, passport, fio, phone, event_type)
-- приводим к нужному виду
WITH transactions AS (
    SELECT 
        trans_id AS transaction_id,
        trans_date AS transaction_date,
        amt AS amount,
        card_num,
        oper_type,
        oper_result,
        terminal,
        ROW_NUMBER() OVER (PARTITION BY card_num ORDER BY trans_date) as rn
    FROM bank.dwh_fact_transactions
), -- находим карты и транзакции, которые могут быть мошенническими
fraud_cards AS (
    SELECT 
        t.card_num,
        t.transaction_date,
        t.amount,
        t.oper_result,
        CASE
            WHEN 
                -- проверяем, что предыдущие три транзакции были отклонены
                LAG(t.oper_result) OVER cards_by_trans_date = 'REJECT'
                AND LAG(t.oper_result, 2) OVER cards_by_trans_date = 'REJECT'
                AND LAG(t.oper_result, 3) OVER cards_by_trans_date = 'REJECT'
                -- проверяем, что предыдущие три транзакции не были депозитами
                AND LAG(t.oper_type) OVER cards_by_trans_date <> 'DEPOSIT'
                AND LAG(t.oper_type, 2) OVER cards_by_trans_date <> 'DEPOSIT'
                AND LAG(t.oper_type, 3) OVER cards_by_trans_date <> 'DEPOSIT'
                -- проверяем, что текущая транзакция была успешной и не была депозитом
                AND t.oper_result = 'SUCCESS' AND t.oper_type <> 'DEPOSIT'
                -- проверяем, что сумма текущей транзакции больше, чем сумма предыдущих трех транзакций
                AND LAG(t.amount) OVER cards_by_trans_date > amount
                AND LAG(t.amount, 2) OVER cards_by_trans_date > LAG(t.amount) OVER cards_by_trans_date
                AND LAG(t.amount, 3) OVER cards_by_trans_date > LAG(t.amount, 2) OVER cards_by_trans_date
                -- проверяем, что интервал между транзакциями меньше 20 минут
                AND (t.transaction_date - LAG(t.transaction_date, 2) OVER cards_by_trans_date) < INTERVAL '20 minutes'
                AND (LAG(t.transaction_date, 2) OVER cards_by_trans_date - LAG(t.transaction_date, 3) OVER cards_by_trans_date) < INTERVAL '20 minutes'
            THEN TRUE
            ELSE FALSE
        END AS rrs
    FROM transactions t
    WINDOW cards_by_trans_date AS (PARTITION BY t.card_num ORDER BY t.transaction_date)
)
SELECT DISTINCT
    fc.transaction_date,
    c.passport_num,
    CONCAT(c.last_name, ' ', c.first_name, ' ', c.patronymic) as fio,
    c.phone,
    'Попытка подбора суммы' as event_type
FROM fraud_cards fc
JOIN bank.cards card ON fc.card_num = card.card_num
JOIN bank.accounts acc ON card.account = acc.account
JOIN bank.clients c ON acc.client = c.client_id
WHERE fc.rrs = TRUE
ON CONFLICT (event_dt, passport, event_type) DO NOTHING;
