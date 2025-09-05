-- Построение витрины мошенничества

-- Очистка витрины
-- DELETE FROM bank.rep_fraud;

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
JOIN bank.accounts acc ON card.account_id = acc.account_id
JOIN bank.clients c ON acc.client_id = c.client_id
LEFT JOIN bank.dwh_fact_passport_blacklist bl ON c.passport_num = bl.passport
WHERE (
    -- Просроченный паспорт: транзакция ПОСЛЕ истечения срока действия
    (c.passport_valid_to IS NOT NULL AND c.passport_valid_to < t.transaction_date)
    OR 
    -- Заблокированный паспорт: паспорт в черном списке
    (bl.passport IS NOT NULL AND bl.entry_dt <= DATE(t.transaction_date))
    OR
    -- Подозрительный случай: NULL для людей младше 45 лет
    (c.passport_valid_to IS NULL AND EXTRACT(YEAR FROM AGE(c.date_of_birth)) < 45)
)
  AND t.transaction_date BETWEEN c.effective_from AND c.effective_to
  AND c.deleted_flg = 0
  AND t.transaction_date BETWEEN acc.effective_from AND acc.effective_to
  AND acc.deleted_flg = 0
  AND t.transaction_date BETWEEN card.effective_from AND card.effective_to
  AND card.deleted_flg = 0;

-- 2. Операции при недействующем договоре
-- Проверяем транзакции, которые произошли ПОСЛЕ истечения срока действия договора
-- INSERT INTO bank.rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
-- SELECT DISTINCT
--     t.transaction_date,
--     c.passport_num,
--     CONCAT(c.last_name, ' ', c.first_name, ' ', c.patronymic) as fio,
--     c.phone,
--     'Недействующий договор' as event_type,
--     CURRENT_TIMESTAMP as report_dt
-- FROM bank.stg_transactions t
-- JOIN bank.dwh_dim_cards_hist card ON t.card_num = card.card_num
-- JOIN bank.dwh_dim_accounts_hist acc ON card.account_id = acc.account_id
-- JOIN bank.dwh_dim_clients_hist c ON acc.client_id = c.client_id
-- WHERE acc.account_valid_to < t.transaction_date  -- Транзакция ПОСЛЕ истечения договора
--   AND t.transaction_date BETWEEN c.effective_from AND c.effective_to
--   AND c.deleted_flg = 0
--   AND t.transaction_date BETWEEN acc.effective_from AND acc.effective_to
--   AND acc.deleted_flg = 0
--   AND t.transaction_date BETWEEN card.effective_from AND card.effective_to
--   AND card.deleted_flg = 0;

-- -- 3. Операции в разных городах в течение часа
-- INSERT INTO bank.rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
-- SELECT DISTINCT
--     t1.transaction_date,
--     c.passport_num,
--     CONCAT(c.last_name, ' ', c.first_name, ' ', c.patronymic) as fio,
--     c.phone,
--     'Операции в разных городах в течение часа' as event_type,
--     CURRENT_TIMESTAMP as report_dt
-- FROM bank.stg_transactions t1
-- JOIN bank.stg_transactions t2 ON t1.card_num = t2.card_num
-- JOIN bank.dwh_dim_cards_hist card ON t1.card_num = card.card_num
-- JOIN bank.dwh_dim_accounts_hist acc ON card.account_id = acc.account_id
-- JOIN bank.dwh_dim_clients_hist c ON acc.client_id = c.client_id
-- JOIN bank.dwh_dim_terminals_hist term1 ON t1.terminal = term1.terminal_id
-- JOIN bank.dwh_dim_terminals_hist term2 ON t2.terminal = term2.terminal_id
-- WHERE t1.transaction_date < t2.transaction_date
--   AND t2.transaction_date <= t1.transaction_date + INTERVAL '1 hour'
--   AND term1.terminal_city <> term2.terminal_city
--   AND t1.transaction_date BETWEEN c.effective_from AND c.effective_to
--   AND c.deleted_flg = 0
--   AND t1.transaction_date BETWEEN acc.effective_from AND acc.effective_to
--   AND acc.deleted_flg = 0
--   AND t1.transaction_date BETWEEN card.effective_from AND card.effective_to
--   AND card.deleted_flg = 0
--   AND t1.transaction_date BETWEEN term1.effective_from AND term1.effective_to
--   AND term1.deleted_flg = 0
--   AND t2.transaction_date BETWEEN term2.effective_from AND term2.effective_to
--   AND term2.deleted_flg = 0;

-- -- 4. Попытка подбора суммы
-- INSERT INTO bank.rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
-- WITH rejected_transactions AS (
--     SELECT 
--         t.card_num,
--         t.transaction_date,
--         t.amount,
--         ROW_NUMBER() OVER (PARTITION BY t.card_num ORDER BY t.transaction_date) as rn
--     FROM bank.stg_transactions t
--     WHERE t.oper_result = 'REJECTED'
-- ),
-- fraud_cards AS (
--     SELECT 
--         rt1.card_num,
--         rt1.transaction_date,
--         rt1.amount
--     FROM rejected_transactions rt1
--     JOIN rejected_transactions rt2 ON rt1.card_num = rt2.card_num
--     JOIN rejected_transactions rt3 ON rt1.card_num = rt3.card_num
--     WHERE rt1.rn < rt2.rn 
--       AND rt2.rn < rt3.rn
--       AND rt2.transaction_date <= rt1.transaction_date + INTERVAL '20 minutes'
--       AND rt3.transaction_date <= rt1.transaction_date + INTERVAL '20 minutes'
--       AND rt1.amount > rt2.amount 
--       AND rt2.amount > rt3.amount
-- )
-- SELECT DISTINCT
--     t.transaction_date,
--     c.passport_num,
--     CONCAT(c.last_name, ' ', c.first_name, ' ', c.patronymic) as fio,
--     c.phone,
--     'Попытка подбора суммы' as event_type,
--     CURRENT_TIMESTAMP as report_dt
-- FROM bank.stg_transactions t
-- JOIN fraud_cards fc ON t.card_num = fc.card_num
-- JOIN bank.dwh_dim_cards_hist card ON t.card_num = card.card_num
-- JOIN bank.dwh_dim_accounts_hist acc ON card.account_id = acc.account_id
-- JOIN bank.dwh_dim_clients_hist c ON acc.client_id = c.client_id
-- WHERE t.oper_result = 'SUCCESS'
--   AND t.transaction_date = fc.transaction_date
--   AND t.transaction_date BETWEEN c.effective_from AND c.effective_to
--   AND c.deleted_flg = 0
--   AND t.transaction_date BETWEEN acc.effective_from AND acc.effective_to
--   AND acc.deleted_flg = 0
--   AND t.transaction_date BETWEEN card.effective_from AND card.effective_to
--   AND card.deleted_flg = 0;
