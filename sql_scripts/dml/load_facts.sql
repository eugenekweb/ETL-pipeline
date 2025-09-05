-- Загрузка фактовых таблиц

-- Загрузка транзакций
INSERT INTO bank.dwh_fact_transactions (
    trans_id, trans_date, amt, card_num, 
    oper_type, oper_result, terminal, create_dt
)
SELECT 
    transaction_id, 
    transaction_date::TIMESTAMP, 
    replace(amount, ',', '.')::DECIMAL(15,2),
    card_num, 
    oper_type, 
    oper_result, 
    terminal, 
    :date_str
FROM bank.stg_transactions_temp
ON CONFLICT (trans_id) DO NOTHING;

-- Загрузка черного списка паспортов
INSERT INTO bank.dwh_fact_passport_blacklist (
    passport, entry_dt, create_dt
)
SELECT 
    passport, 
    date::DATE, 
    :date_str
FROM bank.stg_passport_blacklist_temp
ON CONFLICT (passport, entry_dt) DO NOTHING;
