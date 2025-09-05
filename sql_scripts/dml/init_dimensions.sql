-- Инициализация измерений из существующих данных
-- Этот скрипт загружает начальные данные в таблицы измерений

-- Загрузка клиентов
INSERT INTO bank.dwh_dim_clients_hist (
    client_id, last_name, first_name, patronymic, date_of_birth, 
    passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg
)
SELECT 
    client_id, last_name, first_name, patronymic, date_of_birth,
    passport_num, passport_valid_to, phone,
    COALESCE(create_dt, DATE '1900-01-01'), '9999-12-31', 0
FROM bank.clients
ON CONFLICT (client_id, effective_from) DO NOTHING;

-- Загрузка аккаунтов
INSERT INTO bank.dwh_dim_accounts_hist (
    account_id, client_id, account_valid_to, effective_from, effective_to, deleted_flg
)
SELECT 
    account as account_id, client as client_id, valid_to as account_valid_to,
    COALESCE(create_dt, DATE '1900-01-01'), '9999-12-31', 0
FROM bank.accounts
ON CONFLICT (account_id, effective_from) DO NOTHING;

-- Загрузка карт
INSERT INTO bank.dwh_dim_cards_hist (
    card_num, account_id, card_valid_to, effective_from, effective_to, deleted_flg
)
SELECT 
    card_num, account as account_id, NULL as card_valid_to,
    COALESCE(create_dt, DATE '1900-01-01'), '9999-12-31', 0
FROM bank.cards
ON CONFLICT (card_num, effective_from) DO NOTHING;
