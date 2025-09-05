-- Создание схемы
CREATE SCHEMA IF NOT EXISTS bank;
SET search_path TO bank;

-- Создание таблиц измерений (SCD2)
CREATE TABLE IF NOT EXISTS bank.dwh_dim_terminals_hist (
    terminal_id VARCHAR(128),
    terminal_type VARCHAR(50),
    terminal_city VARCHAR(100),
    terminal_address VARCHAR(255),
    effective_from DATE DEFAULT '1970-01-01',
    effective_to DATE DEFAULT '9999-12-31',
    deleted_flg INTEGER DEFAULT 0,
    PRIMARY KEY (terminal_id, effective_from)
);

-- Создание фактовых таблиц
CREATE TABLE IF NOT EXISTS bank.dwh_fact_transactions (
    trans_id VARCHAR(128) PRIMARY KEY,
    trans_date TIMESTAMP,
    amt DECIMAL(15,2),
    card_num VARCHAR(128),
    oper_type VARCHAR(50),
    oper_result VARCHAR(50),
    terminal VARCHAR(128),
    create_dt DATE DEFAULT '1970-01-01',
    update_dt DATE DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS bank.dwh_fact_passport_blacklist (
    passport VARCHAR(128),
    entry_dt DATE,
    create_dt DATE DEFAULT '1970-01-01',
    update_dt DATE DEFAULT CURRENT_DATE,
    PRIMARY KEY (passport, entry_dt)
);

-- Создание витрины мошенничества
CREATE TABLE IF NOT EXISTS bank.rep_fraud (
    event_dt TIMESTAMP,
    passport VARCHAR(128),
    fio VARCHAR(255),
    phone VARCHAR(128),
    event_type VARCHAR(255),
    report_dt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_dt, passport, event_type)
);

-- Создание таблиц метаданных
CREATE TABLE IF NOT EXISTS bank.meta_load_info (
    load_id SERIAL PRIMARY KEY,
    load_date DATE,
    file_type VARCHAR(50),
    file_name VARCHAR(255),
    records_loaded INTEGER,
    load_status VARCHAR(50),
    error_message TEXT,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bank.meta_last_update (
    table_name VARCHAR(128) PRIMARY KEY,
    last_update_date TIMESTAMP,
    last_update_type VARCHAR(50)
);

-- Создание индексов для оптимизации
CREATE INDEX IF NOT EXISTS idx_transactions_card_num ON bank.dwh_fact_transactions(card_num);
CREATE INDEX IF NOT EXISTS idx_transactions_date ON bank.dwh_fact_transactions(trans_date);
CREATE INDEX IF NOT EXISTS idx_blacklist_passport ON bank.dwh_fact_passport_blacklist(passport);
CREATE INDEX IF NOT EXISTS idx_fraud_event_dt ON bank.rep_fraud(event_dt);
CREATE INDEX IF NOT EXISTS idx_fraud_passport ON bank.rep_fraud(passport);
