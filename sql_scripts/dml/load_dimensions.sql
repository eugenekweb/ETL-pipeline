-- Загрузка измерений 
-- Загрузка терминалов (SCD2)
-- 1. Закрываем старые версии, если данные изменились
UPDATE bank.dwh_dim_terminals_hist tgt
SET effective_to = CAST(:date_str as DATE) - INTERVAL '1 day'
FROM bank.stg_terminals_temp src
WHERE tgt.terminal_id = src.terminal_id
  AND tgt.effective_to = '9999-12-31'
  AND tgt.deleted_flg <> 1
  AND (
      tgt.terminal_type <> src.terminal_type OR
      tgt.terminal_city <> src.terminal_city OR
      tgt.terminal_address <> src.terminal_address
  );

-- 2. Обновляем список терминалов
INSERT INTO bank.dwh_dim_terminals_hist (
    terminal_id, terminal_type, terminal_city, terminal_address, 
    effective_from
)
SELECT
    src.terminal_id, src.terminal_type, src.terminal_city, src.terminal_address,
    :date_str
FROM bank.stg_terminals_temp src
JOIN bank.dwh_dim_terminals_hist tgt
  ON src.terminal_id = tgt.terminal_id
WHERE (
      tgt.terminal_type <> src.terminal_type OR
      tgt.terminal_city <> src.terminal_city OR
      tgt.terminal_address <> src.terminal_address
  ) AND tgt.deleted_flg <> 1
ON CONFLICT (terminal_id, effective_from) DO NOTHING;

-- 3. Вставляем новые терминалы
INSERT INTO bank.dwh_dim_terminals_hist (
    terminal_id, terminal_type, terminal_city, terminal_address, effective_from
)
SELECT
    src.terminal_id, src.terminal_type, src.terminal_city, src.terminal_address,
    :date_str
FROM bank.stg_terminals_temp src
LEFT JOIN bank.dwh_dim_terminals_hist tgt
  ON src.terminal_id = tgt.terminal_id
WHERE tgt.terminal_id IS NULL
ON CONFLICT (terminal_id, effective_from) DO NOTHING;
