import pandas as pd
import logging
import os
from sqlalchemy import text

from .file_utils import (
    get_files_by_date, load_file_to_df, archive_file, normalize_date
)
from .db_utils import get_engine, get_connection
from .db_manager import DBManager
from .load_config import load_config


class ETLPipeline:
    """Основной класс ETL-процесса"""

    def __init__(self, config_path="config.json"):
        self.config = load_config(config_path)
        self.engine = get_engine(self.config)
        self.db_manager = DBManager(self.config)

    def process_date(self, date_str):
        """
        Основной метод обработки данных за указанную дату
        """
        logging.info(f"Начинаю обработку данных за дату: {date_str}")

        # Получение файлов для указанной даты
        files = get_files_by_date(self.config['paths']['files_dir'], date_str)

        if not any(files.values()):
            message = f"Не найдены файлы для даты {date_str}"
            logging.warning(message)
            raise FileNotFoundError(message)

        # Обработка каждого типа файлов
        try:
            # 0. Инициализация БД и очистка staging
            with get_connection(self.config) as conn:
                self.db_manager.ensure_database_ready(conn)
            
            # 1. Загрузка транзакций
            if files['transactions']:
                count = self._process_transactions(files['transactions'])
                self._log_meta_load(date_str, 'transactions', files['transactions'], count, 'SUCCESS')

            # 2. Загрузка черного списка паспортов
            if files['blacklist']:
                count = self._process_blacklist(files['blacklist'])
                self._log_meta_load(date_str, 'passport_blacklist', files['blacklist'], count, 'SUCCESS')

            # 3. Загрузка терминалов
            if files['terminals']:
                count = self._process_terminals(files['terminals'])
                self._log_meta_load(date_str, 'terminals', files['terminals'], count, 'SUCCESS')

            # 4. Загрузка измерений
            self._load_dimensions(date_str)
            self._upsert_last_update('dwh_dim_terminals_hist', 'dimensions')

            # 5. Загрузка фактов
            self._load_facts(date_str)
            self._upsert_last_update('dwh_fact_transactions', 'facts')
            self._upsert_last_update('dwh_fact_passport_blacklist', 'facts')

            # 6. Построение витрины мошенничества
            self._build_fraud_report()
            self._upsert_last_update('rep_fraud', 'report')

            # 7. Архивирование файлов
            self._archive_files(files)

            logging.info(
                f"Обработка данных за дату {date_str} завершена успешно"
            )

        except Exception as e:
            logging.error(
                f"Ошибка при обработке данных за дату {date_str}: {str(e)}"
            )
            # Пишем неуспешные загрузки по известным файлам
            try:
                if files.get('transactions'):
                    self._log_meta_load(date_str, 'transactions', files['transactions'], 0, 'ERROR', str(e))
                if files.get('blacklist'):
                    self._log_meta_load(date_str, 'passport_blacklist', files['blacklist'], 0, 'ERROR', str(e))
                if files.get('terminals'):
                    self._log_meta_load(date_str, 'terminals', files['terminals'], 0, 'ERROR', str(e))
            except Exception:
                pass
            raise

    def _create_temp_table(self, data_frame, file_path):
        """Создание временной таблицы"""
        table_name = f'stg_{file_path[len(self.config["paths"]["files_dir"]) + 1:file_path.rfind(".") - 9]}_temp'
        data_frame.to_sql(
            table_name, self.engine, if_exists='replace', 
            index=False, schema='bank'
        )
        logging.info(f"Временная таблица '{table_name}' создана/перезаписана")
        logging.info(f"Загружено {len(data_frame)} записей")
        return len(data_frame)

    def _process_transactions(self, file_path):
        """Обработка файла транзакций"""
        logging.info(f"Обработка транзакций из файла: {file_path}")

        df = load_file_to_df(file_path, "txt")
        return self._create_temp_table(df, file_path)

    def _process_blacklist(self, file_path):
        """Обработка файла черного списка паспортов"""
        logging.info(f"Обработка черного списка из файла: {file_path}")

        df = load_file_to_df(file_path, "xlsx")
        return self._create_temp_table(df, file_path)

    def _process_terminals(self, file_path):
        """Обработка файла терминалов"""
        logging.info(f"Обработка терминалов из файла: {file_path}")

        df = load_file_to_df(file_path, "xlsx")
        return self._create_temp_table(df, file_path)

    def _log_meta_load(self, date_str, file_type, file_name, records_loaded, status, error_message=None):
        """Запись в bank.meta_load_info"""
        load_date = f"{date_str[4:]}-{date_str[2:4]}-{date_str[:2]}"
        with self.engine.connect() as conn:
            conn.execute(text(
                """
                INSERT INTO bank.meta_load_info(
                    load_date, file_type, file_name, records_loaded, load_status, error_message
                ) VALUES (:load_date, :file_type, :file_name, :records_loaded, :load_status, :error_message)
                """
            ), {
                'load_date': load_date,
                'file_type': file_type,
                'file_name': os.path.basename(file_name) if file_name else None,
                'records_loaded': int(records_loaded) if records_loaded is not None else 0,
                'load_status': status,
                'error_message': error_message
            })
            conn.commit()

    def _upsert_last_update(self, table_name, update_type):
        """Апсерт bank.meta_last_update по имени таблицы"""
        with self.engine.connect() as conn:
            conn.execute(text(
                """
                INSERT INTO bank.meta_last_update(table_name, last_update_date, last_update_type)
                VALUES (:t, CURRENT_TIMESTAMP, :tp)
                ON CONFLICT (table_name) DO UPDATE SET
                    last_update_date = EXCLUDED.last_update_date,
                    last_update_type = EXCLUDED.last_update_type
                """
            ), {'t': table_name, 'tp': update_type})
            conn.commit()

    def _load_dimensions(self, date_str):
        """Загрузка измерений с использованием SCD2"""
        logging.info("Начинаю загрузку измерений")

        # Загрузка терминалов (SCD2)
        sql_script = os.path.join(
            self.config['paths']['dml_sql'], 'load_dimensions.sql'
        )
        if os.path.exists(sql_script):
            with self.engine.connect() as conn:
                with open(sql_script, 'r', encoding='utf-8') as f:
                    sql = f.read()
                    conn.execute(
                        text(sql),
                        {"date_str":
                            f"{date_str[-4:]}-{date_str[2:4]}-{date_str[:2]}"})
                    conn.commit()

        logging.info("Загрузка измерений завершена")

    def _load_facts(self, date_str):
        """Загрузка фактовых таблиц"""
        logging.info("Начинаю загрузку фактов")

        sql_script = os.path.join(
            self.config['paths']['dml_sql'], 'load_facts.sql'
        )
        if os.path.exists(sql_script):
            with self.engine.connect() as conn:
                with open(sql_script, 'r', encoding='utf-8') as f:
                    sql = f.read()
                    conn.execute(
                        text(sql),
                        {"date_str":
                            f"{date_str[-4:]}-{date_str[2:4]}-{date_str[:2]}"})
                    conn.commit()

        logging.info("Загрузка фактов завершена")

    def _build_fraud_report(self):
        """Построение витрины мошенничества"""
        logging.info("Начинаю построение витрины мошенничества")

        sql_script = os.path.join(
            self.config['paths']['dml_sql'], 'build_fraud_report.sql'
        )
        if os.path.exists(sql_script):
            with self.engine.connect() as conn:
                with open(sql_script, 'r', encoding='utf-8') as f:
                    sql = f.read()
                    conn.execute(text(sql))
                    conn.commit()

        logging.info("Витрина мошенничества построена")

    def _archive_files(self, files):
        """Архивирование обработанных файлов"""
        logging.info("Начинаю архивирование файлов")

        archive_dir = self.config['paths']['archive_dir']

        for file_type, file_path in files.items():
            if file_path and os.path.exists(file_path):
                try:
                    archived_path = archive_file(file_path, archive_dir)
                    logging.info(
                        f"Файл {file_path} перемещен в архив: {archived_path}"
                    )
                except Exception as e:
                    logging.error(
                        f"Ошибка при архивировании файла {file_path}: "
                        f"{str(e)}"
                    )

    def get_fraud_report(self, date_str=None):
        """Получение отчета по мошенничеству"""
        query = "SELECT * FROM bank.rep_fraud"
        if date_str:
            date_condition = (
                f" WHERE DATE(event_dt) = "
                f"'{date_str[:2]}-{date_str[2:4]}-{date_str[4:]}'"
            )
            query += date_condition
        query += " ORDER BY event_dt DESC"

        df = pd.read_sql(query, self.engine)
        return df
