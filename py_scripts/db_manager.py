import logging
from .db_utils import execute_sql_script


class DBManager:
    """Управляет жизненным циклом базы данных"""
    
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def ensure_database_ready(self, connection):
        """Проверяет готовность БД и создает недостающие элементы"""
        try:
            # Создание схемы
            self._create_schema(connection)
            
            # Всегда создаем таблицы
            self._create_tables(connection)
            
        except Exception as e:
            self.logger.error(f"Ошибка при инициализации БД: {str(e)}")
            raise
        
    def _create_schema(self, connection):
        """Создание схемы bank"""
        try:
            cursor = connection.cursor()
            cursor.execute("CREATE SCHEMA IF NOT EXISTS bank")
            connection.commit()
        except Exception as e:
            self.logger.error(f"Ошибка при создании схемы: {str(e)}")
            raise
            
    def _create_tables(self, connection):
        """Создание всех таблиц из DDL-скрипта"""
        try:
            execute_sql_script(
                connection, 
                self.config['paths']['ddl_sql'], 
                'create_tables'
            )
        except Exception as e:
            self.logger.error(f"Ошибка при создании таблиц: {str(e)}")
            raise
