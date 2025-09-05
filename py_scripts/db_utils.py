import psycopg2
import logging
from sqlalchemy import create_engine


def get_connection(config):
    """Создаёт соединение c БД"""
    db = config["db"]
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["dbname"],
        user=db["user"],
        password=db["password"]
    )


def get_engine(config):
    """Создаёт движок SQLAlchemy для загрузки из файлов"""
    db = config["db"]
    return create_engine(
        f"postgresql://{db['user']}:{db['password']}@"
        f"{db['host']}:{db['port']}/{db['dbname']}"
    )


def execute_sql_script(connection, sql_dir, script_name, params=None):
    """Выполняет SQL-скрипт из указанного по шаблону файла"""
    logger = logging.getLogger(__name__)
    path = f"{sql_dir}/{script_name}.sql"
    
    logger.info(f"Выполняю SQL-скрипт: {path}")
    
    with open(path, 'r', encoding='utf-8') as f:
        sql = f.read()
        if params:
            sql = sql.format(**params)
        
        # Разбиваем на команды
        commands = [cmd.strip() for cmd in sql.split(';') if cmd.strip()]
        logger.info(f"Найдено {len(commands)} команд для выполнения")
        
        cursor = connection.cursor()
        
        for i, command in enumerate(commands, 1):
            if command:
                # Убираем комментарии из начала команды
                clean_command = command
                lines = command.split('\n')
                clean_lines = []
                
                for line in lines:
                    line = line.strip()
                    if line and not line.startswith('--'):
                        clean_lines.append(line)
                
                if clean_lines:
                    clean_command = ' '.join(clean_lines)
                    
                    try:
                        logger.info(f"Выполняю команду {i}: "
                                   f"{clean_command[:50]}...")
                        cursor.execute(clean_command)
                        logger.info(f"Команда {i} выполнена успешно")
                    except Exception as e:
                        logger.error(f"ОШИБКА в команде {i}: {str(e)}")
                        logger.error(f"Команда: {clean_command}")
                        raise
        
        connection.commit()
        logger.info("SQL-скрипт выполнен успешно")