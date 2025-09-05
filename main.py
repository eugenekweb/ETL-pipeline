import sys
import os
import logging
from datetime import datetime

try:
    from py_scripts.etl_pipeline import ETLPipeline
    from py_scripts.file_utils import normalize_date
except ImportError as e:
    print(f"Ошибка импорта модулей: {e}")
    print("Убедитесь, что все файлы находятся в правильных директориях")
    sys.exit(1)


def setup_logging():
    """Настройка логирования"""
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(
        log_dir, 
        f"etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return log_file


def validate_arguments():
    """Валидация аргументов командной строки"""
    if len(sys.argv) < 2:
        print("Ошибка: Не указана дата")
        print("Использование: python main.py ДАТА [--config config.json]")
        print("Поддерживаемые форматы даты:")
        print("  - DDMMYYYY (например: 01032021)")
        print("  - DD-MM-YYYY (например: 01-03-2021)")
        print("  - DD.MM.YYYY (например: 01.03.2021)")
        print("  - DD/MM/YYYY (например: 01/03/2021)")
        sys.exit(1)
    
    date_str = sys.argv[1]
    
    # Нормализация даты в формат DDMMYYYY
    try:
        normalized_date = normalize_date(date_str)
        print(f"Дата '{date_str}' преобразована в формат: {normalized_date}")
        return normalized_date
    except ValueError as e:
        print(f"Ошибка в формате даты: {e}")
        print("Поддерживаемые форматы:")
        print("  - DDMMYYYY (например: 01032021)")
        print("  - DD-MM-YYYY (например: 01-03-2021)")
        print("  - DD.MM.YYYY (например: 01.03.2021)")
        print("  - DD/MM/YYYY (например: 01/03/2021)")
        sys.exit(1)


def main():
    """Главная функция"""
    print("=" * 60)
    print("ETL-процесс для банковских данных")
    print("=" * 60)
    
    # Настройка логирования
    log_file = setup_logging()
    print(f"Лог-файл: {log_file}")
    
    # Валидация и нормализация аргументов
    date_str = validate_arguments()

    # Определение конфигурационного файла
    config_path = "config.json"
    if "--config" in sys.argv:
        config_index = sys.argv.index("--config")
        if config_index + 1 < len(sys.argv):
            config_path = sys.argv[config_index + 1]

    print(f"Дата обработки: {date_str}")
    print(f"Конфигурационный файл: {config_path}")
    print("-" * 60)

    try:
        # Проверка существования конфигурационного файла
        if not os.path.exists(config_path):
            print(
                f"Ошибка: Конфигурационный файл не найден: {config_path}"
            )
            sys.exit(1)
        
        # Создание и запуск ETL-процесса
        logging.info(
            f"Запуск ETL-процесса для даты: {date_str}"
        )
        etl = ETLPipeline(config_path)
        
        # Обработка данных
        etl.process_date(date_str)
        
        
        # Получение и вывод отчета по мошенничеству
        print("\n" + "=" * 60)
        print("ОТЧЕТ ПО МОШЕННИЧЕСТВУ")
        print("=" * 60)
        
        # fraud_report = etl.get_fraud_report(date_str)
        fraud_report = None
        
        if not fraud_report.empty:
            print(f"Найдено {len(fraud_report)} случаев мошенничества:")
            print("-" * 60)
            
            # Группировка по типам мошенничества
            fraud_types = fraud_report.groupby('event_type').size()
            for fraud_type, count in fraud_types.items():
                print(f"{fraud_type}: {count}")
            
            print("\nПодробная информация:")
            print("-" * 60)
            
            # Вывод детальной информации
            for _, row in fraud_report.iterrows():
                print(f"Время: {row['event_dt']}")
                print(f"Паспорт: {row['passport']}")
                print(f"ФИО: {row['fio']}")
                print(f"Тип: {row['event_type']}")
                print("-" * 30)
        else:
            print("Случаев мошенничества не найдено")
        
        print("\n" + "=" * 60)
        print("ОБРАБОТКА ЗАВЕРШЕНА УСПЕШНО")
        print("=" * 60)
        
    except FileNotFoundError as e:
        print(f"Ошибка: Файл не найден - {e}")
        logging.error(f"Файл не найден: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"Ошибка валидации: {e}")
        logging.error(f"Ошибка валидации: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        logging.error(f"Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
