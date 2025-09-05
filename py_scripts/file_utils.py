import os
import shutil
import pandas as pd
import re


def get_files_by_date(files_dir, date_str) -> dict:
    """
    Получает файлы для конкретной даты
    """
    files = {}
    
    # Паттерны файлов для конкретной даты
    patterns = {
        'transactions': f'transactions_{date_str}.txt',
        'blacklist': f'passport_blacklist_{date_str}.xlsx',
        'terminals': f'terminals_{date_str}.xlsx'
    }
    
    for file_type, pattern in patterns.items():
        file_path = os.path.join(files_dir, pattern)
        if os.path.exists(file_path):
            files[file_type] = file_path
        else:
            files[file_type] = None
    
    return files


def load_file_to_df(file_path, file_type) -> pd.DataFrame:
    """Загружает файл в DataFrame"""
    if file_type == "txt":
        return pd.read_csv(file_path, sep=';', dtype=str)
    elif file_type == "xlsx":
        return pd.read_excel(file_path, dtype=str)
    else:
        raise ValueError(f"Неизвестный тип файла: {file_type}")


def archive_file(src_path, archive_dir) -> str:
    """Перемещает файл в архив с расширением .backup"""
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"Файл для архивирования не найден: {src_path}")
    
    # Создаем директорию архива если её нет
    os.makedirs(archive_dir, exist_ok=True)
    
    filename = os.path.basename(src_path)
    dst_path = os.path.join(archive_dir, f"{filename}.backup")
    
    # Проверяем, не существует ли уже файл с таким именем
    if os.path.exists(dst_path):
        os.remove(dst_path)
    
    shutil.move(src_path, dst_path)
    return dst_path


def normalize_date(date_str: str) -> str:
    """
    Нормализует дату из различных форматов в DDMMYYYY
    
    Поддерживаемые форматы:
    - DD-MM-YYYY, DD.MM.YYYY, DD/MM/YYYY
    - DD-MM-YY, DD.MM.YY, DD/MM/YY (YY преобразуется в 20YY)
    - DDMMYYYY (без изменений)
    """
    # Убираем все пробелы
    date_str = date_str.strip()
    
    # Если уже в формате DDMMYYYY, возвращаем как есть
    if re.match(r'^\d{8}$', date_str):
        return date_str
    
    # Паттерны для различных форматов даты
    patterns = [
        # DD-MM-YYYY или DD.MM.YYYY или DD/MM/YYYY
        r'^(\d{1,2})[-./](\d{1,2})[-./](\d{4})$',
        # DD-MM-YY или DD.MM.YY или DD/MM/YY
        r'^(\d{1,2})[-./](\d{1,2})[-./](\d{2})$'
    ]
    
    for pattern in patterns:
        match = re.match(pattern, date_str)
        if match:
            day, month, year = match.groups()
            
            # Преобразуем в числа для валидации
            day_int = int(day)
            month_int = int(month)
            year_int = int(year)
            
            # Если год двузначный, добавляем 20
            if year_int < 100:
                year_int += 2000
            
            # Валидация даты
            if day_int < 1 or day_int > 31:
                raise ValueError(f"Некорректный день: {day_int}")
            if month_int < 1 or month_int > 12:
                raise ValueError(f"Некорректный месяц: {month_int}")
            if year_int < 2000 or year_int > 2099:
                raise ValueError(f"Некорректный год: {year_int}")
            
            # Форматируем в DDMMYYYY
            return f"{day_int:02d}{month_int:02d}{year_int}"
    
    raise ValueError(f"Неподдерживаемый формат даты: {date_str}. "
                    f"Используйте DD-MM-YYYY, DD.MM.YYYY, DD/MM/YYYY или DDMMYYYY")
