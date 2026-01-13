#!/usr/bin/env python3
"""
Константы для БД.
"""

# Пути к файлам
METADATA_FILE = "db_meta.json"
DATA_DIR = "data"

# Поддерживаемые типы данных
SUPPORTED_TYPES = {"int", "str", "bool"}

# Операторы сравнения для WHERE условий
COMPARISON_OPERATORS = {">", "<", ">=", "<=", "==", "!="}

# Булевые значения
TRUE_VALUES = {"true", "1", "yes", "да"}
FALSE_VALUES = {"false", "0", "no", "нет"}

# Автоматические колонки
AUTO_ID_COLUMN = ("ID", "int")

# Сообщения об ошибках
ERROR_TABLE_EXISTS = 'Таблица "{}" уже существует.'
ERROR_TABLE_NOT_EXISTS = 'Таблица "{}" не существует.'
ERROR_COLUMN_NOT_EXISTS = 'Столбец "{}" не существует в таблице "{}".'
ERROR_INVALID_TYPE = 'Неверный тип данных: "{}". Поддерживаемые типы: int, str, bool.'
ERROR_INVALID_FORMAT = 'Некорректный формат: "{}". Используйте "{}".'
