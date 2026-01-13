#!/usr/bin/env python3
import json
import os
from typing import Any, Dict, List, Optional, Tuple


def load_metadata(filepath: str = "db_meta.json") -> Dict[str, Any]:
    """Загружаем метаданные из json"""
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"Metadata file '{filepath}' not found. Creating new database.")
        return {}
    except json.JSONDecodeError as e:
        print(f"Error reading metadata file: {e}")
        return {}


def save_metadata(filepath: str, data: Dict[str, Any]) -> None:
    """Сохраняем в json"""
    try:
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=2, ensure_ascii=False)
        print(f"Metadata saved to {filepath}")
    except IOError as e:
        print(f"Error saving metadata: {e}")


def validate_column_definition(
    column_def: str
) -> Optional[Tuple[str, str]]:
    """Проверяет корректность имя:тип"""
    if ':' not in column_def:
        print(f"Invalid column format: '{column_def}'. Use 'name:type'")
        return None
    
    name, col_type = column_def.split(':', 1)
    name = name.strip()
    col_type = col_type.strip().lower()
    
    if not name:
        print(f"Column name cannot be empty in: '{column_def}'")
        return None
    
    if col_type not in ['int', 'str', 'bool']:
        print(
            f"Invalid data type: '{col_type}'. "
            f"Supported types: int, str, bool"
        )
        return None
    
    return (name, col_type)


def print_help() -> None:
    """Выводит основную информацию для помощи юзеру"""
    print("\n***Работа с таблицей***")
    print("Для манипуляций используйте функции:")
    print(
        "create_table <назовите таблицу> <столбец:тип> <столбец2:тип> и тд. "
        "- создать таблицу(типы(bool(true, 1, yes, да),int или str))"
    )
    print("list_tables - показать список всех таблиц")
    print(
        "drop_table <введите имя таблцы которую хотите удалить> "
        "- удалить таблицу"
    )
    print("insert <имя_таблицы> <столбец=значение> и тд - добавить запись")
    print(
        "select <имя_таблицы> [where условие(><==)] - показать записи"
    )
    print(
        "update <таблица> set <столбец=значение> "
        "[where условие] - обновить записи"
    )
    print(
        "Например: update users set age=26 или  "
        "update users set score=90 where name=='John'"
    )
    print("delete <таблица> [where условие] - удалить записи")
    print("Например: delete users, delete users where age<18")
    print("exit - выход из программы")
    print("help - справочная информация\n")


def save_table_data(
    table_name: str, data: list, data_dir: str = "data"
) -> None:
    """Сохраняет актуальные данные таблицы в json"""
    try:
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        filepath = os.path.join(data_dir, f"{table_name}.json")
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=2, ensure_ascii=False)
        print(f"Данные таблицы '{table_name}' сохранены в {filepath}")
    except IOError as e:
        print(f"Ошибка сохранения данных: {e}")


def load_table_data(
    table_name: str, data_dir: str = "data"
) -> list:
    """Загружает из json данные таблиц"""
    try:
        filepath = os.path.join(data_dir, f"{table_name}.json")
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as file:
                return json.load(file)
        return []
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def pretty_print_table(records: List[Dict], table_name: str) -> None:
    """Для вывода таблицы в виде тоблицы"""
    if not records:
        print(f"Таблица '{table_name}' пуста")
        return
    
    print(f"\n Данные таблицы '{table_name}':")
    print("=" * 50)
    
    all_keys = set()
    for record in records:
        all_keys.update(record.keys())
    
    sorted_keys = sorted(all_keys)
    if 'ID' in sorted_keys:
        sorted_keys.remove('ID')
        sorted_keys = ['ID'] + sorted_keys
    
    header = " | ".join(sorted_keys)
    print(header)
    print("-" * len(header))
    
    for record in records:
        row = []
        for key in sorted_keys:
            value = record.get(key, '')
            row.append(str(value))
        print(" | ".join(row))
    
    print(f"Всего записей: {len(records)}")
