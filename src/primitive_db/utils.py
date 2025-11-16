#!/usr/bin/env python3

import json
from typing import Dict, Any


def load_metadata(filepath: str = "db_meta.json") -> Dict[str, Any]:
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
    try:
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=2, ensure_ascii=False)
        print(f"Metadata saved to {filepath}")
    except IOError as e:
        print(f" Error saving metadata: {e}")


def validate_column_definition(column_def: str) -> tuple[str, str] | None:
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
        print(f"Invalid data type: '{col_type}'. Supported types: int, str, bool")
        return None
    
    return name, col_type


def print_help():
    """
    Print help message
    """
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")
