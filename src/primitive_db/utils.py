#!/usr/bin/env python3
"""
Utility functions for database operations.
Working with metadata files and data validation.
"""

import os
import json
from typing import Dict, Any, Tuple, Optional
from typing import Dict, Any, Tuple, Optional, List

def load_metadata(filepath: str = "db_meta.json") -> Dict[str, Any]:
    """
    Load metadata from JSON file.
    
    Args:
        filepath (str): Path to metadata file
        
    Returns:
        Dict[str, Any]: Loaded metadata or empty dict if file doesn't exist
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"🆕 Metadata file '{filepath}' not found. Creating new database.")
        return {}
    except json.JSONDecodeError as e:
        print(f"❌ Error reading metadata file: {e}")
        return {}


def save_metadata(filepath: str, data: Dict[str, Any]) -> None:
    """
    Save metadata to JSON file.
    
    Args:
        filepath (str): Path to metadata file
        data (Dict): Metadata to save
    """
    try:
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=2, ensure_ascii=False)
        print(f"💾 Metadata saved to {filepath}")
    except IOError as e:
        print(f"❌ Error saving metadata: {e}")


def validate_column_definition(column_def: str) -> Optional[Tuple[str, str]]:
    """
    Validate column definition format: 'name:type'
    
    Args:
        column_def (str): Column definition string
        
    Returns:
        Optional[Tuple[str, str]]: (column_name, column_type) if valid, None if invalid
    """
    if ':' not in column_def:
        print(f"❌ Invalid column format: '{column_def}'. Use 'name:type'")
        return None
    
    name, col_type = column_def.split(':', 1)
    name = name.strip()
    col_type = col_type.strip().lower()
    
    if not name:
        print(f"❌ Column name cannot be empty in: '{column_def}'")
        return None
    
    if col_type not in ['int', 'str', 'bool']:
        print(f"❌ Invalid data type: '{col_type}'. Supported types: int, str, bool")
        return None
    
    return (name, col_type)


def print_help():
    """
    Print help message for table management mode.
    """
    print("\n***Процесс работы с таблицей***")
    print("Функции:")
    print("<command> create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> .. - создать таблицу")
    print("<command> list_tables - показать список всех таблиц")
    print("<command> drop_table <имя_таблицы> - удалить таблицу")
    print("<command> insert <имя_таблицы> <столбец1=значение> ... - добавить запись")
    print("<command> select <имя_таблицы> [where условие] - показать записи")
    print("  Примеры: select users, select users where age>25")
    print("<command> exit - выход из программы")
    print("<command> help - справочная информация\n")

def save_table_data(table_name: str, data: list, data_dir: str = "data") -> None:
    """
    Save table data to a separate JSON file.
    
    Args:
        table_name (str): Name of the table
        data (list): Table data to save
        data_dir (str): Directory for data files
    """
    try:
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        filepath = os.path.join(data_dir, f"{table_name}.json")
        with open(filepath, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=2, ensure_ascii=False)
        print(f"💾 Данные таблицы '{table_name}' сохранены в {filepath}")
    except IOError as e:
        print(f"❌ Ошибка сохранения данных: {e}")


def load_table_data(table_name: str, data_dir: str = "data") -> list:
    """
    Load table data from JSON file.
    
    Args:
        table_name (str): Name of the table
        data_dir (str): Directory for data files
        
    Returns:
        list: Loaded data or empty list if file doesn't exist
    """
    try:
        filepath = os.path.join(data_dir, f"{table_name}.json")
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as file:
                return json.load(file)
        return []
    except (FileNotFoundError, json.JSONDecodeError):
        return []
def pretty_print_table(records: List[Dict], table_name: str) -> None:
    """
    Pretty print table records.
    
    Args:
        records (List[Dict]): Records to display
        table_name (str): Name of the table
    """
    if not records:
        print(f"📭 Таблица '{table_name}' пуста")
        return
    
    print(f"\n📋 Данные таблицы '{table_name}':")
    print("=" * 50)
    
    # Get all column names
    all_keys = set()
    for record in records:
        all_keys.update(record.keys())
    
    # Order columns: ID first, then alphabetical
    sorted_keys = sorted(all_keys)
    if 'ID' in sorted_keys:
        sorted_keys.remove('ID')
        sorted_keys = ['ID'] + sorted_keys
    
    # Print header
    header = " | ".join(sorted_keys)
    print(header)
    print("-" * len(header))
    
    # Print rows
    for record in records:
        row = []
        for key in sorted_keys:
            value = record.get(key, '')
            row.append(str(value))
        print(" | ".join(row))
    
    print(f"Всего записей: {len(records)}")
