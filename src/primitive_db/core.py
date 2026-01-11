#!/usr/bin/env python3
"""
Core module for table management operations.
Handles table creation, deletion, and metadata management.
"""

from typing import Dict, Any, List
from src.primitive_db.utils import validate_column_definition


def create_table(metadata: Dict[str, Any], table_name: str, columns: List[str]) -> Dict[str, Any]:
    """
    Create a new table with specified columns.
    
    Args:
        metadata (Dict): Current database metadata
        table_name (str): Name of the table to create
        columns (List[str]): List of column definitions
        
    Returns:
        Dict[str, Any]: Updated metadata
        
    Raises:
        ValueError: If table already exists or invalid columns
    """
    # Check if table already exists
    if table_name in metadata:
        raise ValueError(f'Таблица "{table_name}" уже существует.')
    
    # Validate and process columns
    validated_columns = []
    
    # Automatically add ID column as first column
    validated_columns.append(('ID', 'int'))
    print(f"✅ Автоматически добавлен столбец ID:int")
    
    # Process user-defined columns
    for column_def in columns:
        result = validate_column_definition(column_def)
        if result is None:
            raise ValueError(f'Некорректное значение: "{column_def}"')
        
        col_name, col_type = result
        validated_columns.append((col_name, col_type))
    
    # Create table structure in metadata
    metadata[table_name] = {
        'columns': validated_columns,
        'data': []  # Will store actual data records later
    }
    
    # Format column list for success message
    column_list = ', '.join([f'{name}:{type}' for name, type in validated_columns])
    print(f'✅ Таблица "{table_name}" успешно создана со столбцами: {column_list}')
    
    return metadata


def drop_table(metadata: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    """
    Drop (delete) a table.
    
    Args:
        metadata (Dict): Current database metadata
        table_name (str): Name of the table to drop
        
    Returns:
        Dict[str, Any]: Updated metadata
        
    Raises:
        ValueError: If table doesn't exist
    """
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    
    # Remove table from metadata
    del metadata[table_name]
    print(f'✅ Таблица "{table_name}" успешно удалена.')
    
    return metadata


def list_tables(metadata: Dict[str, Any]) -> None:
    """
    List all tables in the database.
    
    Args:
        metadata (Dict): Current database metadata
    """
    if not metadata:
        print("📭 В базе данных нет таблиц.")
        return
    
    print("📋 Список таблиц:")
    for table_name in metadata.keys():
        print(f"- {table_name}")


def get_table_info(metadata: Dict[str, Any], table_name: str) -> Dict[str, Any] | None:
    """
    Get information about a specific table.
    
    Args:
        metadata (Dict): Current database metadata
        table_name (str): Name of the table
        
    Returns:
        Dict[str, Any] | None: Table info or None if table doesn't exist
    """
def insert_record(metadata: Dict[str, Any], table_name: str, values: List[str]) -> Dict[str, Any]:
    """
    Insert a new record into a table.
    
    Args:
        metadata (Dict): Current database metadata
        table_name (str): Name of the table
        values (List[str]): List of value assignments like "name=John"
        
    Returns:
        Dict[str, Any]: Updated metadata
        
    Raises:
        ValueError: If table doesn't exist or invalid values
    """
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    
    table_info = metadata[table_name]
    columns = table_info['columns'][1:]  # Skip ID column (it's auto-generated)
    
    # Parse values
    record = {}
    for value_str in values:
        if '=' not in value_str:
            raise ValueError(f'Некорректный формат значения: "{value_str}". Используйте "столбец=значение"')
        
        col_name, col_value = value_str.split('=', 1)
        col_name = col_name.strip()
        col_value = col_value.strip()
        
        # Find column definition
        column_def = None
        for col_def_name, col_type in columns:
            if col_def_name == col_name:
                column_def = (col_def_name, col_type)
                break
        
        if not column_def:
            raise ValueError(f'Столбец "{col_name}" не существует в таблице "{table_name}"')
        
        # Validate and convert value based on type
        col_name, col_type = column_def
        try:
            if col_type == 'int':
                record[col_name] = int(col_value)
            elif col_type == 'bool':
                # Accept different boolean representations
                col_value_lower = col_value.lower()
                if col_value_lower in ['true', '1', 'yes', 'да']:
                    record[col_name] = True
                elif col_value_lower in ['false', '0', 'no', 'нет']:
                    record[col_name] = False
                else:
                    raise ValueError(f'Неверное значение для bool: "{col_value}"')
            elif col_type == 'str':
                # Remove quotes if present
                if (col_value.startswith('"') and col_value.endswith('"')) or \
                   (col_value.startswith("'") and col_value.endswith("'")):
                    record[col_name] = col_value[1:-1]
                else:
                    record[col_name] = col_value
            else:
                raise ValueError(f'Неизвестный тип данных: {col_type}')
        except ValueError as e:
            raise ValueError(f'Неверное значение для столбца "{col_name}" (тип {col_type}): "{col_value}"')
    
    # Check if all required columns are provided
    for col_name, col_type in columns:
        if col_name not in record:
            raise ValueError(f'Отсутствует значение для обязательного столбца: "{col_name}"')
    
    # Generate ID (auto-increment)
    existing_data = table_info.get('data', [])
    if existing_data:
        last_id = max([r.get('ID', 0) for r in existing_data])
        new_id = last_id + 1
    else:
        new_id = 1
    
    # Create complete record with ID
    complete_record = {'ID': new_id}
    complete_record.update(record)
    
    # Add to data
    if 'data' not in table_info:
        table_info['data'] = []
    
    table_info['data'].append(complete_record)
    metadata[table_name] = table_info
    
    print(f'✅ Запись добавлена в таблицу "{table_name}" с ID={new_id}')
    return metadata
