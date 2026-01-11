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
    return metadata.get(table_name)
