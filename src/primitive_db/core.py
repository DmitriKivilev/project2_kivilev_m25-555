#!/usr/bin/env python3
import re
from typing import Any, Dict, List

from src.primitive_db.constants import (
    AUTO_ID_COLUMN,
    COMPARISON_OPERATORS,
    ERROR_COLUMN_NOT_EXISTS,
    ERROR_INVALID_FORMAT,
    ERROR_INVALID_TYPE,
    ERROR_TABLE_EXISTS,
    ERROR_TABLE_NOT_EXISTS,
    FALSE_VALUES,
    TRUE_VALUES,
)
from src.primitive_db.decorators import (
    cache_results,
    confirm_action,
    handle_db_errors,
    log_time,
)
from src.primitive_db.utils import validate_column_definition


@handle_db_errors
@log_time
def create_table(
    metadata: Dict[str, Any], table_name: str, columns: List[str]
) -> Dict[str, Any]:
    """Создает новую таблицу с указанными столбцами"""
    if table_name in metadata:
        raise ValueError(ERROR_TABLE_EXISTS.format(table_name))
    
    validated_columns = []
    
    validated_columns.append(AUTO_ID_COLUMN)
    print(
        f"Автоматически добавлен столбец "
        f"{AUTO_ID_COLUMN[0]}:{AUTO_ID_COLUMN[1]}"
    )
    
    for column_def in columns:
        result = validate_column_definition(column_def)
        if result is None:
            raise ValueError(f'Некорректное значение: "{column_def}"')
        
        col_name, col_type = result
        validated_columns.append((col_name, col_type))
    
    metadata[table_name] = {
        'columns': validated_columns,
        'data': []
    }
    
    column_list = ', '.join(
        [f'{name}:{type}' for name, type in validated_columns]
    )
    print(
        f'Таблица "{table_name}" успешно создана '
        f'со столбцами: {column_list}'
    )
    
    return metadata


@handle_db_errors
@confirm_action("удалить таблицу")
@log_time
def drop_table(
    metadata: Dict[str, Any], table_name: str
) -> Dict[str, Any]:
    """Удаляет таблицу из базы данных."""

    if table_name not in metadata:
        raise ValueError(ERROR_TABLE_NOT_EXISTS.format(table_name))
    
    del metadata[table_name]
    print(f'Таблица "{table_name}" успешно удалена.')
    
    return metadata


@log_time
def list_tables(metadata: Dict[str, Any]) -> None:
    """Выводит список всех таблиц в базе данных."""

    if not metadata:
        print("В базе данных нет таблиц.")
        return
    
    print("Список таблиц:")
    for table_name in metadata.keys():
        print(f"- {table_name}")


def get_table_info(
    metadata: Dict[str, Any], table_name: str
) -> Dict[str, Any] | None:
    """Получает информацию о таблице."""

    return metadata.get(table_name)


@handle_db_errors
@log_time
@cache_results(max_size=50)
def insert_record(
    metadata: Dict[str, Any], table_name: str, values: List[str]
) -> Dict[str, Any]:
    """Добавляет новую запись в таблицу."""

    if table_name not in metadata:
        raise ValueError(ERROR_TABLE_NOT_EXISTS.format(table_name))
    
    table_info = metadata[table_name]
    columns = table_info['columns'][1:]  # Пропускаем ID столбец
    
    record = {}
    for value_str in values:
        if '=' not in value_str:
            raise ValueError(
                ERROR_INVALID_FORMAT.format(value_str, "столбец=значение")
            )
        
        col_name, col_value = value_str.split('=', 1)
        col_name = col_name.strip()
        col_value = col_value.strip()
        
        column_def = None
        for col_def_name, col_type in columns:
            if col_def_name == col_name:
                column_def = (col_def_name, col_type)
                break
        
        if not column_def:
            raise ValueError(
                ERROR_COLUMN_NOT_EXISTS.format(col_name, table_name)
            )
        
        col_name, col_type = column_def
        try:
            if col_type == 'int':
                record[col_name] = int(col_value)
            elif col_type == 'bool':
                col_value_lower = col_value.lower()
                if col_value_lower in TRUE_VALUES:
                    record[col_name] = True
                elif col_value_lower in FALSE_VALUES:
                    record[col_name] = False
                else:
                    raise ValueError(
                        f'Неверное значение для bool: "{col_value}"'
                    )
            elif col_type == 'str':
                if (col_value.startswith('"') and col_value.endswith('"')) \
                   or (col_value.startswith("'") and col_value.endswith("'")):
                    record[col_name] = col_value[1:-1]
                else:
                    record[col_name] = col_value
            else:
                raise ValueError(ERROR_INVALID_TYPE.format(col_type))
        except ValueError:
            raise ValueError(
                f'Неверное значение для столбца "{col_name}" '
                f'(тип {col_type}): "{col_value}"'
            )
    
    for col_name, col_type in columns:
        if col_name not in record:
            raise ValueError(
                f'Отсутствует значение для обязательного столбца: "{col_name}"'
            )
    
    existing_data = table_info.get('data', [])
    if existing_data:
        last_id = max([r.get('ID', 0) for r in existing_data])
        new_id = last_id + 1
    else:
        new_id = 1
    
    complete_record = {'ID': new_id}
    complete_record.update(record)
    
    if 'data' not in table_info:
        table_info['data'] = []
    
    table_info['data'].append(complete_record)
    metadata[table_name] = table_info
    
    print(f'Запись добавлена в таблицу "{table_name}" с ID={new_id}')
    return metadata


@handle_db_errors
@log_time
def select_records(
    metadata: Dict[str, Any], table_name: str, condition: str = None
) -> List[Dict]:
    """Выбирает записи из таблицы с опциональным условием."""

    if table_name not in metadata:
        raise ValueError(ERROR_TABLE_NOT_EXISTS.format(table_name))
    
    table_info = metadata[table_name]
    records = table_info.get('data', [])
    
    if not condition:
        return records
    
    match = re.match(r'(\w+)([<>=!]+)(.+)', condition)
    if not match:
        raise ValueError(
            f'Некорректное условие: "{condition}". '
            f'Используйте "столбец оператор значение"'
        )
    
    col_name, operator, value_str = match.groups()
    
    if operator not in COMPARISON_OPERATORS:
        raise ValueError(f'Неподдерживаемый оператор: "{operator}"')
    
    column_types = {name: type for name, type in table_info['columns']}
    if col_name not in column_types:
        raise ValueError(
            ERROR_COLUMN_NOT_EXISTS.format(col_name, table_name)
        )
    
    col_type = column_types[col_name]
    
    try:
        if col_type == 'int':
            value = int(value_str)
        elif col_type == 'bool':
            value_lower = value_str.lower()
            value = value_lower in TRUE_VALUES
        elif col_type == 'str':
            if (value_str.startswith('"') and value_str.endswith('"')) \
               or (value_str.startswith("'") and value_str.endswith("'")):
                value = value_str[1:-1]
            else:
                value = value_str
        else:
            raise ValueError(ERROR_INVALID_TYPE.format(col_type))
    except ValueError:
        raise ValueError(
            f'Неверное значение для столбца "{col_name}" '
            f'(тип {col_type}): "{value_str}"'
        )
    
    filtered_records = []
    for record in records:
        record_value = record.get(col_name)
        
        if col_name not in record:
            continue
            
        match_condition = False
        if operator == '>':
            match_condition = record_value > value
        elif operator == '<':
            match_condition = record_value < value
        elif operator == '>=':
            match_condition = record_value >= value
        elif operator == '<=':
            match_condition = record_value <= value
        elif operator == '==':
            match_condition = record_value == value
        elif operator == '!=':
            match_condition = record_value != value
        
        if match_condition:
            filtered_records.append(record)
    
    return filtered_records


@handle_db_errors
@confirm_action("обновить записи")
@log_time
def update_records(
    metadata: Dict[str, Any], table_name: str, 
    set_clause: str, where_clause: str = None
) -> Dict[str, Any]:
    """Обновляет записи в таблице."""

    if table_name not in metadata:
        raise ValueError(ERROR_TABLE_NOT_EXISTS.format(table_name))
    
    table_info = metadata[table_name]
    records = table_info.get('data', [])
    
    if not records:
        print(f"Таблица '{table_name}' пуста, нечего обновлять")
        return metadata
    
    set_updates = {}
    set_parts = [part.strip() for part in set_clause.split(',')]
    
    for set_part in set_parts:
        if '=' not in set_part:
            raise ValueError(
                ERROR_INVALID_FORMAT.format(set_part, "столбец=значение")
            )
        
        col_name, new_value_str = set_part.split('=', 1)
        col_name = col_name.strip()
        new_value_str = new_value_str.strip()
        
        column_types = {name: type for name, type in table_info['columns']}
        if col_name not in column_types:
            raise ValueError(
                ERROR_COLUMN_NOT_EXISTS.format(col_name, table_name)
            )
        
        col_type = column_types[col_name]
        
        try:
            if col_type == 'int':
                set_updates[col_name] = int(new_value_str)
            elif col_type == 'bool':
                value_lower = new_value_str.lower()
                set_updates[col_name] = value_lower in TRUE_VALUES
            elif col_type == 'str':
                if (new_value_str.startswith('"') 
                    and new_value_str.endswith('"')) \
                   or (new_value_str.startswith("'") 
                       and new_value_str.endswith("'")):
                    set_updates[col_name] = new_value_str[1:-1]
                else:
                    set_updates[col_name] = new_value_str
            else:
                raise ValueError(ERROR_INVALID_TYPE.format(col_type))
        except ValueError:
            raise ValueError(
                f'Неверное значение для столбца "{col_name}" '
                f'(тип {col_type}): "{new_value_str}"'
            )
    
    def record_matches(record: Dict, condition: str) -> bool:
        if not condition:
            return True
        
        match = re.match(r'(\w+)([<>=!]+)(.+)', condition)
        if not match:
            raise ValueError(f'Некорректное условие WHERE: "{condition}"')
        
        col_name, operator, value_str = match.groups()
        
        if col_name not in record:
            return False
        
        col_type = column_types.get(col_name)
        if not col_type:
            return False
        
        try:
            if col_type == 'int':
                value = int(value_str)
            elif col_type == 'bool':
                value_lower = value_str.lower()
                value = value_lower in TRUE_VALUES
            elif col_type == 'str':
                if (value_str.startswith('"') and value_str.endswith('"')) \
                   or (value_str.startswith("'") and value_str.endswith("'")):
                    value = value_str[1:-1]
                else:
                    value = value_str
            else:
                return False
        except ValueError:
            return False
        
        record_value = record[col_name]
        if operator == '>':
            return record_value > value
        elif operator == '<':
            return record_value < value
        elif operator == '>=':
            return record_value >= value
        elif operator == '<=':
            return record_value <= value
        elif operator == '==':
            return record_value == value
        elif operator == '!=':
            return record_value != value
        
        return False
    
    updated_count = 0
    for record in records:
        if record_matches(record, where_clause):
            for col_name, new_value in set_updates.items():
                record[col_name] = new_value
            updated_count += 1
    
    table_info['data'] = records
    metadata[table_name] = table_info
    
    if updated_count > 0:
        print(f"Обновлено {updated_count} записей в таблице '{table_name}'")
    else:
        print(f"Не найдено записей для обновления в таблице '{table_name}'")
    
    return metadata


@handle_db_errors
@confirm_action("удалить записи")
@log_time
def delete_records(
    metadata: Dict[str, Any], table_name: str, 
    where_clause: str = None
) -> Dict[str, Any]:
    """Удаляет записи из таблицы."""

    if table_name not in metadata:
        raise ValueError(ERROR_TABLE_NOT_EXISTS.format(table_name))
    
    table_info = metadata[table_name]
    records = table_info.get('data', [])
    
    if not records:
        print(f"Таблица '{table_name}' уже пуста")
        return metadata
    
    if not where_clause:
        table_info['data'] = []
        metadata[table_name] = table_info
        print(f"Удалены все записи из таблицы '{table_name}'")
        return metadata
    
    match = re.match(r'(\w+)([<>=!]+)(.+)', where_clause)
    if not match:
        raise ValueError(
            f'Некорректное условие: "{where_clause}". '
            f'Используйте "столбец оператор значение"'
        )
    
    col_name, operator, value_str = match.groups()
    
    column_types = {name: type for name, type in table_info['columns']}
    if col_name not in column_types:
        raise ValueError(
            ERROR_COLUMN_NOT_EXISTS.format(col_name, table_name)
        )
    
    col_type = column_types[col_name]
    
    try:
        if col_type == 'int':
            value = int(value_str)
        elif col_type == 'bool':
            value_lower = value_str.lower()
            value = value_lower in TRUE_VALUES
        elif col_type == 'str':
            if (value_str.startswith('"') and value_str.endswith('"')) \
               or (value_str.startswith("'") and value_str.endswith("'")):
                value = value_str[1:-1]
            else:
                value = value_str
        else:
            raise ValueError(ERROR_INVALID_TYPE.format(col_type))
    except ValueError:
        raise ValueError(
            f'Неверное значение для столбца "{col_name}" '
            f'(тип {col_type}): "{value_str}"'
        )
    
    def record_matches(record: Dict) -> bool:
        if col_name not in record:
            return False
        
        record_value = record[col_name]
        
        if operator == '>':
            return record_value > value
        elif operator == '<':
            return record_value < value
        elif operator == '>=':
            return record_value >= value
        elif operator == '<=':
            return record_value <= value
        elif operator == '==':
            return record_value == value
        elif operator == '!=':
            return record_value != value
        
        return False
    
    initial_count = len(records)
    filtered_records = [
        record for record in records if not record_matches(record)
    ]
    deleted_count = initial_count - len(filtered_records)
    
    table_info['data'] = filtered_records
    metadata[table_name] = table_info
    
    if deleted_count > 0:
        print(f"Удалено {deleted_count} записей из таблицы '{table_name}'")
    else:
        print(f"Не найдено записей для удаления в таблице '{table_name}'")
    
    return metadata
