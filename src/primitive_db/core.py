#!/usr/bin/env python3
from typing import Dict, Any, List
from src.primitive_db.utils import validate_column_definition


def create_table(metadata: Dict[str, Any], table_name: str, columns: List[str]) -> Dict[str, Any]:
    if table_name in metadata:
        raise ValueError(f'Таблица "{table_name}" уже существует.')
    
    validated_columns = []
    
    validated_columns.append(('ID', 'int'))
    print("Автоматически добавлен столбец ID:int")
    
    for column_def in columns:
        result = validate_column_definition(column_def)
        if result is None:
            raise ValueError(f'Некорректное значение: "{column_def}"')
        
        col_name, col_type = result
        validated_columns.append((col_name, col_type))
    
    metadata[table_name] = {
        'columns': validated_columns,
        'data': []  # Will store actual data records later
    }
    
    column_list = ', '.join([f'{name}:{type}' for name, type in validated_columns])
    print(f' Таблица "{table_name}" успешно создана со столбцами: {column_list}')
    return metadata

def drop_table(metadata: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    
    # Remove table from metadata
    del metadata[table_name]
    print(f' Таблица "{table_name}" успешно удалена.')
    return metadata

def list_tables(metadata: Dict[str, Any]) -> None:
    if not metadata:
        print("В базе данных нет таблиц.")
        return
    
    print("Список таблиц:")
    for table_name in metadata.keys():
        print(f"- {table_name}")


def get_table_info(metadata: Dict[str, Any], table_name: str) -> Dict[str, Any] | None:
    return metadata.get(table_name)


def insert_record(metadata: Dict[str, Any], table_name: str, values: List[str]) -> Dict[str, Any]:
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
        
        col_name, col_type = column_def
        try:
            if col_type == 'int':
                record[col_name] = int(col_value)
            elif col_type == 'bool':
                col_value_lower = col_value.lower()
                if col_value_lower in ['true', '1', 'yes', 'да']:
                    record[col_name] = True
                elif col_value_lower in ['false', '0', 'no', 'нет']:
                    record[col_name] = False
                else:
                    raise ValueError(f'Неверное значение для bool: "{col_value}"')
            elif col_type == 'str':
                if (col_value.startswith('"') and col_value.endswith('"')) or \
                   (col_value.startswith("'") and col_value.endswith("'")):
                    record[col_name] = col_value[1:-1]
                else:
                    record[col_name] = col_value
            else:
                raise ValueError(f'Неизвестный тип данных: {col_type}')
        except ValueError as e:
            raise ValueError(f'Неверное значение для столбца "{col_name}" (тип {col_type}): "{col_value}"')
    
    for col_name, col_type in columns:
        if col_name not in record:
            raise ValueError(f'Отсутствует значение для обязательного столбца: "{col_name}"')
    
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

def select_records(metadata: Dict[str, Any], table_name: str, condition: str = None) -> List[Dict]:
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    
    table_info = metadata[table_name]
    records = table_info.get('data', [])
    
    if not condition:
        return records
    
    import re
    
    match = re.match(r'(\w+)([<>=!]+)(.+)', condition)
    if not match:
        raise ValueError(f'Некорректное условие: "{condition}". Используйте "столбец оператор значение"')
    
    col_name, operator, value_str = match.groups()
    
    column_types = {name: type for name, type in table_info['columns']}
    if col_name not in column_types:
        raise ValueError(f'Столбец "{col_name}" не существует в таблице "{table_name}"')
    
    col_type = column_types[col_name]
    
    try:
        if col_type == 'int':
            value = int(value_str)
        elif col_type == 'bool':
            value_lower = value_str.lower()
            value = value_lower in ['true', '1', 'yes', 'да']
        elif col_type == 'str':
            # Remove quotes if present
            if (value_str.startswith('"') and value_str.endswith('"')) or \
               (value_str.startswith("'") and value_str.endswith("'")):
                value = value_str[1:-1]
            else:
                value = value_str
        else:
            raise ValueError(f'Неизвестный тип данных: {col_type}')
    except ValueError:
        raise ValueError(f'Неверное значение для столбца "{col_name}" (тип {col_type}): "{value_str}"')
    
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
        else:
            raise ValueError(f'Неподдерживаемый оператор: "{operator}"')
        
        if match_condition:
            filtered_records.append(record)
    
    return filtered_records
    return metadata
def update_records(metadata: Dict[str, Any], table_name: str, set_clause: str, where_clause: str = None) -> Dict[str, Any]:
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    
    table_info = metadata[table_name]
    records = table_info.get('data', [])
    
    if not records:
        print(f"️ Таблица '{table_name}' пуста, нечего обновлять")
        return metadata
    
    set_updates = {}
    set_parts = [part.strip() for part in set_clause.split(',')]
    
    for set_part in set_parts:
        if '=' not in set_part:
            raise ValueError(f'Некорректный SET: "{set_part}". Используйте "столбец=значение"')
        
        col_name, new_value_str = set_part.split('=', 1)
        col_name = col_name.strip()
        new_value_str = new_value_str.strip()
        
        column_types = {name: type for name, type in table_info['columns']}
        if col_name not in column_types:
            raise ValueError(f'Столбец "{col_name}" не существует в таблице "{table_name}"')
        
        col_type = column_types[col_name]
        
        try:
            if col_type == 'int':
                set_updates[col_name] = int(new_value_str)
            elif col_type == 'bool':
                value_lower = new_value_str.lower()
                set_updates[col_name] = value_lower in ['true', '1', 'yes', 'да']
            elif col_type == 'str':
                # Remove quotes if present
                if (new_value_str.startswith('"') and new_value_str.endswith('"')) or \
                   (new_value_str.startswith("'") and new_value_str.endswith("'")):
                    set_updates[col_name] = new_value_str[1:-1]
                else:
                    set_updates[col_name] = new_value_str
            else:
                raise ValueError(f'Неизвестный тип данных: {col_type}')
        except ValueError:
            raise ValueError(f'Неверное значение для столбца "{col_name}" (тип {col_type}): "{new_value_str}"')
    
    def record_matches(record: Dict, condition: str) -> bool:
        if not condition:
            return True
        
        import re
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
                value = value_lower in ['true', '1', 'yes', 'да']
            elif col_type == 'str':
                if (value_str.startswith('"') and value_str.endswith('"')) or \
                   (value_str.startswith("'") and value_str.endswith("'")):
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
        print(f" Обновлено {updated_count} записей в таблице '{table_name}'")
    else:
        print(f" Не найдено записей для обновления в таблице '{table_name}'")
    
    return metadata

def delete_records(metadata: Dict[str, Any], table_name: str, where_clause: str = None) -> Dict[str, Any]:
    if table_name not in metadata:
        raise ValueError(f'Таблица "{table_name}" не существует.')
    
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
    
    import re
    
    match = re.match(r'(\w+)([<>=!]+)(.+)', where_clause)
    if not match:
        raise ValueError(f'Некорректное условие: "{where_clause}". Используйте "столбец оператор значение"')
    
    col_name, operator, value_str = match.groups()
    
    column_types = {name: type for name, type in table_info['columns']}
    if col_name not in column_types:
        raise ValueError(f'Столбец "{col_name}" не существует в таблице "{table_name}"')
    
    col_type = column_types[col_name]
    
    try:
        if col_type == 'int':
            value = int(value_str)
        elif col_type == 'bool':
            value_lower = value_str.lower()
            value = value_lower in ['true', '1', 'yes', 'да']
        elif col_type == 'str':
            if (value_str.startswith('"') and value_str.endswith('"')) or \
               (value_str.startswith("'") and value_str.endswith("'")):
                value = value_str[1:-1]
            else:
                value = value_str
        else:
            raise ValueError(f'Неизвестный тип данных: {col_type}')
    except ValueError:
        raise ValueError(f'Неверное значение для столбца "{col_name}" (тип {col_type}): "{value_str}"')
    
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
    filtered_records = [record for record in records if not record_matches(record)]
    deleted_count = initial_count - len(filtered_records)
    
    table_info['data'] = filtered_records
    metadata[table_name] = table_info
    
    if deleted_count > 0:
        print(f"Удалено {deleted_count} записей из таблицы '{table_name}'")
    else:
        print(f"Не найдено записей для удаления в таблице '{table_name}'")
    
    return metadata
