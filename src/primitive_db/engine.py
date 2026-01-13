#!/usr/bin/env python3

import prompt

from src.primitive_db.constants import DATA_DIR, METADATA_FILE
from src.primitive_db.core import (
    create_table,
    delete_records,
    drop_table,
    insert_record,
    list_tables,
    select_records,
    update_records,
)
from src.primitive_db.parser import (
    parse_command,
    parse_create_table,
    parse_delete,
    parse_drop_table,
    parse_insert,
    parse_select,
    parse_update,
)
from src.primitive_db.utils import (
    load_metadata,
    pretty_print_table,
    print_help,
    save_metadata,
    save_table_data,
)


def run() -> None:
    """Основной цикл работы базы данных"""
    print("База данных запущена!")
    print_help()
    
    while True:
        try:
            metadata = load_metadata(METADATA_FILE)
            
            user_input = prompt.string(">>>Введите команду: ").strip()
            if not user_input:
                continue
            
            try:
                command, args = parse_command(user_input)
            except ValueError as e:
                print(f"{e}")
                continue
            
            if command == "exit":
                print("Выход из программы. Данные сохранены.")
                break
                
            elif command == "help":
                print_help()
                
            elif command == "create_table":
                try:
                    table_name, columns = parse_create_table(args)
                    metadata = create_table(metadata, table_name, columns)
                    save_metadata(METADATA_FILE, metadata)
                except ValueError as e:
                    print(f"{e}")
                    
            elif command == "drop_table":
                try:
                    table_name = parse_drop_table(args)
                    metadata = drop_table(metadata, table_name)
                    save_metadata(METADATA_FILE, metadata)
                except ValueError as e:
                    print(f"{e}")
                    
            elif command == "insert":
                try:
                    table_name, values = parse_insert(args)
                    metadata = insert_record(metadata, table_name, values)
                    save_metadata(METADATA_FILE, metadata)
                    
                    table_data = metadata[table_name].get('data', [])
                    save_table_data(table_name, table_data, DATA_DIR)
                    
                except ValueError as e:
                    print(f"{e}")
                    
            elif command == "list_tables":
                list_tables(metadata)
                
            elif command == "select":
                try:
                    table_name, condition = parse_select(args)
                    records = select_records(
                        metadata, table_name, condition
                    )
                    pretty_print_table(records, table_name)
                    
                except ValueError as e:
                    print(f"{e}")
                
            elif command == "update":
                try:
                    table_name, set_clause, where_clause = parse_update(args)
                    metadata = update_records(
                        metadata, table_name, set_clause, where_clause
                    )
                    save_metadata(METADATA_FILE, metadata)
                    
                    table_data = metadata[table_name].get('data', [])
                    save_table_data(table_name, table_data, DATA_DIR)
                    
                except ValueError as e:
                    print(f"{e}")
                
            elif command == "delete":
                try:
                    table_name, where_clause = parse_delete(args)
                    metadata = delete_records(
                        metadata, table_name, where_clause
                    )
                    save_metadata(METADATA_FILE, metadata)
                    
                    table_data = metadata[table_name].get('data', [])
                    save_table_data(table_name, table_data, DATA_DIR)
                    
                except ValueError as e:
                    print(f"{e}")
                
            else:
                print(f"Функции '{command}' нет. Попробуйте снова.")
                
        except KeyboardInterrupt:
            print("\n Прервано пользователем. Выход.")
            break
        except Exception as e:
            print(f" Неожиданная ошибка: {e}")
