#!/usr/bin/env python3
"""
Engine module - handles command parsing and user interaction.
The main loop of our database application.
"""

import prompt
import shlex
from src.primitive_db.core import create_table, drop_table, list_tables, insert_record, select_records
from src.primitive_db.utils import load_metadata, save_metadata, print_help, pretty_print_table

def run():
    """
    Main database loop - handles user commands and coordinates operations.
    """
    metadata_file = "db_meta.json"
    
    print("🚀 База данных запущена!")
    print_help()
    
    while True:
        try:
            # Load current metadata at the start of each command
            metadata = load_metadata(metadata_file)
            
            # Get user input
            user_input = prompt.string(">>>Введите команду: ").strip()
            if not user_input:
                continue
                
            # Split command using shlex for proper handling of quotes and spaces
            args = shlex.split(user_input)
            command = args[0].lower()
            
            # Process commands
            if command == "exit":
                print("👋 Выход из программы. Данные сохранены.")
                break
                
            elif command == "help":
                print_help()
                
            elif command == "create_table":
                if len(args) < 2:
                    print("❌ Ошибка: Используйте: create_table <имя_таблицы> <столбец1:тип> ...")
                    continue
                
                table_name = args[1]
                columns = args[2:]  # Remaining arguments are column definitions
                
                try:
                    metadata = create_table(metadata, table_name, columns)
                    save_metadata(metadata_file, metadata)
                except ValueError as e:
                    print(f"❌ {e}")
                    
            elif command == "drop_table":
                if len(args) != 2:
                    print("❌ Ошибка: Используйте: drop_table <имя_таблицы>")
                    continue
                
                table_name = args[1]
                
                try:
                    metadata = drop_table(metadata, table_name)
                    save_metadata(metadata_file, metadata)
                except ValueError as e:
                    print(f"❌ {e}")
                    
            elif command == "insert":
                if len(args) < 3:
                    print("❌ Ошибка: Используйте: insert <имя_таблицы> <столбец1=значение> ...")
                    continue
                
                table_name = args[1]
                values = args[2:]  # Column=value pairs
                
                try:
                    metadata = insert_record(metadata, table_name, values)
                    save_metadata(metadata_file, metadata)
                    
                    # Также сохраняем данные в отдельный файл
                    from src.primitive_db.utils import save_table_data
                    table_data = metadata[table_name].get('data', [])
                    save_table_data(table_name, table_data)
                    
                except ValueError as e:
                    print(f"❌ {e}")
                    
            elif command == "list_tables":
                list_tables(metadata)
                
            elif command == "select":
                if len(args) < 2:
                    print("❌ Ошибка: Используйте: select <имя_таблицы> [where условие]")
                    continue
                
                table_name = args[1]
                condition = None
                
                # Check for "where" keyword
                if len(args) >= 4 and args[2].lower() == "where":
                    condition = args[3]
                elif len(args) >= 3:
                    # If no "where" but 3+ args, assume condition without keyword
                    condition = args[2]
                
                try:
                    records = select_records(metadata, table_name, condition)
                    pretty_print_table(records, table_name)
                    
                except ValueError as e:
                    print(f"❌ {e}") 
            else:
                print(f"❌ Функции '{command}' нет. Попробуйте снова.")  
        except KeyboardInterrupt:
            print("\n👋 Прервано пользователем. Выход.")
            break
        except Exception as e:
            print(f"❌ Неожиданная ошибка: {e}")
