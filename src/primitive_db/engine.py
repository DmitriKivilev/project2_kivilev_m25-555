#!/usr/bin/env python3
"""
Engine module - handles command parsing and user interaction.
The main loop of our database application.
"""

import prompt
import shlex
from src.primitive_db.core import create_table, drop_table, list_tables
from src.primitive_db.utils import load_metadata, save_metadata, print_help


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
                    
            elif command == "list_tables":
                list_tables(metadata)
                
            else:
                print(f"❌ Функции '{command}' нет. Попробуйте снова.")
                
        except KeyboardInterrupt:
            print("\n👋 Прервано пользователем. Выход.")
            break
        except Exception as e:
            print(f"❌ Неожиданная ошибка: {e}")
