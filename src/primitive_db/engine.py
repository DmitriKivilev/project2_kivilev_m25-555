#!/usr/bin/env python3
#!/usr/bin/env python3

import prompt
import shlex
from src.primitive_db.core import create_table, drop_table, list_tables, insert_record, select_records, update_records, delete_records
from src.primitive_db.utils import load_metadata, save_metadata, print_help, pretty_print_table


def run():
    metadata_file = "db_meta.json"
    
    print("База данных запущена!")
    print_help()
    
    while True:
        try:
            metadata = load_metadata(metadata_file)
            
            user_input = prompt.string(">>>Введите команду: ").strip()
            if not user_input:
                continue
                
            args = shlex.split(user_input)
            command = args[0].lower()
            
            if command == "exit":
                print("Выход из программы. Данные сохранены.")
                break
                
            elif command == "help":
                print_help()
                
            elif command == "create_table":
                if len(args) < 2:
                    print("Ошибка: Используйте: create_table <имя_таблицы> <столбец1:тип> ...")
                    continue
                
                table_name = args[1]
                columns = args[2:]
                
                try:
                    metadata = create_table(metadata, table_name, columns)
                    save_metadata(metadata_file, metadata)
                except ValueError as e:
                    print(f"{e}")
                    
            elif command == "drop_table":
                if len(args) != 2:
                    print("Ошибка: Используйте: drop_table <имя_таблицы>")
                    continue
                
                table_name = args[1]
                
                try:
                    metadata = drop_table(metadata, table_name)
                    save_metadata(metadata_file, metadata)
                except ValueError as e:
                    print(f"{e}")
                    
            elif command == "insert":
                if len(args) < 3:
                    print("Ошибка: Используйте: insert <имя_таблицы> <столбец1=значение> ...")
                    continue
                
                table_name = args[1]
                values = args[2:]
                
                try:
                    metadata = insert_record(metadata, table_name, values)
                    save_metadata(metadata_file, metadata)
                    
                    from src.primitive_db.utils import save_table_data
                    table_data = metadata[table_name].get('data', [])
                    save_table_data(table_name, table_data)
                    
                except ValueError as e:
                    print(f"{e}")
                    
            elif command == "list_tables":
                list_tables(metadata)
                
            elif command == "select":
                if len(args) < 2:
                    print("Ошибка: Используйте: select <имя_таблицы> [where условие]")
                    continue
                
                table_name = args[1]
                condition = None
                
                if len(args) >= 4 and args[2].lower() == "where":
                    condition = args[3]
                elif len(args) >= 3:
                    condition = args[2]
                
                try:
                    records = select_records(metadata, table_name, condition)
                    pretty_print_table(records, table_name)
                    
                except ValueError as e:
                    print(f"{e}")
                
            elif command == "update":
                if len(args) < 4:
                    print("Ошибка: Используйте: update <таблица> set <столбец=значение> [where условие]")
                    continue
                
                table_name = args[1]
                
                if args[2].lower() != "set":
                    print("Ошибка: Отсутствует ключевое слово 'set'. Используйте: update <таблица> set ...")
                    continue
                
                set_clause_parts = []
                where_clause = None
                found_where = False
                
                for i in range(3, len(args)):
                    if args[i].lower() == "where":
                        found_where = True
                        continue
                    
                    if found_where:
                        where_clause = args[i]
                        break
                    else:
                        set_clause_parts.append(args[i])
                
                set_clause = " ".join(set_clause_parts)
                
                try:
                    metadata = update_records(metadata, table_name, set_clause, where_clause)
                    save_metadata(metadata_file, metadata)
                    
                    from src.primitive_db.utils import save_table_data
                    table_data = metadata[table_name].get('data', [])
                    save_table_data(table_name, table_data)
                    
                except ValueError as e:
                    print(f"{e}")
            
            elif command == "delete":
                if len(args) < 2:
                    print("Ошибка: Используйте: delete <таблица> [where условие]")
                    continue
                
                table_name = args[1]
                where_clause = None
                
                if len(args) >= 4 and args[2].lower() == "where":
                    where_clause = args[3]
                elif len(args) >= 3:
                    where_clause = args[2]
                
                try:
                    metadata = delete_records(metadata, table_name, where_clause)
                    save_metadata(metadata_file, metadata)
                    
                    from src.primitive_db.utils import save_table_data
                    table_data = metadata[table_name].get('data', [])
                    save_table_data(table_name, table_data)
                    
                except ValueError as e:
                    print(f"{e}")
    
            else:
                print(f"Функции '{command}' нет. Попробуйте снова.")
                
        except KeyboardInterrupt:
            print("\nПрервано пользователем. Выход.")
            break
        except Exception as e:
            print(f"Неожиданная ошибка: {e}")
