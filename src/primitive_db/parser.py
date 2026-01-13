#!/usr/bin/env python3

# Парсинг команд базы данных бд

import shlex
from typing import List, Optional, Tuple


def parse_command(user_input: str) -> Tuple[str, List[str]]:
    """Разбирает пользовательский ввод на команду и аргументы."""
    if not user_input.strip():
        return "", []
    
    try:
        args = shlex.split(user_input)
        command = args[0].lower()
        arguments = args[1:]
        return command, arguments
    except ValueError as e:
        raise ValueError(f"Ошибка парсинга команды: {e}")


def parse_create_table(args: List[str]) -> Tuple[str, List[str]]:
    """Парсит аргументы команды create_table"""
    if len(args) < 1:
        raise ValueError(
            "Недостаточно аргументов. "
            "Используйте: create_table <имя_таблицы> <столбец1:тип> ..."
        )
    
    table_name = args[0]
    columns = args[1:] if len(args) > 1 else []
    
    return table_name, columns


def parse_drop_table(args: List[str]) -> str:
    """Парсит аргументы команды drop_table."""
    if len(args) != 1:
        raise ValueError(
            "Неверное количество аргументов. "
            "Используйте: drop_table <имя_таблицы>"
        )
    
    return args[0]


def parse_insert(args: List[str]) -> Tuple[str, List[str]]:
    """Парсит аргументы команды insert"""
    if len(args) < 2:
        raise ValueError(
            "Недостаточно аргументов. "
            "Используйте: insert <имя_таблицы> <столбец1=значение> ..."
        )
    
    table_name = args[0]
    values = args[1:]
    
    return table_name, values


def parse_select(args: List[str]) -> Tuple[str, Optional[str]]:
    """Парсит аргументы команды select"""
    if len(args) < 1:
        raise ValueError(
            "Недостаточно аргументов. "
            "Используйте: select <имя_таблицы> [where условие]"
        )
    
    table_name = args[0]
    condition = None
    
    if len(args) >= 3 and args[1].lower() == "where":
        condition = args[2]
    elif len(args) >= 2:
        condition = args[1]
    
    return table_name, condition


def parse_update(args: List[str]) -> Tuple[str, str, Optional[str]]:
    """Парсит аргументы команды update"""
    if len(args) < 3:
        raise ValueError(
            "Недостаточно аргументов. "
            "Используйте: update <таблица> set <столбец=значение> "
            "[where условие]"
        )
    
    table_name = args[0]
    
    if args[1].lower() != "set":
        raise ValueError(
            "Отсутствует ключевое слово 'set'. "
            "Используйте: update <таблица> set ..."
        )
    
    set_clause_parts = []
    where_clause = None
    found_where = False
    
    for i in range(2, len(args)):
        if args[i].lower() == "where":
            found_where = True
            continue
        
        if found_where:
            where_clause = args[i]
            break
        else:
            set_clause_parts.append(args[i])
    
    set_clause = " ".join(set_clause_parts)
    
    return table_name, set_clause, where_clause


def parse_delete(args: List[str]) -> Tuple[str, Optional[str]]:
    """Парсит аргументы команды delete."""
    if len(args) < 1:
        raise ValueError(
            "Недостаточно аргументов. "
            "Используйте: delete <таблица> [where условие]"
        )
    
    table_name = args[0]
    where_clause = None
    
    if len(args) >= 3 and args[1].lower() == "where":
        where_clause = args[2]
    elif len(args) >= 2:
        where_clause = args[1]
    
    return table_name, where_clause


def validate_condition(condition: str) -> bool:
    """Проверяет корректность условия WHERE."""
    import re
    
    if not condition:
        return True
    
    pattern = r'^\w+[<>=!]+\S+$'
    return bool(re.match(pattern, condition))
