#!/usr/bin/env python3
"""
Декораторы для базы данных.
"""
import json
import time
from functools import wraps
from typing import Any, Callable


def handle_db_errors(func: Callable) -> Callable:
    """
    Декоратор для обработки ошибок базы данных.
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        try:
            return func(*args, **kwargs)
        except ValueError as e:
            raise e
        except Exception as e:
            # Для других ошибок выводим подробности
            print(f"Критическая ошибка в {func.__name__}: {e}")
            raise
    return wrapper


def confirm_action(action_description: str) -> Callable:
    """
    Декоратор для подтверждения действий.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            import prompt
            answer = prompt.string(
                f"Вы уверены, что хотите {action_description}? (yes/no): "
            )
            
            if answer.lower() in ['yes', 'y', 'да', 'д']:
                return func(*args, **kwargs)
            else:
                print(" Действие отменено.")
                # Возвращаем метаданные без изменений
                if args:
                    return args[0]  # первый аргумент — это metadata
                return None
        return wrapper
    return decorator

def log_time(func: Callable) -> Callable:
    """
    Декоратор для измерения времени выполнения функции.
    """
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        execution_time = end_time - start_time
        print(
            f"️ Функция '{func.__name__}' "
            f"выполнилась за {execution_time:.4f} секунд"
        )
        
        return result
    return wrapper


def cache_results(max_size: int = 100) -> Callable:
    """
    Декоратор для кэширования результатов функций.
    """
    def decorator(func: Callable) -> Callable:
        cache = {}
        cache_keys = []
        
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Создаем сериализуемый ключ кэша
            try:
                cache_key = json.dumps((args, kwargs), sort_keys=True, default=str)
            except:
                cache_key = str((args, kwargs))
            
            if cache_key in cache:
                print(f"Результат взят из кэша (функция: {func.__name__})")
                return cache[cache_key]
            
            result = func(*args, **kwargs)
            
            # Добавляем в кэш
            cache[cache_key] = result
            cache_keys.append(cache_key)
            
            # Ограничение для размера кэша
            if len(cache_keys) > max_size:
                oldest_key = cache_keys.pop(0)
                del cache[oldest_key]
            
            return result
        
        # Управление кэшом
        def clear_cache():
            """Очистить кэш."""
            cache.clear()
            cache_keys.clear()
        
        def get_cache_size():
            """Текущий размер кэша."""
            return len(cache)
        
        wrapper.clear_cache = clear_cache
        wrapper.get_cache_size = get_cache_size
        
        return wrapper
    return decorator
