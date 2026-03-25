from jinja2 import Template
from typing import Tuple
import re

def sanitize_artifact_key(raw_key: str) -> str:
    """
    Преобразует строку в безопасный ключ артефакта для Prefect.
    Допустимы только строчные буквы, цифры и дефисы.
    """
    # Приводим к нижнему регистру
    key = raw_key.lower()
    # Заменяем любые символы, кроме a-z, 0-9 и дефиса, на дефис
    key = re.sub(r'[^a-z0-9-]', '-', key)
    # Убираем повторяющиеся дефисы
    key = re.sub(r'-+', '-', key)
    # Удаляем дефисы в начале и конце
    key = key.strip('-')
    # Если строка пуста, возвращаем запасной вариант
    return key or 'empty-key'

def render_text(template:str, data:dict) -> str:
    """
    Формирует команду для запуска в оболочке на основе шаблона и данных.
    """
    return Template(template).render(**data)

def interpret_exit_code(exit_code:int) -> Tuple[bool, str]:
    """
    Интерпретирует числовой exit-код.
    Args:
        exit_code: числовой код окончания процесса
    Returns:
        Кортеж, где первый элемент — флаг успеха (True/False), 
        а второй — сообщение об ошибке (пустая строка при успехе).
    """
    match exit_code:
        case 0:
            return (True, '')
        case 127:
            return (False, 'Command not found')
        case _:
            return (False, f'Processing failed, exitcode: {exit_code}')