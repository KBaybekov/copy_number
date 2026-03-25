from jinja2 import Template
from typing import Tuple

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