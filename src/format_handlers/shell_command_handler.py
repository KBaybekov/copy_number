from jinja2 import Template
from prefect_shell import ShellOperation

def form_shell_command(shell_command_template:str, data:dict) -> str:
    """
    Формирует команду для запуска в оболочке на основе шаблона и данных.
    """
    return Template(shell_command_template).render(**data)