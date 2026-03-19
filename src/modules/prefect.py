from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result
from typing import Any, Coroutine, Dict, Tuple

from prefect.types import StrictVariableValue
from prefect.variables import Variable
from prefect_shell import ShellOperation

from format_handlers.shell_command_handler import form_shell_command
from modules.utils import interpret_exit_code
from modules.logger import get_logger

logger = get_logger()


cfg_template = Variable.get('nxf_cfg_alignment_v1')

def prepare_shell_block(
                        block_name: str,
                        data: dict | None = None
                       ) -> ShellOperation | Coroutine[Any, Any, ShellOperation] | None:
    """
    Подготовка блока ShellOperation для выполнения команды.
    В случае, если в передаваемых данных есть словарь, его ключи и значения должны быть строками.
    Разделы: env[dict], shell[str], commands[dict], extension[str], working_dir[Path], stream_output[bool].
    """
    block = get_prefect_shell_block(block_name)
    if isinstance(block, ShellOperation):
        if data is not None:
            for k,v in data.items():
                match k:
                    case 'env':
                        match v:
                            case dict():
                                block.env.update(v)
                            case None:
                                block.env.clear()
                            case _:
                                logger.error(f"Ошибка при изменении блока {block_name}. Раздел {k}. Данные: {data}")
                    case 'shell':
                        match v:
                            case str():
                                block.shell = v
                            case None:
                                block.shell = None
                            case _:
                                logger.error(f"Ошибка при изменении блока {block_name}. Раздел {k}. Данные: {data}")
                    case 'commands':
                        match v:
                            case dict():
                                new_cmds = []
                                for cmd_template in block.commands:
                                    new_cmds.append(form_shell_command(cmd_template, v))
                                block.commands = new_cmds
                            case None:
                                block.commands = []
                            case _:
                                logger.error(f"Ошибка при изменении блока {block_name}. Раздел {k}. Данные: {data}")
                    case 'extension':
                        match v:
                            case str():
                                block.extension = v
                            case None:
                                block.extension = None
                            case _:
                                logger.error(f"Ошибка при изменении блока {block_name}. Раздел {k}. Данные: {data}")
                    case 'working_dir':
                        match v:
                            case Path():
                                block.working_dir = v
                            case None:
                                block.working_dir = None
                            case _:
                                logger.error(f"Ошибка при изменении блока {block_name}. Раздел {k}. Данные: {data}")
                    case 'stream_output':
                        match v:
                            case bool():
                                block.stream_output = v
                            case _:
                                logger.error(f"Ошибка при изменении блока {block_name}. Раздел {k}. Данные: {data}")
                    case _:
                        logger.error(f"Ошибка при изменении блока {block_name}. Раздел {k}. Данные: {data}")
    return block
                    
def prepare_variable(variable_name: str, data: Dict[str, str] ) -> str | None:
    """
    Подготовка Prefect Variable.
    В передаваемом словаре ключи и значения должны быть строками.   
    """
    var = get_prefect_variable(variable_name)
    if var is not None:
        var = form_shell_command(var.__str__(), data)
    return var

@retry(
       stop=stop_after_attempt(3), 
       wait=wait_fixed(2),
       retry=retry_if_result(lambda res: res is None)
      )
def get_prefect_shell_block(block_name: str) -> ShellOperation | Coroutine[Any, Any, ShellOperation] | None:
    return ShellOperation.load(block_name)

@retry(
       stop=stop_after_attempt(3), 
       wait=wait_fixed(2),
       retry=retry_if_result(lambda res: res is None)
      )
def get_prefect_variable(variable_name: str) -> StrictVariableValue:
    return Variable.get(variable_name)

def run_nextflow_pipeline(operation_data:dict) -> Tuple[bool, str]:
    """
    Запуск пайплайна Nextflow.
    Возвращает код завершения.
    """
    # Формируем команду запуска
    shell_op = prepare_shell_block(
                                   block_name='nextflow-v1',
                                   data=operation_data
                                  )
    # Запускаем процесс
    process = shell_op.trigger() # type: ignore
    # Ждем завершения (заблокирует выполнение потока до конца пайплайна)
    process.wait_for_completion() # type: ignore
    return_code = process.return_code # type: ignore
    return interpret_exit_code(return_code)