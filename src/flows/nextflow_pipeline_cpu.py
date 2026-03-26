from jinja2 import Template
from pathlib import Path
from prefect import flow
from prefect_shell import ShellOperation
from prefect.variables import Variable
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result
from typing import Any, Dict, List, Tuple

# Конфигурация повторных попыток при запросе данных с сервера
RETRY_SENSITIVE_ACTIONS = retry(
       stop=stop_after_attempt(3), 
       wait=wait_fixed(2),
       retry=retry_if_result(lambda res: res is None)
      )


@RETRY_SENSITIVE_ACTIONS
def get_prefect_variable(variable_name: str) -> str:
    return Variable.get(variable_name).__str__()


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

def render_text(template:str, data:dict) -> str:
    """
    Формирует команду для запуска в оболочке на основе шаблона и данных.
    """
    return Template(template).render(**data)

# Загрузка переменных из Prefect
prefect_vars = ['nxf_cfg_alignment_v1', 'nxf_cmd_docker']
LOADED_PREFECT_VARS = {var:get_prefect_variable(var) for var in prefect_vars}

@flow
async def nextflow_pipeline_cpu(
                          pipeline:Path|str,
                          log:Path,
                          configuration_parameters:Dict[str, Any]
                         ) -> Tuple[bool, str]:
    """
    Запуск пайплайна Nextflow через отдельный деплой.
    Args:
        pipeline: название пайплайна или путь к папке, содержащей main.nf
        log: Path-объект файла лога
        configuration_parameters:
            словарь, содержащий параметры пайплайна.\n
            **ДОЛЖЕН** содержать:
                - 'cfg_file' - Path-объект для сохранения конфигурации пайплайна
                - 'cfg_template' - Prefect-переменная, содержащая шаблон конфигурации
                - 'shell_working_dir' - Path-объект, путь рабочей директории
            **ОПЦИОНАЛЬНО**:
                - 'cmds_before' - список str-команд, выполняемых до запуска Nextflow
                - 'cmds_after' - список str-команд, выполняемых после запуска Nextflow
                - 'env' - словарь переменных среды

    Returns:
        Кортеж, где первый элемент — флаг успеха (True/False),
        а второй — сообщение об ошибке (пустая строка при успехе).
    """
    # Извлекаем обязательные и опциональные аргументы для запуска
    cfg_file:Path = configuration_parameters.pop('cfg_file')
    cfg_template:str = configuration_parameters.pop('cfg_template')
    shell_working_dir:Path = configuration_parameters.pop('shell_working_dir')
    optional_shell_args = {}
    for arg in ['cmds_before', 'cmds_after', 'env']:
        try:
            arg_val = configuration_parameters.pop(arg)
        except KeyError:
            match arg:
                case 'env':
                    arg_val = {}
                case _:
                    arg_val = []
        optional_shell_args.update({arg:arg_val})
    
    
    # Формируем файл конфигурации
    with open(cfg_file, 'w') as f:
        config = render_text(
                             template=LOADED_PREFECT_VARS.get(cfg_template, ""),
                             data=configuration_parameters
                            )
        f.write(config)

    # Формируем данные для заполнения шаблона
    cmd_data = {
                "log_path": log.as_posix(),
                "pipeline":pipeline,
                "nxf_cfg": cfg_file.as_posix()
               }

    # Формируем shell-команду
    nextflow_command = [render_text(
                                   template=LOADED_PREFECT_VARS.get("nxf_cmd_docker", ""),
                                   data=cmd_data
                                  )]
    # Добавляем подготовительные и постпроцессинговые команды
    shell_cmds:List[str] = optional_shell_args['cmds_before'] + nextflow_command + optional_shell_args['cmds_after']
    
    # Запускаем пайплайн
    shell_op = ShellOperation(
                              commands=shell_cmds,
                              env=optional_shell_args.get('env', {}),
                              working_dir=shell_working_dir,
                              stream_output=True
                             )
    # Запускаем процесс
    process = shell_op.trigger() # type: ignore
    # Ждем завершения (заблокирует выполнение потока до конца пайплайна)
    await process.wait_for_completion() # type: ignore
    return_code:int = process.return_code # type: ignore
    return interpret_exit_code(return_code)
