from asyncio import as_completed as as_completed_async
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result, retry_if_exception_type
from httpx import RequestError
from typing import Any, Coroutine, Dict, Tuple

from prefect.types import StrictVariableValue
from prefect.variables import Variable
from prefect_shell import ShellOperation
from prefect import flow, get_client
from prefect.exceptions import ObjectAlreadyExists, ObjectNotFound
from prefect.futures import PrefectFuture, as_completed as as_completed_prefect
from prefect.tasks import Task

from format_handlers.shell_command_handler import form_shell_command
from modules.utils import interpret_exit_code
from modules.logger import get_logger
from config import CPUS_PER_WORKER, CPUS_MAX_LOAD_PERC, GPUS_PER_WORKER, RAM_PER_WORKER, RAM_MAX_LOAD_PERC

logger = get_logger()

cfg_template = Variable.get('nxf_cfg_alignment_v1')

# Конфигурация повторных попыток при запросе данных с сервера
RETRY_SENSITIVE_ACTIONS = retry(
       stop=stop_after_attempt(3), 
       wait=wait_fixed(2),
       retry=retry_if_result(lambda res: res is None)
      )

# Конфигурация повторных попыток при работе с тегами
RETRY_TAG_ACTIONS = retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(1),
    retry=retry_if_exception_type(RequestError)
)

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

@RETRY_SENSITIVE_ACTIONS
def get_prefect_shell_block(block_name: str) -> ShellOperation | Coroutine[Any, Any, ShellOperation] | None:
    return ShellOperation.load(block_name)

@RETRY_SENSITIVE_ACTIONS
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

async def set_tag_gcl(tag:str, resource_type:str, demand:int | None = None) -> None:
    """
    Устанавливает/изменяет в Prefect глобальный concurrency лимит по тегу 
    (все задачи, запущенные с этим тегом, будут ограничены этим лимитом).
    В расчёт берется общее кол-во ресурсов воркера и его максимальная загрузка (%) 
    Args:
        tag: Имя тега (например, "cpu:worker1:type_a").
        resource_type: Тип ресурса ("cpu", "gpu", "ram").
        demand: Количество единиц ресурса, необходимое для одной задачи.
                Если None, лимит удаляется.
    """
    @RETRY_TAG_ACTIONS
    async def create_or_update():
        try:
            await client.create_concurrency_limit(tag=tag, concurrency_limit=tag_limit)
        except ObjectAlreadyExists:
            try:
                await client.delete_concurrency_limit_by_tag(tag=tag)
            except ObjectNotFound:
                pass
            await client.create_concurrency_limit(tag=tag, concurrency_limit=tag_limit)
        return None

    async with get_client() as client:
        if demand is None:
            # удаление лимита
            try:
                await client.delete_concurrency_limit_by_tag(tag)
            except ObjectNotFound:
                # лимит не существовал
                return None
        else:
            resource_amount = 0
            match resource_type:
                case 'cpu':
                    resource_amount = CPUS_PER_WORKER * CPUS_MAX_LOAD_PERC / 100
                case 'gpu':
                    resource_amount = GPUS_PER_WORKER
                case 'ram':
                    resource_amount = RAM_PER_WORKER * RAM_MAX_LOAD_PERC / 100
                case _:
                    logger.error(f"Неверный идентификатор ресурса: {resource_type}")
                    return None
                
            tag_limit = int(resource_amount // demand)
            if tag_limit == 0:
                logger.warning(f"ВНИМАНИЕ! Для тега '{tag}' установлен лимит 0.")
            
            await create_or_update()
    return None

@flow(name="One Task Subflow")
async def one_task_subflow(
                           prefect_task_args: Dict[str, Any],
                           run_args: Dict[str, Any],
                           handler: Task
                          ) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    """
    Флоу-обёртка для запуска задания вне основного воркфлоу (например, в GPU-очереди)
    """
    """run = await submit_to_prefect(prefect_task_args, run_args, handler)
    return run"""
    future = handler.with_options(**prefect_task_args).submit(**run_args)
    return await future

def submit_to_prefect(
                            prefect_task_args: Dict[str, Any],
                            run_args: Dict[str, Any],
                            handler: Task,
                            prefect_flow_args: Dict[str, Any] | None = None,
                           ) -> Coroutine | PrefectFuture[Tuple[Dict[str, Dict[str, Any]], bool]]:
    """
    Запуск в работу флоу/таски Prefect.
    Если prefect_flow_args переданы, запускаем подпоток (one_task_subflow) с этими опциями.
    Иначе — обычную задачу.
    Возвращаем PrefectFuture, который можно дождаться через await.
    """
    # Добываем образец и имя задания
    sample = run_args.pop('sample')
    task_name = run_args.pop('task_name')
    match prefect_flow_args:
        case dict():
            """run = one_task_subflow.with_options(**prefect_flow_args)(prefect_task_args, run_args, handler)
            return run"""
            run =  one_task_subflow.with_options(flow_run_name=f"[Subflow] {task_name}", **prefect_flow_args)(
                prefect_task_args=prefect_task_args,
                run_args=run_args,
                handler=handler
            )
            return run
        case None:
            return handler.with_options(task_run_name=f"[Task] {task_name}", **prefect_task_args).submit(sample=sample, **run_args)

def collect_from_prefect(
                               tasks: Dict[str, PrefectFuture | Coroutine],
                               timeout:float
                               ) -> Dict[str, Any]:
    """
    Собирает результаты из списка, содержащего PrefectFuture и корутины.
    
    Args:
        tasks: список объектов, которые можно ожидать (awaitable).
        
    Returns:
        Список результатов в порядке завершения задач.
    """
    results = {}
    coroutines = {}
    prefect_futures = {}
    for task_name, task in tasks.items():
        match task:
            case PrefectFuture():
                prefect_futures.update({task_name:task})
            case Coroutine():
                coroutines.update({task_name:task})
    try:
        for task in as_completed_prefect(list(prefect_futures.values()), timeout=timeout):
            task_name:str = next((k for k in prefect_futures.keys() if tasks[k]==task), 'unknown')
            result = task.result()
            results.update({task_name:result})
    except TimeoutError:
        pass
    try:
        for task in as_completed_async(list(coroutines.values()), timeout=timeout):
            task_name:str = next((k for k in coroutines.keys() if tasks[k]==task), 'unknown')
            result = task.result()
            results.update({task_name:result})
    except TimeoutError:
        pass
    return results
