from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result, retry_if_exception_type
from httpx import RequestError
from typing import Any, Coroutine, Dict, Tuple
from uuid import UUID

from prefect import get_client
from prefect.deployments import run_deployment
from prefect.exceptions import ObjectAlreadyExists, ObjectNotFound
from prefect.futures import as_completed, PrefectFuture
from prefect_shell import ShellOperation
from prefect.states import raise_state_exception
from prefect.tasks import Task
from prefect.variables import Variable

from modules.utils import render_text
from modules.logger import get_logger

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

@RETRY_SENSITIVE_ACTIONS
def get_prefect_variable(variable_name: str) -> str:
    return Variable.get(variable_name).__str__()

def prepare_variable(variable_name: str, data: Dict[str, str] ) -> str | None:
    """
    Подготовка Prefect Variable.
    В передаваемом словаре ключи и значения должны быть строками.   
    """
    var = get_prefect_variable(variable_name)
    if var is not None:
        var = render_text(var, data)
    return var


@RETRY_SENSITIVE_ACTIONS
def get_prefect_shell_block(block_name: str) -> ShellOperation | Coroutine[Any, Any, ShellOperation] | None:
    return ShellOperation.load(block_name)

def prepare_shell_block(
                        block_name: str,
                        data: dict | None = None
                       ) -> ShellOperation | Coroutine[Any, Any, ShellOperation] | None:
    """
    Подготовка блока ShellOperation для выполнения команды.
    В случае, если в передаваемых данных есть словарь, его ключи и значения должны быть строками.
    Разделы: env[dict], shell[str], commands[dict], extension[str], working_dir[Path], stream_output[bool].
    """
    logger = get_logger()
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
                                    new_cmds.append(render_text(cmd_template, v))
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
                    
async def set_tag_gcl(tag:str, resource_type:str, demand:int | None) -> None:
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
    from config import CPUS_PER_WORKER, CPUS_MAX_LOAD_PERC, GPUS_PER_WORKER, RAM_PER_WORKER, RAM_MAX_LOAD_PERC
    logger = get_logger()
    
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

def submit_to_prefect(
                      prefect_task_params: Dict[str, Any],
                      run_args: Dict[str, Any],
                      handler: Task,
                      prefect_subflow_params: Dict[str, Any] | None = None,
                     ) -> PrefectFuture[Tuple[Dict[str, Dict[str, Any]], bool]]:
    """
    Запуск в работу таски Prefect.
    Если prefect_flow_params переданы, запускаем подпоток с этими опциями.
    Иначе — обычную задачу.
    Возвращаем PrefectFuture, который можно дождаться через await.
    """
    # Добываем имя задания
    task_name = run_args.pop('task_name')
    # Если предполагается
    print(f"prefect_task_params:, {prefect_task_params}\nrun args: {run_args}\nprefect_subflow_params: {prefect_subflow_params}")
    match prefect_subflow_params:
        case dict():
            prefect_subflow_params.update({'flow_run_name':f"[Subflow] {task_name}"})
            run_args.update(**prefect_subflow_params)
    return handler.with_options(task_run_name=f"[Task] {task_name}", **prefect_task_params).submit(**run_args)

def collect_from_prefect(
                         tasks: Dict[str, PrefectFuture],
                         timeout:float
                        ) -> Dict[str, Any]:
    """
    Собирает результаты из списка, содержащего PrefectFuture и корутины.
    
    Args:
        tasks: список объектов, которые можно ожидать (awaitable).
        timeout: таймаут ожидания        
    Returns:
        Словарь вида {имя задания: результаты} в порядке завершения задач.
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
        for task in as_completed(list(prefect_futures.values()), timeout=timeout):
            task_name:str = next((k for k in prefect_futures.keys() if tasks[k]==task), 'unknown')
            result = task.result()
            results.update({task_name:result})
    except TimeoutError:
        pass
    return results

def get_result_from_subflow(
                            deployment_name:str|UUID,
                            run_parameters:Dict[str, Any],
                            subflow_parameters:Dict[str, Any]
                           ) -> Any:
    """
    Запускает синхронно сабфлоу на основе развёрнутого деплоймента.
    Args:
        deployment_name: имя/идентификатор деплоя
        run_parameters: аргументы для флоу-функции
        subflow_parameters: аргументы для запуска деплоймента
    Returns:
        Результаты выполнения сабфлоу
    """
    subflow = run_deployment(
                             name=deployment_name,
                             parameters=run_parameters,
                             **subflow_parameters
                            )
    raise_state_exception(subflow.state) # type: ignore
    result = subflow.state.result(raise_on_failure=True) # type: ignore
    return result
    