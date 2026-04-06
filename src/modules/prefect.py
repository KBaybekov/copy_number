from asyncio import sleep as asleep, run as arun, iscoroutine
from pathlib import Path
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_result, retry_if_exception_type
from httpx import RequestError
from typing import Any, Coroutine, Dict, Optional, Tuple
from uuid import UUID

from prefect import get_client
from prefect.client.schemas import FlowRun, State
from prefect.context import get_run_context, FlowRunContext, TaskRunContext
from prefect.deployments import run_deployment, arun_deployment
from prefect.exceptions import ObjectAlreadyExists, ObjectNotFound
from prefect.futures import as_completed, PrefectFuture
from prefect_shell import ShellOperation
from prefect.states import get_state_result, raise_state_exception
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

async def prepare_shell_block(
                        block_name: str,
                        data: dict | None = None
                       ) -> ShellOperation | Coroutine[Any, Any, ShellOperation] | None:
    """
    Подготовка блока ShellOperation для выполнения команды.
    В случае, если в передаваемых данных есть словарь, его ключи и значения должны быть строками.
    Разделы: env[dict], shell[str], commands[dict], extension[str], working_dir[Path], stream_output[bool].
    """
    logger = await get_logger()
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
    logger = await get_logger()
    
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

async def submit_to_prefect(
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

async def collect_from_prefect(
    tasks: Dict[str, PrefectFuture],
    timeout: float
) -> Dict[str, Any]:
    """
    Асинхронно собирает результаты PrefectFuture, возвращая словарь {имя_задачи: результат}.
    """
    import asyncio
    results = {}
    # Оставляем только PrefectFuture (если есть другие типы – не обрабатываем)
    prefect_futures = {name: task for name, task in tasks.items() if isinstance(task, PrefectFuture)}
    if not prefect_futures:
        return results

    # Вспомогательная корутина, возвращающая (имя, результат)
    async def named_coro(name: str, future: PrefectFuture):
        return name, future.result()

    coros = [named_coro(name, future) for name, future in prefect_futures.items()]

    try:
        # asyncio.as_completed возвращает итератор корутин, которые нужно дождаться
        for coro in asyncio.as_completed(coros, timeout=timeout):
            name, result = await coro
            results[name] = result
    except TimeoutError:
        # По таймауту просто возвращаем то, что успели собрать
        pass

    return results

async def get_result_from_subflow(
    deployment_name: str|UUID,
    run_parameters: Dict[str, Any],
    subflow_parameters: Dict[str, Any],
    poll_interval: int = 10,
    timeout: Optional[int] = None
) -> Tuple[bool, str]:
    """
    Запускает деплоймент и ожидает его завершения с помощью polling.

    Returns:
        (success, error_message)
        - success: True если подпоток завершился успешно
        - error_message: описание ошибки (если success=False), иначе пустая строка
    """
    async def check_flow_run(flow_run: FlowRun):
        poll_interval = 5
        flow_run_id = flow_run.id
        async with get_client() as client:
            while True:
                flow_run = await client.read_flow_run(flow_run_id)  # перечитываем каждую итерацию
                if flow_run.state and flow_run.state.is_final():
                    if flow_run.state.is_completed():
                        return await get_state_result(flow_run.state)
                    else:
                        # обработать ошибку, вернуть (False, причина)
                        return (False, f"Subflow failed: {flow_run.state.message}")
                await asleep(poll_interval)

    try:
        created_flow_run = arun_deployment(
            name=deployment_name,
            parameters=run_parameters,
            **subflow_parameters,
            timeout=0
        )
        match created_flow_run:
            case FlowRun():
                result = await check_flow_run(created_flow_run)
            case _ if iscoroutine(created_flow_run):
                result = await check_flow_run(await created_flow_run)
        return result
    except Exception as e:
        return (False, f"Deployment failed: {str(e)}")

async def _get_result_from_subflow(
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
    print("Now we're in get_result_from_subflow() method")
    # Сериализуем передаваемые в другой флоу данные
    subflow =  arun_deployment(
                             name=deployment_name,
                             parameters=run_parameters,
                             **subflow_parameters
                            )
    
    raise_state_exception(subflow.state)
    print("Getting result of subflow!")
    result = subflow.state.result(raise_on_failure=True) # type: ignore

    """print("run_deployment() happened! Waiting for result...")
    match subflow:
        case FlowRun():
            match subflow.state:
                case State():
                    print("Checking for exceptions...")
                    raise_state_exception(subflow.state)
                    print("Getting result of subflow!")
                    result = subflow.state.result(raise_on_failure=True) # type: ignore"""
    return result

async def __get_result_from_subflow(
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
    poll_interval = 5
    try:
        created_flow_run = await arun_deployment(
            name=deployment_name,
            parameters=run_parameters,
            **subflow_parameters,
            timeout=0
        )

        flow_run_id = created_flow_run.id

        async with get_client() as client:
            flow_run = await client.read_flow_run(flow_run_id)
            while True:
                match flow_run.state:
                    case None:
                        pass
                    case State():
                        match flow_run.state.is_final():
                            case False:
                                pass
                            case True:
                                match flow_run.state.is_completed():
                                    case False:
                                        pass
                                    case True:
                                        result = get_state_result(flow_run.state)
                                        return result
                await asleep(poll_interval)
    except Exception as e:
        # Логируем и возвращаем информацию об ошибке
        return (False, f"Deployment failed: {str(e)}")
        


    print("Now we're in get_result_from_subflow() method")
    # Сериализуем передаваемые в другой флоу данные
    subflow =  await arun_deployment(
                             name=deployment_name,
                             parameters=run_parameters,
                             **subflow_parameters
                            )
    print("run_deployment() happened! Waiting for result...")
    match subflow:
        case FlowRun():
            match subflow.state:
                case State():
                    print("Checking for exceptions...")
                    raise_state_exception(subflow.state)
                    print("Getting result of subflow!")
                    result = subflow.state.result(raise_on_failure=True) # type: ignore
    return result

async def get_run_id() -> str:
    run_id = "unknown"
    try:
        ctx = get_run_context()
        match ctx:
            case FlowRunContext():
                match ctx.flow_run:
                    case FlowRun():
                        run_id = ctx.flow_run.id.__str__()
                    case None:
                        print("FlowRunContext есть, но flow_run = None")
            case TaskRunContext():
                run_id = ctx.task_run.id.__str__()
    except RuntimeError:
        print("Вне контекста Prefect!")
    return run_id

async def create_prefect_run_name(
                            type:str,
                            name:str,
                            parent_id:Optional[str]=None,
                            sample_id: Optional[str]=None,
                            timestamp: Optional[str]=None
                           ) -> str:
    """
    Создаёт строку имени запуска Prefect вида "[Pipeline]:{name}_{timestamp}"/[Subflow|Task]:{name}-[Sample]:{sample_id}-[Parent_id]:{parent_flow_id}
    Args:
        type: Pipeline|Subflow|Task
        name: Произвольное имя
        parent_id: UUID родительского объекта в виде строки
        sample_id: id образца
        timestamp: строковая временная отметка
    """
    match type:
        case 'Pipeline':
            return f"[Pipeline]:{name}_{timestamp}"
        case 'Task':
            return f":{name}-[Sample]:{sample_id}-[Parent_id]:{parent_id}"
        case _:
            return f"[{type}]:{name}-[Sample]:{sample_id}-[Parent_id]:{parent_id}"
    