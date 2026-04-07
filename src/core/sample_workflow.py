# -*- coding: utf-8 -*-
from __future__ import annotations

from asyncio import create_task as create_atask
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from random import randint
from typing import Any, Awaitable, Callable, Dict, List, cast, Coroutine, Tuple, TypeAlias
from uuid import UUID
from datetime import datetime

from prefect import flow
from prefect.tasks import Task
from prefect.futures import PrefectFuture
from prefect.artifacts import create_markdown_artifact


from classes.sample import Sample, apply_changes
from config import STAGE_DEPENDENCIES as config_stage_deps
from modules.prefect import collect_from_prefect, get_run_id, submit_to_prefect

# Функция принимает Sample и произвольные именованные аргументы (**kwargs)
ArgFactory: TypeAlias = Callable[..., Awaitable[Dict[str, Dict[str, Any]]]]

now = datetime.now()
formatted_now = now.strftime("%d-%m-%Y_%H:%M:%S.%f")
loop_duration = 10

"""
Идея реализации каналов:
- Sample получает аттрибут task_channels: {stage: [task1_args_dict, task2_args_dict, ...]} (словарь аргументов включает и id будущей таски)
- На этапе инициализации обработки мы наполняем task_channels с помощью специальных функций (каждая для своей стадии)
- затем закидываем sample на обработку, по одной таске за раз
- таска, отработав, кладёт в task_channels словарик с аргументами для таски, которая должна быть запущена после неё
- с помощью as_completed мы меняем Sample с каждым завершившимся заданием
- обновлённый Sample проходит через начальный цикл проверки условий для всех стадий
- таким образом, мы не пропустим инициализацию заданий, не запускаемых другими заданиями (например, использующих объединённый результат выполнения нескольких задач)
"""

def load_callable(spec: str) -> Callable:
    """
    Загружает Callable объект из файла по строке вида "modules/task1.py:task()"
    
    Args:
        spec: Строка с путём к файлу и именем вызываемого объекта.
              Примеры:
                "modules/task1.py:task"
                "utils/helpers.py:process()"
    
    Returns:
        Callable объект (функция, класс или любой объект с __call__)
    
    Raises:
        ValueError: Если формат строки неверный или объект не является callable.
        FileNotFoundError: Если указанный файл не существует.
        ImportError: Если не удалось загрузить модуль.
        AttributeError: Если в модуле нет указанного атрибута.
    """
    # 1. Проверяем валидность переданной строки
    match spec:
        case _ if ":" in spec:
            file_path_str, callable_name = spec.split(':', 1)
            if not callable_name:
                raise ValueError(f"Не указано имя вызываемого объекта в '{spec}'")
            # 2. Удаляем возможные скобки в конце (например "task()" -> "task")
            if callable_name.endswith('()'):
                callable_name = callable_name[:-2]
            # 3. Преобразуем относительный путь в абсолютный (от текущей рабочей директории)
            file_path = Path(file_path_str).resolve()
            if not file_path.exists():
                raise FileNotFoundError(f"Файл не найден: {file_path}")
            # 4. Генерируем уникальное имя модуля (на основе пути)
            module_name = file_path.stem
            # Добавляем суффикс, чтобы избежать конфликтов имён
            unique_name = f"dynamic_{module_name}_{hash(str(file_path))}"
            
            # 5. Загружаем модуль из файла
            spec_loader = spec_from_file_location(unique_name, file_path)
            match spec_loader:
                case None:
                    raise ImportError(f"Не удалось создать спецификацию для '{file_path}'")
                case _:
                    module = module_from_spec(spec_loader)
            match spec_loader.loader:
                case None:
                    raise ImportError(f"Не удалось создать спецификацию для '{file_path}': loader is None")
                case _:
                    try:
                        spec_loader.loader.exec_module(module)
                    except Exception as e:
                        raise ImportError(f"Ошибка при выполнении модуля {file_path}: {e}")
                    # 6. Получаем атрибут
                    if not hasattr(module, callable_name):
                        raise AttributeError(f"Модуль {file_path} не содержит атрибут '{callable_name}'")
                    
                    callable_obj = getattr(module, callable_name)
                    
                    # 7. Проверяем, что объект вызываемый
                    if not callable(callable_obj):
                        raise ValueError(f"Объект '{callable_name}' в {file_path} не является вызываемым")
                    
                    return callable_obj            
        case _:
            raise ValueError(f"Неверный формат: ожидается 'путь:имя', получено '{spec}'")

def dict_non_empty(d:dict) -> bool:
    """
    Проверяет, пуст ли словарь
    """
    return len(d.keys()) > 0

@flow(version="03-2026")
async def _sample_workflow(
                          sample: Sample,
                          config_stage_deps:Dict[str, Dict[str, Any]]
                         ) -> Sample:
    """
    Prefect-поток обработки одного образца.
    Управляет:
      - Зависимостями стадий (STAGE_DEPENDENCIES)
      - Условиями запуска (STAGE_CONDITIONS)
      - Обработкой ошибок и отменой при падении
    """
    print("Initializing logger")
    #logger = await get_logger()
    print("Logger initialized")
    '''
    async def gather_task_statistics(
                               submitted_tasks: Dict[str, PrefectFuture],
                               task_statistics: Dict[str, Any]
                              ) -> Dict[str, Any]:
        """
        Функция для получения статистики по выполнению задач.
        Возвращает список активных задач.
        """
        # 1. Собираем статистику
        for task_id, task in submitted_tasks.items():
            task_stats = task_statistics.get(task_id, {'is_final': False, 'status': ''})
            if task_stats['is_final']:
                continue
            else:
                task_stats['is_final'] = task.state.is_final()
                task_stats['status'] = task.state.name
                task_statistics[task_id] = task_stats
        
        logger.debug(f"Task_statistics: {task_statistics}")
        # словарь неоднороден, поэтому вычленяем только данные заданий
        only_task_stats = {k:v for k,v in task_statistics.items() if k != 'running_stages'}
        finished_tasks = [t for t in only_task_stats.values() if t.get('is_final')]

        # 2. Формируем Markdown текст
        #**Запущенные стадии обработки:** {', '.join(task_statistics['running_stages'])}
        markdown_report = f"""
        ### 📊 Статистика выполнения задач
        **Всего задач:** {len(submitted_tasks)}
        **Завершено (Final):** {len(finished_tasks)}
        **В процессе:** {len(submitted_tasks) - len(finished_tasks)}
        


        | Task ID | Status | Completed |
        | :--- | :--- | :--- |
        {chr(10).join([f"| {task_id} | {v['status']} |{ v['is_final']} |"
                       for task_id, v in task_statistics.items()
                       if task_id != 'running_stages'])}
        """
        # 3. Публикуем артефакт
        await cast(Coroutine[Any, Any, UUID], create_markdown_artifact(
                                 key="task-execution-stats",
                                 markdown=markdown_report,
                                 description="Текущий статус всех запущенных задач"
                                ))
        return task_statistics

'''
    
    # Получение id запуска
    flow_id = await get_run_id()
    match flow_id:
        case "unknown": # Stop
            print("Контекст потока Prefect недоступен!")
            return sample
        case _: # Go
            # Проверяем наличие рабочей и результирующей папок
            match (sample.res_folder, sample.work_folder):
                case (None, _) | (_, None): # Stop
                    print("Не указана рабочая/результирующая папка")
                    return sample
                case (Path(), Path()): # Go
                    print(f"Запуск обработки образца {sample.id} через Prefect")
                    #print(f"STAGE_DEPENDENCIES:\n{STAGE_DEPENDENCIES}")
                    # Список стадий, которые ещё не начаты
                    STAGE_DEPENDENCIES = config_stage_deps.copy()
                    for stage_data in STAGE_DEPENDENCIES.values():
                        stage_data['handler'] = load_callable(stage_data['handler'])
                        stage_data['arg_factory'] = load_callable(stage_data['arg_factory'])
                    stages = list(STAGE_DEPENDENCIES.keys())
                    submitted_tasks: Dict[str, PrefectFuture|Coroutine] = {}
                    active_tasks: Dict[str, PrefectFuture] = {}
                    finished_tasks: List[str] = []
                    task_statistics: Dict[str, Any] = {} # пока не используется

                    print("Entering loop")
                    loop_no = 0
                    while active_tasks or not sample.finished:
                        loop_no += 1
                        print(f"Loop no.{loop_no}")
                        start = datetime.now()
                        #print(f"stage_statuses: {sample.stage_statuses}")
                        # Проверяем, какие стадии можно запустить, коль образец в нормальном состоянии
                        #print("Entering stage loop")
                        match sample.success:
                            case False: # Stop
                                print("Sample.success = False! Завершаем workflow.")
                                break
                            case True: # Go
                                for stage_name in stages:
                                    print(f"Stage: {stage_name}")
                                    stage_data:Dict[str, Any]|None = STAGE_DEPENDENCIES.get(stage_name)
                                    match stage_data:
                                        case None: # Stop
                                            print(f"Отсутствуют данные для стадии обработки: {stage_name}")
                                        case dict() if all(isinstance(k, str) for k in stage_data.keys()): # Go
                                            # Получаем дефолтные аргументы для всех тасок стадии обработки
                                            stage_args_default:dict = stage_data.get('args', {})
                                            prefect_task_params:Dict[str, Any] = stage_data.get('prefect_task_params', {})
                                            prefect_subflow_params:Dict[str, Any] = stage_data.get('prefect_subflow_params', {})
                                            arg_factory: ArgFactory|None = stage_data.get('arg_factory')
                                            match arg_factory:
                                                case None: # Stop
                                                    print(f"Отсутствует функция формирования аргументов для стадии обработки: {stage_name}")
                                                    continue
                                                case _ if callable(arg_factory): # Go
                                                    handler: Coroutine[Any, Any, Tuple[Dict[str, Dict[str, Any]], bool]]|None = stage_data.get('handler') #Task[..., Tuple[Dict[str, Dict[str, Any]], bool]]|None
                                                    match handler:
                                                        case None: # Stop
                                                            print(f"Хэндлер для стадии '{stage_name}' не найден")
                                                            continue
                                                        case _ if isinstance(handler, Task): # Go
                                                            # Формируем пути к папкам стадии
                                                            stage_dirs = [d / stage_name for d in [sample.work_folder, sample.res_folder]]
                                                            stage_args_default.update({'stage_dirs':stage_dirs})
                                                            # Формируем список наборов аргументов
                                                            new_stage_factories:Dict[str, Dict[str, Any]] = await arg_factory(sample, flow_id, **stage_args_default)
                                                            match new_stage_factories:
                                                                case _ if not dict_non_empty(new_stage_factories): # Stop
                                                                    print(f"Каналы для {stage_name} не сформированы")
                                                                    continue
                                                                case _ if len(new_stage_factories.keys()) > 0: # Go
                                                                    # Добавляем сформированные фабрики аргументов в каналы, исключая дублирование
                                                                    #print(f"formed new stage factories: {new_stage_factories}")
                                                                    # Создаём для каждой стадии список, если его ещё не было
                                                                    if stage_name not in sample.task_channels.keys():
                                                                        sample.task_channels[stage_name] = {}
                                                                    for task_name, run_args in new_stage_factories.items():
                                                                        match (
                                                                               task_name not in sample.task_channels[stage_name],
                                                                               task_name not in submitted_tasks
                                                                              ):
                                                                            case (False, _): # Stop
                                                                                print(f"Задание {task_name} было сформировано ранее, не добавляем в список задач стадии")
                                                                            case (_, False): # Stop
                                                                                print(f"Задание {task_name} было запущено ранее, не добавляем в список задач стадии")
                                                                            case (True, True): # Go
                                                                                print(f"Добавление задания {task_name} в очередь на запуск")
                                                                                sample.task_channels[stage_name].update({task_name:run_args})
                                                                    #print(f"sample.task_channels: {sample.task_channels}")
                                                                    # Отправляем задачи на обработку
                                                                    stage_tasks = sample.task_channels[stage_name].copy()
                                                                    for task_name, run_args in stage_tasks.items():
                                                                        # Добавляем к аргументам образец и имя задания
                                                                        run_args.update({'sample':sample})
                                                                        prefect_task_params.update({'task_run_name':task_name})
                                                                        task = submit_to_prefect(
                                                                                                prefect_task_params=prefect_task_params,
                                                                                                prefect_subflow_params=None,
                                                                                                #prefect_subflow_params=prefect_subflow_params,
                                                                                                handler=handler,
                                                                                                run_args=run_args
                                                                                                )
                                                                        
                                                                        # Обновляем списки с заданиями
                                                                        sample.task_channels[stage_name].pop(task_name)
                                                                        if not sample.task_channels[stage_name]:
                                                                            sample.task_channels.pop(stage_name)
                                                                        for task_dict in [submitted_tasks, active_tasks]:
                                                                            task_dict.update({task_name:task})
                                                                    del stage_tasks
                                                                    print(f"active_tasks: {active_tasks.keys()}")
                        match dict_non_empty(active_tasks):
                            # Если ничего не запущено и условий для запуска новых нет — выходим
                            case False:
                                print("Все стадии завершены, активных задач нет. Завершаем workflow.")
                                break
                            case True:
                                # Собираем статистику, выдерживаем паузу до следующего цикла
                                left_time = max(0, (loop_duration - (datetime.now() - start).total_seconds()))
                                #task_statistics = await gather_task_statistics(submitted_tasks, task_statistics)
                                
                                # Ждем завершения любой из запущенных задач
                                just_finished_tasks: List[str] = []
                                completed_tasks = await collect_from_prefect(active_tasks, left_time)
                                match dict_non_empty(completed_tasks):
                                    case False:
                                        print(f"Ни одна задача не завершилась за отведенное время [{left_time.__round__(2)} sec.]")
                                    case True:
                                        for task_name, task_result in completed_tasks.items():
                                            #changes, is_processing_ok = task_result
                                            if isinstance(task_result, Exception):
                                                print(f"Task {task_name} failed with exception: {task_result}")
                                                changes = {}
                                                is_processing_ok = False
                                            else:
                                                changes, is_processing_ok = task_result
                                            print(f"Task: {task_name}\nChanges: {changes}\nProcessing successful: {is_processing_ok}")
                                            # Обновляем основной Sample
                                            await apply_changes(sample, changes)
                                            match is_processing_ok:
                                                case False:
                                                    sample.task_statuses[task_name] = "FAIL"
                                                    sample.success = False
                                                case True:
                                                    sample.task_statuses[task_name] = "OK"
                                            # Обновляем списки заданий
                                            finished_tasks.append(task_name)
                                            just_finished_tasks.append(task_name)
                                        for task in just_finished_tasks:
                                            active_tasks.pop(task)
                    # Финализация
                    sample.finished = True
                    create_atask(sample.log_sample_data(
                                                        stage_name="Main_flow",
                                                        sample_ok=sample.success,
                                                        fail_reason="End of processing"
                                                       ))

                    if sample.success:
                        print(f"Образец {sample.id} успешно обработан.")
                    else:
                        print(f"Образец {sample.id} завершился с ошибкой.")
                    return sample


@flow(version="03-2026")
async def sample_workflow(
                          sample: Sample,

                         ) -> Sample:
    """
    Prefect-поток обработки одного образца.
    Управляет:
      - Зависимостями стадий (STAGE_DEPENDENCIES)
      - Условиями запуска (STAGE_CONDITIONS)
      - Обработкой ошибок и отменой при падении
    """
    from modules.logger import get_logger

    print("Initializing logger")
    logger = await get_logger()
    logger.info("Logger initialized")
    '''
    async def gather_task_statistics(
                               submitted_tasks: Dict[str, PrefectFuture],
                               task_statistics: Dict[str, Any]
                              ) -> Dict[str, Any]:
        """
        Функция для получения статистики по выполнению задач.
        Возвращает список активных задач.
        """
        # 1. Собираем статистику
        for task_id, task in submitted_tasks.items():
            task_stats = task_statistics.get(task_id, {'is_final': False, 'status': ''})
            if task_stats['is_final']:
                continue
            else:
                task_stats['is_final'] = task.state.is_final()
                task_stats['status'] = task.state.name
                task_statistics[task_id] = task_stats
        
        logger.debug(f"Task_statistics: {task_statistics}")
        # словарь неоднороден, поэтому вычленяем только данные заданий
        only_task_stats = {k:v for k,v in task_statistics.items() if k != 'running_stages'}
        finished_tasks = [t for t in only_task_stats.values() if t.get('is_final')]

        # 2. Формируем Markdown текст
        #**Запущенные стадии обработки:** {', '.join(task_statistics['running_stages'])}
        markdown_report = f"""
        ### 📊 Статистика выполнения задач
        **Всего задач:** {len(submitted_tasks)}
        **Завершено (Final):** {len(finished_tasks)}
        **В процессе:** {len(submitted_tasks) - len(finished_tasks)}
        


        | Task ID | Status | Completed |
        | :--- | :--- | :--- |
        {chr(10).join([f"| {task_id} | {v['status']} |{ v['is_final']} |"
                       for task_id, v in task_statistics.items()
                       if task_id != 'running_stages'])}
        """
        # 3. Публикуем артефакт
        await cast(Coroutine[Any, Any, UUID], create_markdown_artifact(
                                 key="task-execution-stats",
                                 markdown=markdown_report,
                                 description="Текущий статус всех запущенных задач"
                                ))
        return task_statistics

'''
    
    # Получение id запуска
    flow_id = await get_run_id()
    match flow_id:
        case "unknown": # Stop
            logger.error("Контекст потока Prefect недоступен!")
            return sample
        case _: # Go
            # Проверяем наличие рабочей и результирующей папок
            match (sample.res_folder, sample.work_folder):
                case (None, _) | (_, None): # Stop
                    logger.error("Не указана рабочая/результирующая папка")
                    return sample
                case (Path(), Path()): # Go
                    logger.info(f"Запуск обработки образца {sample.id} через Prefect")
                    #print(f"STAGE_DEPENDENCIES:\n{STAGE_DEPENDENCIES}")
                    # Список стадий, которые ещё не начаты
                    STAGE_DEPENDENCIES = config_stage_deps.copy()
                    for stage_data in STAGE_DEPENDENCIES.values():
                        stage_data['handler'] = load_callable(stage_data['handler'])
                        stage_data['arg_factory'] = load_callable(stage_data['arg_factory'])
                    stages = list(STAGE_DEPENDENCIES.keys())
                    submitted_tasks: Dict[str, PrefectFuture|Coroutine] = {}
                    active_tasks: Dict[str, PrefectFuture] = {}
                    finished_tasks: List[str] = []
                    task_statistics: Dict[str, Any] = {} # пока не используется

                    print("Entering loop")
                    loop_no = 0
                    while active_tasks or not sample.finished:
                        loop_no += 1
                        print(f"Loop no.{loop_no}")
                        start = datetime.now()
                        #print(f"stage_statuses: {sample.stage_statuses}")
                        # Проверяем, какие стадии можно запустить, коль образец в нормальном состоянии
                        #print("Entering stage loop")
                        match sample.success:
                            case False: # Stop
                                logger.error("Sample.success = False! Завершаем workflow.")
                                break
                            case True: # Go
                                for stage_name in stages:
                                    print(f"Stage: {stage_name}")
                                    stage_data:Dict[str, Any]|None = STAGE_DEPENDENCIES.get(stage_name)
                                    match stage_data:
                                        case None: # Stop
                                            logger.error(f"Отсутствуют данные для стадии обработки: {stage_name}")
                                        case dict() if all(isinstance(k, str) for k in stage_data.keys()): # Go
                                            # Получаем дефолтные аргументы для всех тасок стадии обработки
                                            stage_args_default:dict = stage_data.get('args', {})
                                            prefect_task_params:Dict[str, Any] = stage_data.get('prefect_task_params', {})
                                            prefect_subflow_params:Dict[str, Any] = stage_data.get('prefect_subflow_params', {})
                                            arg_factory: ArgFactory|None = stage_data.get('arg_factory')
                                            match arg_factory:
                                                case None: # Stop
                                                    logger.error(f"Отсутствует функция формирования аргументов для стадии обработки: {stage_name}")
                                                    continue
                                                case _ if callable(arg_factory): # Go
                                                    handler: Coroutine[Any, Any, Tuple[Dict[str, Dict[str, Any]], bool]]|None = stage_data.get('handler') #Task[..., Tuple[Dict[str, Dict[str, Any]], bool]]|None
                                                    match handler:
                                                        case None: # Stop
                                                            logger.error(f"Хэндлер для стадии '{stage_name}' не найден")
                                                            continue
                                                        case _ if isinstance(handler, Task): # Go
                                                            # Формируем пути к папкам стадии
                                                            stage_dirs = [d / stage_name for d in [sample.work_folder, sample.res_folder]]
                                                            stage_args_default.update({'stage_dirs':stage_dirs})
                                                            # Формируем список наборов аргументов
                                                            new_stage_factories:Dict[str, Dict[str, Any]] = await arg_factory(sample, flow_id, **stage_args_default)
                                                            match new_stage_factories:
                                                                case _ if not dict_non_empty(new_stage_factories): # Stop
                                                                    logger.info(f"Каналы для {stage_name} не сформированы")
                                                                    continue
                                                                case _ if len(new_stage_factories.keys()) > 0: # Go
                                                                    # Добавляем сформированные фабрики аргументов в каналы, исключая дублирование
                                                                    #print(f"formed new stage factories: {new_stage_factories}")
                                                                    # Создаём для каждой стадии список, если его ещё не было
                                                                    if stage_name not in sample.task_channels.keys():
                                                                        sample.task_channels[stage_name] = {}
                                                                    for task_name, run_args in new_stage_factories.items():
                                                                        match (
                                                                               task_name not in sample.task_channels[stage_name],
                                                                               task_name not in submitted_tasks
                                                                              ):
                                                                            case (False, _): # Stop
                                                                                logger.info(f"Задание {task_name} было сформировано ранее, не добавляем в список задач стадии")
                                                                            case (_, False): # Stop
                                                                                logger.info(f"Задание {task_name} было запущено ранее, не добавляем в список задач стадии")
                                                                            case (True, True): # Go
                                                                                logger.info(f"Добавление задания {task_name} в очередь на запуск")
                                                                                sample.task_channels[stage_name].update({task_name:run_args})
                                                                    #print(f"sample.task_channels: {sample.task_channels}")
                                                                    # Отправляем задачи на обработку
                                                                    stage_tasks = sample.task_channels[stage_name].copy()
                                                                    for task_name, run_args in stage_tasks.items():
                                                                        # Добавляем к аргументам образец и имя задания
                                                                        run_args.update({'sample':sample})
                                                                        prefect_task_params.update({'task_run_name':task_name})
                                                                        task = submit_to_prefect(
                                                                                                prefect_task_params=prefect_task_params,
                                                                                                prefect_subflow_params=None,
                                                                                                #prefect_subflow_params=prefect_subflow_params,
                                                                                                handler=handler,
                                                                                                run_args=run_args
                                                                                                )
                                                                        
                                                                        # Обновляем списки с заданиями
                                                                        sample.task_channels[stage_name].pop(task_name)
                                                                        if not sample.task_channels[stage_name]:
                                                                            sample.task_channels.pop(stage_name)
                                                                        for task_dict in [submitted_tasks, active_tasks]:
                                                                            task_dict.update({task_name:task})
                                                                    del stage_tasks
                                                                    print(f"active_tasks: {active_tasks.keys()}")
                        match dict_non_empty(active_tasks):
                            # Если ничего не запущено и условий для запуска новых нет — выходим
                            case False:
                                logger.info("Все стадии завершены, активных задач нет. Завершаем workflow.")
                                break
                            case True:
                                # Собираем статистику, выдерживаем паузу до следующего цикла
                                left_time = max(0, (loop_duration - (datetime.now() - start).total_seconds()))
                                #task_statistics = await gather_task_statistics(submitted_tasks, task_statistics)
                                
                                # Ждем завершения любой из запущенных задач
                                just_finished_tasks: List[str] = []
                                completed_tasks = await collect_from_prefect(active_tasks, left_time)
                                match dict_non_empty(completed_tasks):
                                    case False:
                                        logger.debug(f"Ни одна задача не завершилась за отведенное время [{left_time.__round__(2)} sec.]")
                                    case True:
                                        for task_name, task_result in completed_tasks.items():
                                            #changes, is_processing_ok = task_result
                                            if isinstance(task_result, Exception):
                                                logger.error(f"Task {task_name} failed with exception: {task_result}")
                                                changes = {}
                                                is_processing_ok = False
                                            else:
                                                changes, is_processing_ok = task_result
                                            print(f"Task: {task_name}\nChanges: {changes}\nProcessing successful: {is_processing_ok}")
                                            # Обновляем основной Sample
                                            await apply_changes(sample, changes)
                                            match is_processing_ok:
                                                case False:
                                                    sample.task_statuses[task_name] = "FAIL"
                                                    sample.success = False
                                                case True:
                                                    sample.task_statuses[task_name] = "OK"
                                            # Обновляем списки заданий
                                            finished_tasks.append(task_name)
                                            just_finished_tasks.append(task_name)
                                        for task in just_finished_tasks:
                                            active_tasks.pop(task)
                    # Финализация
                    sample.finished = True
                    create_atask(sample.log_sample_data(
                                                        stage_name="Main_flow",
                                                        sample_ok=sample.success,
                                                        fail_reason="End of processing"
                                                       ))

                    if sample.success:
                        logger.info(f"Образец {sample.id} успешно обработан.")
                    else:
                        logger.warning(f"Образец {sample.id} завершился с ошибкой.")
                    return sample
