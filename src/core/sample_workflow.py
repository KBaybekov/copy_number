# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Callable, Dict, List, cast, Coroutine, Tuple, TypeAlias
from uuid import UUID
from datetime import datetime

from prefect import flow
from prefect.tasks import Task
from prefect.futures import PrefectFuture, as_completed
from prefect.runtime import flow_run
from prefect.artifacts import create_markdown_artifact

from classes.sample import Sample, apply_changes
from config import STAGE_DEPENDENCIES
from modules.logger import get_logger

# Функция принимает Sample и произвольные именованные аргументы (**kwargs)
ArgFactory: TypeAlias = Callable[..., Dict[str, Dict[str, Any]]]

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

@flow
async def sample_workflow(
                          sample: Sample
                         ) -> Sample:
    """
    Prefect-поток обработки одного образца.
    Управляет:
      - Зависимостями стадий (STAGE_DEPENDENCIES)
      - Условиями запуска (STAGE_CONDITIONS)
      - Обработкой ошибок и отменой при падении
    """
    logger = get_logger()

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
        markdown_report = f"""
        ### 📊 Статистика выполнения задач
        **Всего задач:** {len(submitted_tasks)}
        **Завершено (Final):** {len(finished_tasks)}
        **В процессе:** {len(submitted_tasks) - len(finished_tasks)}
        **Запущенные стадии обработки:** {', '.join(task_statistics['running_stages'])}


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

    # Получение параметров запуска
    flow_params = flow_run.get_parameters()
    if not flow_params:
        logger.error("Контекст потока Prefect недоступен!")
        return sample
    
    # Проверяем наличие рабочей и результирующей папок
    if any([sample.res_folder is None, sample.work_folder is None]):
        logger.error("Не указана рабочая/результирующая папка")
        return sample

    logger.info(f"Запуск обработки образца {sample.id} через Prefect")

    # Список стадий, которые ещё не начаты
    stages = list(STAGE_DEPENDENCIES.keys())
    submitted_tasks: Dict[str, PrefectFuture] = {}
    active_tasks: Dict[str, PrefectFuture] = {}
    finished_tasks: List[str] = []
    task_statistics: Dict[str, Any] = {'running_stages':[]}
    # Запущенные стадии и id задач
    running_stage_tasks: Dict[str, List[str]] = {}

    while all([
               sample.success,
               any([
                    active_tasks,
                    not sample.finished
                   ])
              ]):
        start = datetime.now()
        # Проверяем, какие стадии можно запустить
        for stage_name in stages:
            stage_data = STAGE_DEPENDENCIES.get(stage_name)
            if stage_data is None:
                logger.error(f"Отсутствуют данные для стадии обработки: {stage_name}")
            else:
                prefect_stage_args = stage_data.get('prefect_task_args', {})
                handler: Task[..., Tuple[Dict[str, Dict[str, Any]], bool]] = STAGE_DEPENDENCIES.get(stage_name, {}).get('handler') # type: ignore
                if handler is None:
                    logger.error(f"Хэндлер для стадии '{stage_name}' не найден")
                arg_factory: ArgFactory = stage_data.get('arg_factory') # type: ignore
                if arg_factory is None:
                    logger.error(f"Отсутствует функция формирования аргументов для стадии обработки: {stage_name}")
                else:
                    # Создаём для каждой стадии список, если его ещё не было
                    if stage_name not in sample.task_channels.keys():
                        sample.task_channels[stage_name] = {}
                    # Получаем дефолтные аргументы для всех тасок стадии обработки
                    stage_args_default:dict = stage_data.get('args', {})
                    # Формируем путь к папкам стадии
                    stage_dirs = [d / stage_name for d in [sample.work_folder, sample.res_folder]] # type: ignore
                    stage_args_default.update({'stage_dirs':stage_dirs})
                    # Формируем список наборов аргументов
                    new_stage_factories:Dict[str, Dict[str, Any]] = arg_factory(sample, **stage_args_default)
                    # Добавляем сформированные фабрики аргументов в каналы, исключая дублирование
                    for task_name, args in new_stage_factories.items():
                        if task_name not in sample.task_channels[stage_name]:
                            sample.task_channels[stage_name].update({task_name:args})

                    # Отправляем задачи на обработку
                    for task_name, args in sample.task_channels[stage_name].items():
                        task = handler.with_options(
                                                    task_run_name=f"[Task] {task_name}",
                                                    **prefect_stage_args
                                                   ).submit(sample=sample, **args)
                        # Обновляем списки с заданиями
                        for task_dict in [submitted_tasks, active_tasks]:
                            task_dict.update({task_name:task})
                        if stage_name not in task_statistics['running_stages']:
                            task_statistics['running_stages'].append(stage_name)
                        if stage_name not in running_stage_tasks.keys():
                            running_stage_tasks[stage_name] = [task_name]
                        else:
                            running_stage_tasks[stage_name].append(task_name)
        
        if not running_stage_tasks:
            # Если ничего не запущено и условий для запуска новых нет — выходим
            logger.info("Все стадии завершены, активных задач нет. Завершаем workflow.")
            sample.finished = True
            break

        left_time = max(0, (loop_duration - (datetime.now() - start).total_seconds()))
        # Собираем статистику, выдерживаем паузу до следующего цикла
        task_statistics = await gather_task_statistics(submitted_tasks, task_statistics)
        
        # Ждем завершения любой из запущенных стадий
        try:
            just_finished_tasks: List[str] = []
            async for task in as_completed(list(active_tasks.values()), timeout=left_time):
                task_name = next((k for k in active_tasks.keys() if active_tasks[k]==task), 'unknown')
                # Получаем обновлённую копию Sample
                changes, is_processing_ok = await task.result()
                # Обновляем основной Sample
                apply_changes(sample, changes)
                sample.task_statuses[task_name] = "OK" if is_processing_ok else "FAIL"
                # Обновляем списки заданий
                finished_tasks.append(task_name)
                just_finished_tasks.append(task_name)
                # Удаляем из списка активных стадий ту, задание которой завершено
                stage_name = next(
                                  t for t in running_stage_tasks.keys()
                                  if task_name in running_stage_tasks[t]
                                 )
                running_stage_tasks[stage_name].remove(task_name)
                if not running_stage_tasks[stage_name]:
                    running_stage_tasks.pop(stage_name)
                    task_statistics['running_stages'].remove(stage_name)
            for task in just_finished_tasks:
                active_tasks.pop(task)

        except TimeoutError:
            logger.debug(f"Ни одна задача не завершилась за отведенное время [{left_time.__round__(2)} sec.]")

    # Финализация
    sample.finished = True
    

    sample.log_sample_data(
        stage_name="Main_flow",
        sample_ok=sample.success,
        fail_reason="End of processing"
    )

    if sample.success:
        logger.info(f"Образец {sample.id} успешно обработан.")
    else:
        logger.warning(f"Образец {sample.id} завершился с ошибкой.")

    return sample
