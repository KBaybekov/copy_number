# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
from typing import Any, Dict, List, cast, Coroutine
from uuid import UUID
from datetime import datetime

from prefect import flow, task
from prefect.futures import PrefectFuture, as_completed
from prefect.runtime import flow_run
from prefect.artifacts import create_markdown_artifact

from classes.sample import Sample
from config import STAGE_DEPENDENCIES, STAGE_CONDITIONS
from modules.logger import get_logger
from modules.dataflow_stages import run_stage

logger = get_logger()

now = datetime.now()
formatted_now = now.strftime("%d-%m-%Y_%H:%M:%S.%f")
loop_duration = 5


@task(name="Run Pipeline Stage", timeout_seconds=3600)
async def run_pipeline_stage(
    sample: Sample,
    stage_name: str,
    **kwargs
) -> Sample:
    """
    Запускает одну стадию обработки образца в отдельном потоке.
    """
    logger.info(f"Запуск стадии '{stage_name}' для образца {sample.id}")
    try:

        # Запускаем синхронную функцию в потоке
        updated_sample = await asyncio.to_thread(
            run_stage,
            sample=sample,
            stage_name=stage_name,
            **kwargs
        )

        if updated_sample.success:
            logger.info(f"Стадия '{stage_name}' успешно завершена для {sample.id}")
        else:
            logger.warning(f"Стадия '{stage_name}' провалилась для {sample.id}")

        return updated_sample

    except Exception as e:
        logger.error(f"Ошибка при выполнении стадии '{stage_name}' для {sample.id}: {e}", exc_info=True)
        sample.fail(stage_name=stage_name, reason=str(e))
        return sample

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

    async def gather_task_statistics(
                               submitted_tasks: List[PrefectFuture],
                               task_statistics: Dict[str, Any]
                              ) -> Dict[str, Any]:
        """
        Функция для получения статистики по выполнению задач.
        Возвращает список активных задач.
        """
        # 1. Собираем статистику
        for task in submitted_tasks:
            task_id = task.task_run_id.__str__()
            task_stats = task_statistics.get(task_id, {'is_final': False, 'status': ''})
            if task_stats['is_final']:
                continue
            else:
                task_stats['is_final'] = task.state.is_final()
                task_stats['status'] = task.state.name
                task_statistics[task_id] = task_stats

        finished_tasks = [t for t in task_statistics.values() if t['is_final']]

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
        

    logger.info(f"Запуск обработки образца {sample.id} через Prefect")

    # Список стадий, которые ещё не начаты
    stages = list(STAGE_DEPENDENCIES.keys())
    submitted_tasks: List[PrefectFuture] = []
    active_tasks: List[PrefectFuture] = []
    finished_tasks: List[str] = []
    task_statistics: Dict[str, Any] = {'running_stages':[]}
    # Запущенные стадии и id задач
    running_stage_tasks: Dict[str, str] = {}

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
            sample_suitable_for_stage = STAGE_CONDITIONS.get(stage_name, lambda _: False)
            # Образец должен отвечать критериям, а стадия ранее не была запущена
            if all([
                    sample_suitable_for_stage(sample),
                    stage_name not in running_stage_tasks
                   ]):
                logger.debug(f"creating task for {stage_name}")
                stage_data = STAGE_DEPENDENCIES.get(stage_name)
                # Задание запускаем, если прописаны условия для этой стадии
                if stage_data is not None:
                    # Получаем аргументы для самого задания обработки
                    stage_args = stage_data.get('args', {})
                    # Получаем параметры для таски Prefect
                    prefect_task_args = stage_data.get('prefect_task_args', {})
                    task = run_pipeline_stage.with_options(
                                                           task_run_name=f"[Task] {stage_name} [{sample.id}]",
                                                           tags=flow_params.get('tags', []),
                                                           **prefect_task_args                                                        
                                                          ).submit(
                                                                   sample=sample,
                                                                   stage_name=stage_name,
                                                                   **stage_args
                                                                  )
                    # Добавляем задание в список отправленных на выполнение
                    submitted_tasks.append(task)
                    active_tasks.append(task)
                    # Добавляем стадию в список запущенных
                    running_stage_tasks[stage_name] = task.task_run_id.__str__()
                    task_statistics['running_stages'].append(stage_name)
                else:
                    logger.error(f"Указана неверная стадия обработки: {stage_name}")
        
        if not running_stage_tasks:
            # Если ничего не запущено и условий для запуска новых нет — выходим
            sample.finished = True
            break

        left_time = max(0, (loop_duration - (datetime.now() - start).total_seconds()))
        # Собираем статистику, выдерживаем паузу до следующего цикла
        task_statistics = await gather_task_statistics(submitted_tasks, task_statistics)
        
        # 3. Ждем завершения любой из запущенных стадий
        #done, _ = as_completed(active_tasks, timeout=left_time)
        try:
            just_finished_tasks: List[PrefectFuture] = []
            for task in as_completed(active_tasks, timeout=left_time):
                sample = await task.result()
                # Обновляем списки заданий
                task_id = task.task_run_id.__str__()
                finished_tasks.append(task_id)
                just_finished_tasks.append(task)
                # Удаляем из списка активных стадий ту, задание которой завершено
                stage_name = next(t for t in running_stage_tasks.keys() if running_stage_tasks[t] == task_id)
                running_stage_tasks.pop(stage_name)
                task_statistics['running_stages'].remove(stage_name)
            for task in just_finished_tasks:
                active_tasks.remove(task)

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

'''
# Глобальный lock и множество занятых GPU ID
gpu_lock = asyncio.Lock()
used_gpu_ids: Set[int] = set()

@task(name="Acquire GPU ID", retries=3, retry_delay_seconds=5)
async def acquire_gpu_id() -> int | None:
    """Захватывает свободный GPU ID для выполнения задачи."""
    async with gpu_lock:
        for gpu_id in AVAILABLE_GPU_IDS:
            if gpu_id not in used_gpu_ids:
                used_gpu_ids.add(gpu_id)
                logger.info(f"Выделен GPU ID: {gpu_id}")
                return gpu_id
    return None


@task(name="Release GPU ID")
async def release_gpu_id(gpu_id: int):
    """Освобождает GPU ID после завершения задачи."""
    async with gpu_lock:
        used_gpu_ids.discard(gpu_id)
    logger.info(f"GPU ID освобождён: {gpu_id}")
'''