# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, List, cast, Coroutine, Tuple
from uuid import UUID
from datetime import datetime

from prefect import flow, task
from prefect.futures import PrefectFuture, as_completed
from prefect.runtime import flow_run
from prefect.artifacts import create_markdown_artifact
from prefect_shell import ShellOperation

from classes.sample import Sample
from config import STAGE_DEPENDENCIES, STAGE_CONDITIONS
from modules.logger import get_logger
from modules.dataflow_stages import run_stage

logger = get_logger()

now = datetime.now()
formatted_now = now.strftime("%d-%m-%Y_%H:%M:%S.%f")
loop_duration = 5


@task
async def _run_pipeline_stage(
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
    
@task
async def run_pipeline_stage(
    sample: Sample,
    stage_name: str,
    **kwargs
) -> Sample:
    """
    Запускает одну стадию обработки образца
    """
    try:
        logger.info(f"Запуск стадии '{stage_name}' для образца {sample.id}")
        stage_status = "FAIL"
        processing_succesful = False
        # Загружаем функцию обработки
        handler: Callable[..., Tuple[Sample, bool]] | None = STAGE_DEPENDENCIES.get(stage_name, {}).get('handler')
        if handler is None:
            logger.error(f"Handler for stage '{stage_name}' not found in STAGE_DEPENDENCIES. Skipping.")
        else:
            # Загружаем функцию проверки валидности образца
            sample_checker = STAGE_CONDITIONS.get(stage_name, lambda _: False)
            while sample.success:
                if sample_checker(sample):
                    # проверяем, указаны ли основные рабочая и результирующая папки
                    if all([
                            sample.res_folder is not None,
                            sample.work_folder is not None
                        ]):
                        # Создание папок для стадии обработки
                        stage_dirs = []
                        for d in [sample.work_folder, sample.res_folder]:
                            f = d / stage_name # type: ignore
                            f.mkdir(parents=True, exist_ok=True)
                            stage_dirs.append(f)
                    else:
                        sample.fail(
                                    stage_name=stage_name,
                                    reason="Not set: res_folder/work_folder"
                                )
                        break
                    # Все начальные проверки пройдены, запускаем логику для этой стадии
                    sample, processing_succesful = await handler(
                                                    sample=sample,
                                                    stage_dirs=stage_dirs,
                                                    **kwargs
                                                    )
                # sample_checker покажет False, если проверка не пройдена 
                # (либо образец не ок, либо обработка завершена и образец больше в ней не нуждается)
                else:
                    logger.info(f"Sample {sample.id} doesn't meet conditions for {stage_name} [anymore]")
                    break
            else:
                logger.error(f'Sample {sample.id} is broken during "{stage_name}"')

            if processing_succesful:
                stage_status = "OK"
        sample.stage_statuses[stage_name] = stage_status
        return sample

    except Exception as e:
        logger.error(f"Ошибка при выполнении стадии '{stage_name}' для {sample.id}: {e}", exc_info=True)
        sample.fail(stage_name=stage_name, reason=str(e))
        return sample


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

@task
async def run_shell_stage(sample: Sample, stage_name: str, cmd_template: str) -> Sample:
    """
    Заменяет: stage_wrapper, run_stage, form_cmd и run_subprocess_with_stop.
    """
    # 1. Подготовка путей (бывший stage_wrapper)
    work_dir = sample.work_folder / stage_name
    work_dir.mkdir(parents=True, exist_ok=True)

    # 2. Запуск через ShellOperation
    # Мы передаем переменные из sample.to_dict() напрямую в шаблоны {{ }}
    try:
        async with ShellOperation(
            commands=[cmd_template],
            working_dir=work_dir,
            env={"SAMPLE_ID": sample.id}, # Можно прокинуть доп. переменные
        ).lo as shell_batch:
            # Запускаем и стримим логи в Prefect UI
            process = await shell_batch.trigger()
            await process.await_for_completion()
            
            # Получаем результат (выбросит исключение при exit_code != 0)
            result = process.return_code
            
        # 3. Фиксация успеха
        with sample._lock:
            sample.stage_statuses[stage_name] = "OK"
            
    except Exception as e:
        # Если команда упала или была отменена, Prefect поймает это здесь
        logger.error(f"Stage {stage_name} failed: {e}")
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