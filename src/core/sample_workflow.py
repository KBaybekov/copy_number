# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
from typing import Dict, Set
from uuid import UUID
from datetime import datetime

from prefect import flow, task
from prefect.locks import Lock
from prefect.client.orchestration import SyncPrefectClient
from prefect.runtime import flow_run
from prefect.concurrency.asyncio import concurrency

from classes.sample import Sample
from config import AVAILABLE_GPU_IDS, STAGE_DEPENDENCIES, STAGE_CONDITIONS
from modules.logger import get_logger
from modules.dataflow_stages import run_stage

logger = get_logger()

# Глобальный lock и множество занятых GPU ID
gpu_lock = asyncio.Lock()
used_gpu_ids: Set[int] = set()


now = datetime.now()
formatted_now = now.strftime("%d-%m-%Y_%H:%M:%S.%f")

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


@task(name="Run Pipeline Stage", timeout_seconds=3600)
async def run_pipeline_stage(
    sample: Sample,
    stage_name: str,
    gpu_id: int | None = None,
    **kwargs
) -> Sample:
    """
    Запускает одну стадию обработки образца в отдельном потоке.
    """
    logger.info(f"Запуск стадии '{stage_name}' для образца {sample.id}")
    try:
        # Передаём gpu_id как часть kwargs, если доступен
        if gpu_id is not None:
            kwargs["gpu_id"] = gpu_id

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


@flow(
      name="CYP2D6_Sample_Workflow",
      flow_run_name=f"CYP2D6_Sample_Workflow_{formatted_now}",
      retries=0
     )
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

    logger.info(f"Запуск обработки образца {sample.id} через Prefect")

    # Список стадий, которые ещё не начаты
    pending_stages = list(STAGE_DEPENDENCIES.keys())
    running_stages: Dict[str, asyncio.Task[Sample]] = {}

    while all([
               any(pending_stages or running_stages),
               all([sample.success, not sample.finished])
              ]):
        # Проверяем, какие стадии можно запустить
        for stage_name in list(pending_stages):
            sample_suitable_for_stage = STAGE_CONDITIONS.get(stage_name, lambda _: False)
            if sample_suitable_for_stage(sample) and stage_name not in running_stages:
                logger.debug(f"creating subflow for {stage_name}")
                stage_args = STAGE_DEPENDENCIES.get(stage_name, {}).get('args', {})


"""
Вот тут мы подзависли: т.к. лимиты concurr. у нас теперь определяются при деплое, мы должны убрать семафоры. Задача осложняется тем,
что нам нужно разобраться, как запускать нужные сабфлоу. Удачи!
"""

                task = asyncio.create_task(run)

            condition_fn = STAGE_CONDITIONS.get(stage_name, lambda _: False)
            if not condition_fn(sample):
                continue

            if stage_name in running_tasks:
                continue  # уже запущена

            stage_config = STAGE_DEPENDENCIES[stage_name]
            stage_args = stage_config.get("args", {}).copy()
            semaphore = stage_config.get("semaphore")

            # Подготавливаем GPU, если требуется
            gpu_id = None
            needs_gpu = stage_config.get("needs_gpu", False)

            if needs_gpu:
                gpu_id = await acquire_gpu_id()
                if gpu_id is None:
                    logger.debug(f"Нет свободного GPU для стадии {stage_name}, пропуск...")
                    continue

            # Создаём Prefect-задачу с семафором
            task_kwargs = {
                "sample": sample.model_copy(deep=True),
                "stage_name": stage_name,
                **stage_args
            }

            # Обёртываем в семафор, если задан
            if semaphore:
                async with semaphore:
                    task = asyncio.create_task(
                        run_pipeline_stage.submit(  # submit чтобы не блокировать
                            **task_kwargs,
                            gpu_id=gpu_id,
                            wait_for=[]  # можно добавить зависимости
                        )
                    )
            else:
                task = asyncio.create_task(
                    run_pipeline_stage.submit(**task_kwargs, gpu_id=gpu_id)
                )

            running_tasks[stage_name] = task
            pending_stages.remove(stage_name)
            logger.debug(f"Запланирована стадия: {stage_name}")

        # Если ничего не запущено — ждём или выходим
        if not running_tasks:
            await asyncio.sleep(1)
            continue

        # Ждём завершения хотя бы одной задачи
        done, _ = await asyncio.wait(running_tasks.values(), return_when=asyncio.FIRST_COMPLETED)

        for task in done:
            stage_name = next(k for k, v in running_tasks.items() if v == task)
            del running_tasks[stage_name]

            try:
                result_sample: Sample = await task
                sample = result_sample
                logger.info(f"Стадия {stage_name} завершена для {sample.id}")
            except Exception as exc:
                logger.error(f"Стадия {stage_name} упала для {sample.id}: {exc}", exc_info=True)
                sample.success = False
            finally:
                # Освобождаем GPU, если был захвачен
                stage_config = STAGE_DEPENDENCIES.get(stage_name, {})
                if stage_config.get("needs_gpu"):
                    gpu_id = task.get_coro().cr_frame.f_locals.get("gpu_id")
                    if gpu_id is not None:
                        await release_gpu_id(gpu_id)

        # После успешного завершения — можно снова попробовать запустить стадии
        # (например, если циклические или повторные проверки)

    # Финализация
    sample.finished = True
    for task in running_tasks.values():
        task.cancel()

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
