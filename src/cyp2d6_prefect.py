from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Optional, cast, Any, Coroutine
from uuid import UUID

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact

# Импорт кастомных модулей
from file_format_handlers.excel_handler import process_input_data
from modules.logger import get_logger, get_log_file_path
from classes.sample import Sample

# Инициализируем локальный CSV-логгер
logger = get_logger("CYP2D6_MASTER")
now = datetime.now()
formatted_now = now.strftime("%d-%m-%Y_%H:%M:%S")

# Ссылка на Этаж 3 (опишем его следующим шагом)
# from core.sample_lifecycle import sample_workflow 
import random

@flow(name="Sample-Lifecycle-Mock")
async def sample_workflow(sample: Sample) -> Sample:
    """
    Временная заглушка для тестирования Первого и Второго этажей.
    Имитирует задержку и случайную ошибку.
    """
    logger.info(f"Тестовая обработка образца {sample.id}...")
    
    # Имитируем долгую работу (например, проверку файлов)
    await asyncio.sleep(random.uniform(1, 3))
    
    # Имитируем системную ошибку кода на 5% образцов для проверки устойчивости
    if random.random() < 0.05:
        logger.error(f"Критический сбой Python в потоке образца {sample.id}!")
        raise RuntimeError(f"Системная ошибка при обработке {sample.id}")

    # Имитируем штатный FAIL логики биоинформатики (через ваш метод fail)
    if random.random() < 0.1:
        sample.fail(stage_name="Initial_Check", reason="Имитация плохого качества данных")
    else:
        sample.success = True
        logger.info(f"Образец {sample.id} успешно прошел тестовую стадию.")
        
    return sample


@flow(
      name="CYP2D6-Main-Pipeline",
      flow_run_name=f"CYP2D6-Main_{formatted_now}",
      description="Основной конвейер обработки CYP2D6. Загружает таблицу и запускает жизненный цикл для каждого сэмпла.",
      version="1.0.0",
      retries=0,
      persist_result=True,
      #result_serializer=,
      #result_storage=,
      timeout_seconds=None,
      log_prints=True,
      #on_completion: list[FlowStateHook[..., Any]] | None = None,
      #on_failure: list[FlowStateHook[..., Any]] | None = None,
      #on_cancellation: list[FlowStateHook[..., Any]] | None = None,
      #on_crashed: list[FlowStateHook[..., Any]] | None = None,
      #on_running: list[FlowStateHook[..., Any]] | None = None
     )
async def main_pipeline(
                        table_input: str, 
                        sample_data_csv: Optional[str] = None
                       ) -> None:
    """
    Точка входа в систему (Этаж 1-2). 
    
    :param table_input: Путь к исходной таблице Excel с метаданными.
    :param sample_data_csv: Опциональный путь к CSV результатам предыдущих запусков.
    """

    logger.info(f"Запуск пайплайна. Таблица: {table_input}")

    # 1. Загрузка данных (Ваша логика из excel_handler)
    # Превращаем строковые пути из CLI в Path объекты для вашего парсера
    input_path = Path(table_input)
    results_path = Path(sample_data_csv) if sample_data_csv else None
    
    # Инициализация списка объектов Sample
    samples: List[Sample] = process_input_data((input_path, results_path))
    
    if not samples:
        logger.warning("Список образцов пуст. Завершение работы.")
        return

    # Создаем краткий отчет (Artifact) в UI о начале работы
    await cast(Coroutine[Any, Any, UUID], create_markdown_artifact(
                                                                   key="run-summary",
                                                                   markdown=(
                                                                             "## Сводка запуска\n"
                                                                             f"- **Количество образцов:** `{len(samples)}`\n"
                                                                             f"- **Таблица:** `{input_path.name}`\n"
                                                                             f"- **Лог на диске:** `{get_log_file_path().as_posix()}`"
                                                                            ),
                                                                   description="Параметры текущего прогона"
                                                                  ))

    # Порождение независимых потоков (Subflows) для каждого сэмпла   
    logger.info(f"Инициализация асинхронных потоков для {len(samples)} образцов...")
    tasks: List[Coroutine[Any, Any, Sample]] = [sample_workflow(s) for s in samples if not s.finished]
    results: List[Sample | BaseException] = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Анализ итогов пачки
    success_count = sum(1 for r in results if isinstance(r, Sample) and r.success)
    error_count = len(results) - success_count
    logger.info(f"Из {len(results)} образцов {success_count} успешны, {error_count} - нет")
