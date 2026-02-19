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
from modules.logger import setup_custom_logger
from classes.sample import Sample

# Создаём логгер на основе
logger = setup_custom_logger()

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
                        results_dir: str,
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
    sample_data_path = Path(sample_data_csv) if sample_data_csv else None
    status_str = ""
    if sample_data_path:
        status_str = f"- **Таблица с данными обработки образцов:** `{sample_data_path.name}`"
    
    # Инициализация списка объектов Sample
    samples: List[Sample] = process_input_data((input_path, sample_data_path))
    
    if not samples:
        logger.warning("Список образцов пуст. Завершение работы.")
        return

    # Создаем краткий отчет (Artifact) в UI о начале работы
    await cast(Coroutine[Any, Any, UUID], create_markdown_artifact(
                                                                   key="run-summary",
                                                                   markdown=(
                                                                             "## Сводка запуска\n"
                                                                             f"- **Количество образцов:** `{len(samples)}`\n"
                                                                             f"- **Количество образцов, готовых к дальнейшей обработке:** `{len([s for s in samples if not s.finished])}`\n"
                                                                             f"- **Таблица с исходными данными:** `{input_path.name}`\n"
                                                                             f"{status_str} \n"
                                                                             f"- **Лог на диске:** `{get_log_file_path().as_posix()}`"
                                                                            ),
                                                                   description="Параметры запуска"
                                                                  ))

    # Порождение независимых потоков (Subflows) для каждого сэмпла   
    logger.info(f"Инициализация асинхронных потоков для {len(samples)} образцов...")
    tasks: List[Coroutine[Any, Any, Sample]] = [sample_workflow(s) for s in samples if not s.finished]
    results: List[Sample | BaseException] = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Анализ итогов пачки
    success_count = sum(1 for r in results if isinstance(r, Sample) and r.success)
    error_count = len(results) - success_count
    logger.info(f"Из {len(results)} образцов {success_count} успешны, {error_count} - нет")

"""
LOGGER!!!
Чтобы реализовать схему «сетап один раз — пишут все», без гонки за файл из разных контейнеров и проблем с памятью, нужно использовать нативную систему логгирования Prefect.
Решение:
Мы делаем Prefect UI центральным узлом. Все этажи (1, 2, 3, 4) пишут логи стандартным get_logger(), они стекаются в базу Prefect. А единственный процесс, который имеет доступ к CSV-файлу на диске — это Этаж 1 (Main Flow). Он будет «слушать» поток логов всех своих сабфлоу и записывать их в файл.
Однако, есть более элегантный путь в рамках вашей архитектуры:
На этажах 2, 3, 4: Мы используем только PrefectLogHandler и StreamHandler. Они не требуют пути к файлу и работают в любом контейнере «из коробки».
На этаже 1: Мы добавляем FileHandler к логгеру prefect.flow_runs. В Prefect логи дочерних флоу по умолчанию всплывают к родителю.
Модифицируем ваш modules/logger.py:
python
def get_logger(name: str):
    logger = getLogger(name)
    if not logger.handlers:
        logger.setLevel(DEBUG)
        
        # 1. Файловый хэндлер добавляем ТОЛЬКО если путь уже инициализирован (на Этаже 1)
        # В других контейнерах _log_file_path не будет задан, и файл не создастся.
        try:
            if '_log_file_path' in globals() and _log_file_path:
                logger.addHandler(get_file_handler())
        except NameError:
            pass # Путь не задан — мы в удаленном воркере
            
        # 2. Эти работают всегда и везде
        logger.addHandler(get_stream_handler())
        logger.addHandler(get_prefect_handler())
        
    return logger
Используйте код с осторожностью.

Как это работает без гонки:
Логгер в удаленном контейнере (воркере) отправляет данные через PrefectLogHandler по API в базу Prefect.
Главный флоу (Этаж 1) забирает эти логи через контекст и ваш кастомный логгер (который инициализирован с FileHandler) записывает их в итоговый CSV.
Если вы хотите именно ОДИН CSV-файл и прямой доступ:
В Prefect 3.0 для этого используется экспорт логов. Чтобы избежать записи в один файл из 5 контейнеров (что неизбежно приведет к перемешиванию строк в CSV на сетевой шаре), правильная стратегия:
Воркеры пишут логи в Prefect.
Этаж 1 в блоке finally выгружает все логи текущего flow_run_id (включая все сабфлоу) одной командой в CSV.
Хотите реализовать финальную выгрузку логов в CSV на Этаже 1 или оставить «живую» запись через проброс пути в каждый сабфлоу (несмотря на риск гонки)?


"""