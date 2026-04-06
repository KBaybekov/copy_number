# -*- coding: utf-8 -*-
from __future__ import annotations

from asyncio import create_task as create_atask, gather as agather
from pathlib import Path
from typing import Dict, List, Optional, Any, Coroutine

from prefect import flow
from prefect.artifacts import acreate_markdown_artifact
from prefect_shell import ShellOperation


# Импорт кастомных модулей
from config import main_flow_options, STAGE_DEPENDENCIES
from core.sample_workflow import sample_workflow
from format_handlers.excel_handler import process_input_data
from modules.logger import get_logger
from modules.prefect import create_prefect_run_name, get_run_id, set_tag_gcl
from classes.sample import Sample


@flow(**main_flow_options)
async def main_pipeline(
                        table_input: str,
                        sample_data_csv: Optional[str] = None
                       ) -> None:
    """
    Точка входа в систему (Этаж 1-2). 
    
    :param table_input: Путь к исходной таблице Excel с метаданными.
    :param sample_data_csv: Опциональный путь к CSV результатам предыдущих запусков.
    """
        #УДАЛИТЬ, ВРЕМЕННОЕ РЕШЕНИЕ ДЛЯ ЗАПУСКА NEXTFLOW В ЭТОМ ЖЕ КОНТЕЙНЕРЕ
    prep_cmds = [
                    'cat /etc/apt/sources.list',
                    '''echo "deb http://debian.org trixie main
                        deb http://debian.org trixie-security main
                        deb http://debian.org trixie-updates main" | sudo tee -a /etc/apt/sources.list
                    ''',
                    'cat /etc/apt/sources.list',
                    'apt-get update',
                    'apt-get install -y curl openjdk-21-jdk-headless',
                    'rm -rf /var/lib/apt/lists/*',
                    "java --version"
                    'curl -fsSL https://get.nextflow.io | bash && mv nextflow /usr/local/bin/'
                    ]
    async with ShellOperation(
                                        commands=prep_cmds,
                                        env={"NXF_HOME":"/mnt/cephfs8_rw/nanopore2/service/nextflow/"},
                                        stream_output=True
                                        ) as shell_op:
                    # Запускаем процесс
                    process = await shell_op.atrigger()
                    # Ждем завершения (заблокирует выполнение потока до конца пайплайна)
                    await process.await_for_completion()

    logger = await get_logger()

    # Устанавливаем tag-based лимиты одновременной обработки
    tag_limits:Dict[str, Dict[str, int|None]]
    resource_type:str
    demand:int|None
    for stage_data in STAGE_DEPENDENCIES.values():
        match stage_data:
            case {'prefect_tag_limit': tag_limits}:
                for tag in tag_limits.keys():
                    for resource_type, demand in tag_limits[tag].items():
                        create_atask(set_tag_gcl(tag=tag, resource_type=resource_type, demand=demand))
    
    logger.info(f"Запуск пайплайна. Таблица: {table_input}")

    # 1. Загрузка данных (Ваша логика из excel_handler)
    # Превращаем строковые пути из CLI в Path объекты для вашего парсера
    input_path = Path(table_input)
    sample_data_path = Path(sample_data_csv) if sample_data_csv else None
    status_str = ""
    if sample_data_path:
        status_str = f"- **Таблица с данными обработки образцов:** `{sample_data_path.name}`"
    
    # Инициализация списка объектов Sample
    samples: List[Sample] = await process_input_data((input_path, sample_data_path))
    
    if not samples:
        logger.warning("Список образцов пуст. Завершение работы.")
        return

    # Создаем краткий отчет (Artifact) в UI о начале работы
    # await cast(Coroutine[Any, Any, UUID], 
    create_atask(acreate_markdown_artifact(
                                           key="run-summary",
                                           markdown=(
                                                     "## Сводка запуска\n"
                                                     f"- **Количество образцов:** `{len(samples)}`\n"
                                                     f"- **Количество образцов, готовых к дальнейшей обработке:** `{len([s for s in samples if not s.finished])}`\n"
                                                     f"- **Таблица с исходными данными:** `{input_path.name}`\n"
                                                     f"{status_str} \n"
                                                    ),
                                            description="Параметры запуска"
                                           ))

    # Порождение независимых потоков (Subflows) для каждого сэмпла   
    logger.info(f"Инициализация асинхронных потоков для {len(samples)} образцов...")

    flow_id = await get_run_id()
    pipeline_name = main_flow_options['name']
    tasks: List[Coroutine[Any, Any, Sample]] = [
                                                sample_workflow.with_options(
                                                                             flow_run_name=await create_prefect_run_name(type='Subflow',
                                                                                                                   name="Sample_Workflow",
                                                                                                                   parent_id=flow_id,
                                                                                                                   sample_id=s.id
                                                                                                                  ),
                                                                             description=f"Workflow for sample [{s.id}] in pipeline [{pipeline_name}]"
                                                                            )(s) for s in samples if not s.finished]
    results: List[Sample | BaseException] = await agather(*tasks, return_exceptions=True)
    
    # Анализ итогов пачки
    success_count = sum(1 for r in results if isinstance(r, Sample) and r.success)
    error_count = len(results) - success_count
    logger.info(f"Из {len(results)} образцов {success_count} успешны, {error_count} - нет")
    return None
