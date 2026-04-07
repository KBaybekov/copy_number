# -*- coding: utf-8 -*-
"""
Логгирование работы: 
1. В CSV-файл (DEBUG)
2. В stdout (INFO)
3. В Prefect UI (автоматически при наличии активного run_context)
"""
import logging
from prefect import flow, get_run_logger
from prefect.client.schemas import FlowRun
from prefect.context import get_run_context, FlowRunContext, TaskRunContext
from prefect.logging.handlers import APILogHandler
from prefect.artifacts import acreate_link_artifact, create_link_artifact
from datetime import datetime
from pathlib import Path
from os import getenv

from modules.utils import sanitize_artifact_key

LOG_FOLDER = Path(getenv('LOG_FOLDER', '.logs/'))
SERVER_IP = getenv('SERVER_IP', 'localhost')
if SERVER_IP != 'localhost':
    SERVER_IP = f"http://{SERVER_IP}"

TSV_COLUMNS = [
               "Day",
               "Month",
               "Year",
               "Hour",
               "Minutes",
               "Seconds",
               "Microseconds",
               "Prefect_Flow_ID",
               "Level",
               "Logger",
               "Location","Message"
              ]

class TsvFormatter(logging.Formatter):
    """Быстрый TSV форматтер без лишних объектов."""
    def __init__(self, flow_run_id: str):
        super().__init__()
        self.flow_run_id = str(flow_run_id)
    def format(self, record: logging.LogRecord) -> str:
        dt = datetime.fromtimestamp(record.created)
        # Экранируем сообщение (кавычки), если там есть табы или переносы
        msg = str(record.msg).replace('"', '""').replace('\t', ' ').replace('\n', ' ')
        parts = [
            dt.strftime("%d"), dt.strftime("%m"), dt.strftime("%Y"),
            dt.strftime("%H"), dt.strftime("%M"), dt.strftime("%S"),
            dt.strftime("%f"), self.flow_run_id, record.levelname,
            record.filename, f"{record.funcName}:{record.lineno}", f'"{msg}"'
        ]
        return "\t".join(parts)

async def get_logger():
    
    logger = get_run_logger()  # адаптер Prefect
    print("Initialized Prefect logger")
    
    # 1. Разрешаем логгеру флоу глотать DEBUG
    logger.setLevel(logging.DEBUG)
    print("Set level")
    
    # 3. Проверяем, что мы внутри флоу
    ctx_name = ""
    run_id = ""
    try:
        ctx = get_run_context()
        print("Got run context")
    except RuntimeError:
        print("Логгер вызван вне контекста объекта Prefect!")
        raise
    match ctx:
        case FlowRunContext():
            match ctx.flow_run:
                case FlowRun():
                    run_id = ctx.flow_run.id
                    ctx_name =  ctx.flow_run.name
                    print(f"Got context:\n\tctx_name: '{ctx_name}'\n\trun_id: '{run_id}'")
                case None:
                    print("FlowRunContext есть, но flow_run = None")
        case TaskRunContext():
            run_id = ctx.task_run.id
            ctx_name =  ctx.task_run.name
            print(f"Got context:\n\tctx_name: '{ctx_name}'\n\trun_id: '{run_id}'")

    if all([ctx_name, run_id]):
        # 4. Формируем путь к файлу
        now = datetime.now()
        log_dir = LOG_FOLDER / now.strftime("%d_%m_%Y")
        log_dir.mkdir(parents=True, exist_ok=True)
        log_filepath = log_dir / f"{ctx_name}_{now.strftime('%H:%M:%S')}.tsv".replace(" ", "_").replace('[', '').replace(']', '')
        print('Formed log files+dirs')

        # 5. Получаем реальный логгер из адаптера (чтобы добавить обработчик)
        real_logger = logger.logger if hasattr(logger, 'logger') else logger # type: ignore
        print('Got real logger')
        # 6. Защита от дублирования файлового обработчика
        handlers:list[logging.Handler] = real_logger.handlers # type: ignore
        print(f"Handlers: {handlers}")
        if not any(getattr(h, 'baseFilename', None) == str(log_filepath.absolute()) 
                            for h in handlers):
            # Создаём файл с заголовком, если его нет
            if not log_filepath.exists():
                log_filepath.write_text("\t".join(TSV_COLUMNS) + "\n", encoding='utf-8')
            
            # Добавляем файловый обработчик с уровнем DEBUG и TSV-форматированием
            handler = logging.FileHandler(log_filepath, encoding='utf-8')
            handler.setLevel(logging.DEBUG)
            handler.setFormatter(TsvFormatter(flow_run_id=str(run_id)))
            logger.logger.addHandler(handler) # type: ignore

            """
            console_handler = PrefectConsoleHandler(level=logging.INFO)
            logger.logger.addHandler(console_handler)
            """

        for handler in logger.logger.handlers: # type: ignore
            if isinstance(handler, APILogHandler):
                handler.setLevel(logging.INFO)
            """
            if isinstance(handler, PrefectConsoleHandler):
                handler.setLevel(logging.INFO)
        print(logger.logger.handlers)"""
            
        safe_key = sanitize_artifact_key(raw_key=f"{ctx_name}-logs")
        print(f"Formed artifact key: {safe_key}")
        create_link_artifact(
                            key=safe_key,  # общий ключ для всех запусков флоу
                            # преобразуем путь в file:// URL, убираем всё, кроме род. папки и имени файла
                            # (в Apache2 прописан алиас к LOG_FOLDER) 
                            link=f"{SERVER_IP}/{log_filepath.resolve().as_posix().replace(LOG_FOLDER.as_posix(), 'logs')}", 
                            link_text="📄 Открыть лог-файл",
                            description=f"""# Логи запуска {ctx_name}
                                            - ID запуска: `{run_id}`
                                            - Дата: {datetime.now().strftime("%d.%m.%Y %H:%M:%S")}
                                            - Формат: TSV
                                            - Полный путь: {log_filepath.resolve().as_posix()}
                                            Файл содержит полные логи уровня DEBUG и выше.
                                        """
                            )
        print('Created artifact key')
        return logger
    else:
        raise ValueError("Данные контекста Prefect для логгера не получены!")

@flow(name="test-log")
async def some_flow():
    logger = await get_logger()
    logger.debug("Тестовое сообщение debug")
    logger.info("Тестовое сообщение info")
    logger.warning("Тестовое сообщение warning")
    logger.error("Тестовое сообщение error")
    logger.critical("Тестовое сообщение critical")
