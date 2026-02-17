# -*- coding: utf-8 -*-
"""
Логгирование работы: 
1. В CSV-файл (DEBUG)
2. В stdout (INFO)
3. В Prefect UI (автоматически при наличии активного run_context)
"""
from __future__ import annotations
from logging import (
    getLogger, Formatter, FileHandler, StreamHandler, 
    Handler, INFO, DEBUG, LogRecord
)
from pathlib import Path
from csv import writer as csv_writer, QUOTE_ALL
from io import StringIO
from datetime import datetime
from prefect.logging import get_run_logger
from prefect.context import FlowRunContext, TaskRunContext


now = datetime.now()
formatted_time = now.strftime("%d-%m-%Y_%H:%M:%S")

log_file = Path(f'./logs/log_{formatted_time}.csv').resolve()
log_file.parent.mkdir(exist_ok=True)

CSV_COLUMNS = ["Day", "Month", "Year", "Hour", "Minutes", "Seconds", "Microseconds", "Level", "Logger", "Location", "Message"]

class PrefectLogHandler(Handler):
    """
    Кастомный хэндлер для автоматической трансляции логов в Prefect UI.
    """
    def emit(self, record: LogRecord) -> None:
        # Проверяем, находимся ли мы в контексте выполнения Prefect
        if FlowRunContext.get() or TaskRunContext.get():
            try:
                p_logger = get_run_logger()
                # Транслируем уровень лога и сообщение
                p_logger.log(record.levelno, self.format(record))
            except Exception:
                # Если что-то пошло не так с Prefect, не роняем основной поток
                pass

class CsvFormatter(Formatter):
    """Форматтер для записи в CSV структуру."""
    def __init__(self) -> None:
        super().__init__()
        self.output = StringIO()
        self.writer = csv_writer(self.output, quoting=QUOTE_ALL)

    def format(self, record: LogRecord) -> str:
        dt = datetime.fromtimestamp(record.created)
        self.writer.writerow([
            dt.strftime("%d"), dt.strftime("%m"), dt.strftime("%Y"),
            dt.strftime("%H"), dt.strftime("%M"), dt.strftime("%S"),
            dt.strftime("%f"), record.levelname, record.name,
            f"{record.funcName}:{record.lineno}", record.msg
        ])
        data = self.output.getvalue()
        self.output.truncate(0)
        self.output.seek(0)
        return data.strip()

console_fmt = Formatter(
    fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
    datefmt="%d.%m.%Y %H:%M:%S",
)

def get_file_handler() -> FileHandler:
    if not log_file.exists():
        with open(log_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv_writer(f, quoting=QUOTE_ALL)
            writer.writerow(CSV_COLUMNS)
    handler = FileHandler(log_file, encoding='utf-8')
    handler.setLevel(DEBUG)
    handler.setFormatter(CsvFormatter())
    return handler

def get_stream_handler() -> StreamHandler:
    handler = StreamHandler()
    handler.setLevel(INFO)
    handler.setFormatter(console_fmt)
    return handler

def get_prefect_handler() -> PrefectLogHandler:
    """Хэндлер для UI Prefect без лишних оберток."""
    handler = PrefectLogHandler()
    handler.setLevel(INFO)
    # Используем простой формат для UI, чтобы не дублировать дату (Prefect добавит свою)
    handler.setFormatter(Formatter("%(name)s: %(message)s"))
    return handler

def get_logger(name: str):
    logger = getLogger(name)
    if not logger.handlers:
        logger.setLevel(DEBUG)
        logger.addHandler(get_file_handler())
        logger.addHandler(get_stream_handler())
        logger.addHandler(get_prefect_handler())
    return logger

def get_log_file_path() -> Path:
    return log_file
