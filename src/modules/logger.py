import logging
from io import StringIO
from csv import writer as csv_writer, QUOTE_ALL
from prefect import flow, get_run_logger
from prefect.context import FlowRunContext
from datetime import datetime
from pathlib import Path

TSV_COLUMNS = ["Day", "Month", "Year", "Hour", "Minutes", "Seconds", "Microseconds", "Level", "Logger", "Location", "Message"]

class TsvFormatter(logging.Formatter):
    """Быстрый TSV форматтер без лишних объектов."""
    def format(self, record: logging.LogRecord) -> str:
        dt = datetime.fromtimestamp(record.created)
        # Экранируем сообщение (кавычки), если там есть табы или переносы
        msg = str(record.msg).replace('"', '""').replace('\t', ' ').replace('\n', ' ')
        parts = [
            dt.strftime("%d"), dt.strftime("%m"), dt.strftime("%Y"),
            dt.strftime("%H"), dt.strftime("%M"), dt.strftime("%S"),
            dt.strftime("%f"), record.levelname, record.name,
            f"{record.funcName}:{record.lineno}", f'"{msg}"'
        ]
        return "\t".join(parts)

def get_file_handler(log_filepath: Path) -> logging.FileHandler:
    log_filepath.parent.mkdir(exist_ok=True, parents=True)
    if not log_filepath.exists():
        with open(log_filepath, 'w', encoding='utf-8', newline='') as f:
            writer = csv_writer(f, quoting=QUOTE_ALL)
            writer.writerow(TSV_COLUMNS)
    handler = logging.FileHandler(log_filepath, encoding='utf-8')
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(TsvFormatter())
    return handler

def setup_custom_logger(log_folder: Path):
    # определяем контекст флоу
    ctx = FlowRunContext.get()
    if not ctx or not ctx.flow or not ctx.flow_run:
        return # Если вдруг запустили вне флоу
    
    # Формируем путь
    log_dir = log_folder / datetime.now().strftime("%d_%m_%Y")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_filepath = log_dir / f"{ctx.flow.name}_{ctx.flow_run.id}.tsv"
    # Получаем корневой логгер Prefect
    logger = logging.getLogger("prefect.flow_runs")
    # Защита от дублирования хэндлеров в рамках одного процесса
    if not any(getattr(h, 'baseFilename', None) == str(log_filepath.absolute()) for h in logger.handlers):
        if not log_filepath.exists():
            log_filepath.write_text("\t".join(TSV_COLUMNS) + "\n", encoding='utf-8')
        
        handler = logging.FileHandler(log_filepath, encoding='utf-8')
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(TsvFormatter())
        logger.addHandler(handler)

@flow(name="test-log")
def some_flow():
    setup_custom_logger(Path("/mnt/cephfs8_rw/nanopore2/logs"))
    logger = get_run_logger()
    logger.debug("Тестовое сообщение debug")
    logger.info("Тестовое сообщение info")
    logger.warning("Тестовое сообщение warning")
    logger.error("Тестовое сообщение error")
    logger.critical("Тестовое сообщение critical")
