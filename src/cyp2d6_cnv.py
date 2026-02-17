#!/usr/bin/ python3

import asyncio
import signal
import sys
import threading
from modules.argument_parser import get_args
from modules.logger import get_logger
from file_format_handlers.excel_handler import process_input_data
from modules.async_processor import async_process_samples

logger = get_logger(__name__)
# Глобальное событие остановки
stop_event = threading.Event()

def signal_handler(signum, frame):
    """Обработчик SIGINT (Ctrl+C)."""
    logger.warning("Получен сигнал остановки (Ctrl+C)")
    stop_event.set()
    # Отменяем текущую задачу
    if asyncio.current_task() is not None:
        asyncio.current_task().cancel() # type: ignore
    else:
        # Если нет активной задачи — отменяем все
        for task in asyncio.all_tasks():
            if not task.done():
                task.cancel()


async def main():
    global main_task
    try:
        input_data = get_args()
        sample_data = process_input_data(input_data)
        logger.info('Метаданные загружены. Начало асинхронной обработки...')

        main_task = asyncio.current_task()
        await async_process_samples(sample_data, stop_event)

    except asyncio.CancelledError:
        logger.warning("Main task cancelled. Shutting down gracefully...")
        stop_event.set()
        await asyncio.sleep(0.1)
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}", exc_info=True)
    finally:
        logger.info("Shutting down...")
        await asyncio.sleep(0.1)
        sys.exit(0)


if __name__ == "__main__":
    # Регистрируем обработчик сигнала
    signal.signal(signal.SIGINT, signal_handler)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # На всякий случай
        stop_event.set()
        logger.critical("Программа принудительно остановлена.")
