from csv import DictReader, DictWriter, reader as csv_reader
from modules.logger import get_logger
from . import is_file_exists_n_not_empty
from pathlib import Path
from typing import List, Optional
import tempfile
import os
from threading import Lock

CSV_WRITE_LOCK = Lock()


async def write_sample_data(sample_data:dict) -> None:
    logger = await get_logger()

    from config import SAMPLE_CSV
    sample_csv = SAMPLE_CSV
    sample_id = sample_data.get('id', 'unknown')
    fieldnames = list(sample_data.keys())
    if sample_id != 'unknown':
        # Блокируем доступ к файлу для всех остальных потоков
        with CSV_WRITE_LOCK:
            if sample_csv.exists():
                # Если файл есть, создаем временный файл для перезаписи
                fd, temp_path = tempfile.mkstemp(dir=sample_csv.parent)
                try:
                    with os.fdopen(fd, 'w', newline='', encoding='utf-8') as temp_file:
                        with open(sample_csv, 'r', newline='', encoding='utf-8') as original_file:
                            reader = DictReader(original_file, delimiter='\t')
                            # Если в исходном файле другие колонки, подтягиваем их для сохранения структуры
                            current_fieldnames = reader.fieldnames if reader.fieldnames else fieldnames
                            writer = DictWriter(temp_file, fieldnames=current_fieldnames, delimiter='\t')
                            writer.writeheader()

                            updated = False
                            for row in reader:
                                if str(row.get('id')) == sample_id:
                                    # Заменяем старую строку новой
                                    writer.writerow(sample_data)
                                    updated = True
                                else:
                                    # Оставляем существующую строку как есть
                                    writer.writerow(row)
                            
                            # Если ID не был найден в файле, добавляем его в конец
                            if not updated:
                                writer.writerow(sample_data)

                    # Заменяем оригинальный файл временным
                    os.replace(temp_path, sample_csv)
                    
                    status = "updated" if updated else "added as new"
                    logger.debug(f"Data with id {sample_id} {status} in csv")

                except Exception as e:
                    os.remove(temp_path)
                    logger.error(f"Error during CSV update: {e}")
                    raise
            else:
                with open(
                        sample_csv,
                        'a',
                        newline='',
                        encoding='utf-8'
                        ) as csvfile:
                    writer = DictWriter(csvfile, fieldnames=sample_data.keys(), delimiter='\t')
                    # write header while writing first time
                    writer.writeheader()
                    writer.writerow(sample_data)

                logger.debug(f"Created sample CSV & data for {sample_id} added to csv")
            
    else: 
        logger.error(f"Unknown id for data: {sample_data}")
    return None


def extract_value_from_tsv(
                           file_path:Path,
                           row_index,
                           col_index,
                           has_header=True
                          ) -> str:
    """
    Извлекает значение из TSV файла.
    :param file_path: путь к TSV
    :param row_index: номер строки (0-based, если has_header=False)
    :param col_index: номер колонки (0-based)
    :param has_header: если True, то первая строка считается заголовком, и row_index отсчитывается после него
    :return: значение в указанной ячейке
    """
    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv_reader(f, delimiter='\t')
        # Пропускаем заголовок, если нужно
        if has_header:
            next(reader)
        # Добираемся до нужной строки
        for i, row in enumerate(reader):
            if i == row_index:
                if col_index < len(row):
                    return row[col_index]
                else:
                    raise IndexError(f"Колонки с индексом {col_index} нет в строке {row_index}")
        raise IndexError(f"Строки с индексом {row_index} нет в файле")
