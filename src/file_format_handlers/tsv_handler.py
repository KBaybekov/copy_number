from csv import DictReader, DictWriter
from modules.logger import get_logger
from . import is_file_exists_n_not_empty
from config import RES_FOLDER, SAMPLE_CSV
from pathlib import Path
from typing import List, Optional
import tempfile
import os
from threading import Lock

CSV_WRITE_LOCK = Lock()

logger = get_logger(__name__)

def write_sample_data(sample_data:dict) -> None:
    sample_csv = RES_FOLDER / SAMPLE_CSV
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

def form_nxf_tsv(data_dict: dict|List[dict], filepath: Path) -> Optional[Path]:
    """
    Создает TSV файл из словаря. 
    Ключи словаря становятся заголовками, значения — строкой.
    Если данных несколько, ожидается список словарей.
    """
    # Гарантируем, что работаем со списком (даже если передан один объект)
    if isinstance(data_dict, dict):
        data = [data_dict]
    else:
        data = data_dict

    if not data:
        raise ValueError("Данные для записи пусты")

    # Извлекаем заголовки из ключей первого словаря
    fieldnames = data[0].keys()

    # Создаем папку, если ее нет
    if not filepath.parent.exists():
        filepath.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Создана папка для TSV: {filepath.parent}")

    # Записываем данные в файл
    with open(filepath, mode='w', newline='', encoding='utf-8') as tsvfile:
        writer = DictWriter(tsvfile, fieldnames=fieldnames, delimiter='\t')
        
        writer.writeheader()
        writer.writerows(data)
    
    # проверяем, что файл создан, иначе возвращаем None
    if is_file_exists_n_not_empty(filepath):
        return filepath
    else:
        logger.error(f"Файл не создан: {filepath}")
        return None
