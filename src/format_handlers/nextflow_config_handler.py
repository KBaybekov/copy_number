#from yaml import dump
from pathlib import Path
from typing import Optional
from . import is_file_exists_n_not_empty
from jinja2 import Template
from modules.logger import get_logger

def form_nxf_cfg(
                 template:str,
                 data:dict,
                 output_filepath:Path
                ) -> Optional[Path]:
    """
    Формирует файл конфигурации Nextflow для конкретного этапа на основе шаблона и данных.
    Данные вида: {Ключевое слово в шаблоне : значение в образце}
    """
    logger = get_logger()
    if not output_filepath.parent.exists():
        output_filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(output_filepath, 'w') as f:
        text = Template(template).render(**data)
        f.write(text)
    if is_file_exists_n_not_empty(output_filepath):
        logger.debug(f"Файл конфигурации Nextflow сформирован: {output_filepath}")
        return output_filepath
    else:
        if output_filepath.exists():
            output_filepath.unlink()
        logger.error(f"Файл конфигурации Nextflow не сформирован: {output_filepath}")
        return None
