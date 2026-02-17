#from yaml import dump
from pathlib import Path
from typing import Optional
from . import is_file_exists_n_not_empty
from config import ALIGNMENT_CONFIG_TEMPLATE
from modules.logger import get_logger

logger = get_logger(__name__)

def form_nxf_cfg(stage:str, data:dict, output_filepath:Path) -> Optional[Path]:
    """
    Формирует файл конфигурации Nextflow для конкретного этапа на основе шаблона и данных.
    Данные вида: {Ключевое слово в шаблоне : значение в образце}
    """
    templates = {
                 'alignment': ALIGNMENT_CONFIG_TEMPLATE
                }
    if not output_filepath.parent.exists():
        output_filepath.parent.mkdir(parents=True)
    with open(output_filepath, 'w') as f:
        with open(templates[stage], 'r') as template_file:
            template = template_file.read()
        for template_mask, value in data.items():
            if isinstance(value, Path):
                template = template.replace(template_mask, value.as_posix())
            elif isinstance(value, str):
                template = template.replace(template_mask, value)
            elif isinstance(value, (int, float)):
                template = template.replace(template_mask, str(value))
        f.write(template)
    if is_file_exists_n_not_empty(output_filepath):
        logger.debug(f"Файл конфигурации Nextflow для этапа {stage} сформирован: {output_filepath}")
        return output_filepath
    else:
        if output_filepath.exists():
            output_filepath.unlink()
        logger.error(f"Файл конфигурации Nextflow для этапа {stage} не сформирован: {output_filepath}")
        return None
