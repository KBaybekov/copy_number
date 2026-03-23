# src/format_handlers/excel_handler.py
from pathlib import Path
from typing import List, Optional, Tuple
from classes.sample import Sample

def process_input_data(sample_data: Tuple[Path, Optional[Path]]) -> List[Sample]:
    """Создаёт тестовые образцы для проверки оркестратора."""
    samples = []
    for i in range(2):
        sample = Sample(
            id=f"test_{i+1}",
            work_folder=Path("/tmp/work"),
            res_folder=Path("/tmp/res"),
            fq_folders={Path("/fake/fastq_pass")},
            value1=1
        )
        samples.append(sample)
    return samples