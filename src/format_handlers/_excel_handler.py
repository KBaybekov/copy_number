import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from modules.logger import get_logger
from classes.sample import Sample


logger = get_logger()

def process_input_data(sample_data:Tuple[Path, Optional[Path]]) -> List[Sample]:
    samples = []
    sample_xlsx, sample_csv = sample_data
    if sample_csv:
        logger.info("Предоставлена таблица с результатами предыдущих обработок, загружаю...")
        data = pd.read_csv(sample_csv, delimiter='\t', header=0, dtype=str)
        data.fillna('', inplace=True)
        sample_forming_func = Sample.from_dict
    else:
        sample_forming_func = Sample.from_dict_init
        # load df
        data = pd.read_excel(
                                io=sample_xlsx,
                                sheet_name=0,
                                header=0,
                                converters={
                                    'group':str,
                                    'subgroup':str,
                                    'sample':str,
                                    'DB_ID':str,
                                    'Panno':str,
                                    'fast5_pass, G':float,
                                    'fast5_fail, G':float,
                                    'pod5_pass, G':float,
                                    'pod5_fail, G':float,
                                    'fastq_pass, G':float,
                                    'fastq_fail, G':float
                                    })
    logger.debug(f'Найдена таблица с {len(data)} образцами.')
    # form samples' metadata
    for _, row in data.iterrows():
        sample = sample_forming_func(row.to_dict())
        samples.append(sample)
            
    return samples
