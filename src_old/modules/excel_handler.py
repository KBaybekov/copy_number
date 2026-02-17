import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
from modules.logger import get_logger
from classes.sample import Sample


logger = get_logger(__name__)

def process_input_data(sample_xlsx:Path) -> List[Sample]:
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
    samples = []
    for _, row in data.iterrows():
        sample = Sample.from_dict(row.to_dict())
        #sample.gather_ont_metadata()
        samples.append(sample)
        
    return samples
