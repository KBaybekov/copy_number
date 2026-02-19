from asyncio import Semaphore
from pathlib import Path
from typing import Dict, Callable
from classes.sample import Sample

RES_FOLDER = Path('/mnt/cephfs8_rw/nanopore2/service/github/neurology/cyp2d6/result/')
SAMPLE_CSV = 'CYP2D6_samples.tsv'
ONT_FOLDER = Path('/mnt/cephfs8_ro/nanopore/')
# ~FASTQ*8
RAW_DATA_THRESHOLD = 528
# FASTQ Cov >= 20
BASECALLED_DATA_THRESHOLD = 66
BASECALLING_CMD_FAST5 = [
                   'dorado_0-9-6', 'basecaller',
                   '--device', 'cuda:GPU_ID',
                   '--emit-fastq', 'hac',
                   'SRC_DIR'
                  ]
BASECALLING_CMD_POD5 = [
                   'dorado_1-3-1', 'basecaller',
                   '--device', 'cuda:GPU_ID',
                   '--emit-fastq', 'hac',
                   'SRC_DIR'
                  ]

ALIGN_FQ_CMD = [
                'nextflow',
                '-log', 'RES_D_LOG',
                'run', 'epi2me-labs/wf-alignment',
                '-c', 'NXF_CFG',
                '-resume'
               ]

ALIGNMENT_CONFIG_TEMPLATE = Path("data/nxf_alignment_template.config")

# Настройки ограничений
MAX_BASECALL = 5
MAX_ALIGNERS = 18
MAX_MERGE_BAMS = 4
MAX_QC_BAMS = 6
MAX_CALLING = 4
AVAILABLE_GPU_IDS = [2, 3, 4, 6, 7]

THREADS_PER_ALIGNMENT = 16
ALIGNMENT_TIMEOUT = 60*60*10

# Семафоры
basecall_semaphore = Semaphore(MAX_BASECALL)
align_semaphore = Semaphore(MAX_ALIGNERS)
merge_semaphore = Semaphore(MAX_MERGE_BAMS)
qc_semaphore = Semaphore(MAX_QC_BAMS)
call_semaphore = Semaphore(MAX_CALLING)
# ИЗМЕНИТЬ ПРИ ИЗМЕНЕНИИ СПИСКОВ ЗАДАЧ
STAGE_DEPENDENCIES = {
                      'alignment':{
                                   'semaphore':align_semaphore,
                                   'args':{'threads_per_alignment':THREADS_PER_ALIGNMENT}
                                  }
                     }


# Реестр условий входа для этапов
STAGE_CONDITIONS: Dict[str, Callable[[Sample], bool]] = {
                                                         'alignment': lambda s: all([
                                                                                     len(s.fq_folders) != len(s.bams),
                                                                                     s.stage_statuses.get('basecalling') == "OK"
                                                                                    ])
                                                        }