from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Callable
from yaml import safe_load
from prefect.task_runners import ThreadPoolTaskRunner

from classes.sample import Sample
from tasks.alignment import alignment, alignment_arg_factory


now = datetime.now()
formatted_now = now.strftime("%d-%m-%Y_%H:%M:%S")
with open((Path(__file__).resolve().parents[1] / 'main_flow_options.yaml'), 'r') as file:
    main_flow_options: Dict[str, Any] = safe_load(file)
# Additional options for customizing main flow
main_flow_options.update({
                          "flow_run_name": f"{main_flow_options['name']}_{formatted_now}",
                          "task_runner": ThreadPoolTaskRunner()
                         })



RES_FOLDER = Path('/mnt/cephfs8_rw/nanopore2/service/github/neurology/cyp2d6/result/')
SAMPLE_CSV = 'CYP2D6_samples.tsv'
ONT_FOLDER = Path('/mnt/cephfs8_ro/nanopore/')
# ~FASTQ*8
RAW_DATA_THRESHOLD = 528
# FASTQ Cov >= 20
BASECALLED_DATA_THRESHOLD = 66


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

# ИЗМЕНИТЬ ПРИ ИЗМЕНЕНИИ СПИСКОВ ЗАДАЧ
STAGE_DEPENDENCIES = {
                      'alignment':{
                                   'args':{'threads_per_alignment':THREADS_PER_ALIGNMENT},
                                   'prefect_task_args': {
                                                         'description': 'Выравнивание .fastq файлов ONT',
                                                         'timeout': ALIGNMENT_TIMEOUT,
                                                         'retries': 3,
                                                         'retry_delay_seconds': 20,
                                                         'tags': ['nanopore', 'alignment', 'cpu'],
                                                         'log_prints': True                                                        
                                                        },
                                    'prefect_shell_block':'nextflow-v1',
                                    'handler': alignment,
                                    'arg_factory': alignment_arg_factory
                                  }
                     }

"""cache_key_fn
A new cache key function for the task.

cache_expiration
A new cache expiration time for the task.

persist_result
A new option for enabling or disabling result persistence.

result_storage
A new storage type to use for results.

result_serializer
A new serializer to use for results.

result_storage_key
A new key for the persisted result to be stored at.

refresh_cache
A new option for enabling or disabling cache refresh.

on_completion
A new list of callables to run when the task enters a completed state.

on_failure
A new list of callables to run when the task enters a failed state.

retry_condition_fn
An optional callable run when a task run returns a Failed state. Should return True if the task should continue to its retry policy, and False if the task should end as failed. Defaults to None, indicating the task should always continue to its retry policy.

viz_return_value
An optional value to return when the task dependency tree is visualized."""


# Реестр условий входа для этапов
STAGE_CONDITIONS: Dict[str, Callable[[Sample], bool]] = {
                                                         'alignment': lambda s: all([
                                                                                     len(s.fq_folders) != len(s.bams),
                                                                                     s.stage_statuses.get('basecalling') == "OK"
                                                                                    ])
                                                        }
