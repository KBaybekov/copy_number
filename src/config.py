from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from yaml import safe_load

from prefect.task_runners import ThreadPoolTaskRunner
from prefect.utilities.annotations import NotSet

from tasks.alignment import alignment, alignment_arg_factory

RES_FOLDER = Path('/mnt/cephfs8_rw/nanopore2/service/github/neurology/cyp2d6/result/')
SAMPLE_CSV = 'CYP2D6_samples.tsv'
ONT_FOLDER = Path('/mnt/cephfs8_ro/nanopore/')
# ~FASTQ*8
RAW_DATA_THRESHOLD = 528
# FASTQ Cov >= 20
BASECALLED_DATA_THRESHOLD = 66


# Настройки ограничений
  # CPU
# максимальная загрузка CPU
CPUS_PER_WORKER = 256
CPUS_MAX_LOAD_PERC = 90

CPUS_ALIGNMENT = 14
  # GPU
GPUS_PER_WORKER = 0
  # RAM
RAM_PER_WORKER = 2000
RAM_MAX_LOAD_PERC = 70



MAX_BASECALL = 5
MAX_ALIGNERS = 18
MAX_MERGE_BAMS = 4
MAX_QC_BAMS = 6
MAX_CALLING = 4
AVAILABLE_GPU_IDS = [2, 3, 4, 6, 7]

THREADS_PER_ALIGNMENT = 16
ALIGNMENT_TIMEOUT = 60*60*10

now = datetime.now()
formatted_now = now.strftime("%d-%m-%Y_%H:%M:%S")
with open((Path(__file__).resolve().parents[1] / 'main_flow_options.yaml'), 'r') as file:
    main_flow_options: Dict[str, Any] = safe_load(file)
# Additional options for customizing main flow
main_flow_options.update({
                          "flow_run_name": f"{main_flow_options['name']}_{formatted_now}",
                          "task_runner": ThreadPoolTaskRunner()
                         })

# Аргументы по умолчанию для флоу/тасок заданий
DEFAULT_COMMON_ARGS = {
                       'timeout_seconds': None,
                       'retries': 3,
                       'retry_delay_seconds': 20,
                       'persist_result': NotSet,
                       'result_storage': NotSet,
                       'result_serializer': NotSet,
                       'log_prints': True,
                       'on_completion': None,
                       'on_failure': None,
                      }

DEFAULT_FLOW_ARGS = {
                     'task_runner': ThreadPoolTaskRunner(),
                     'validate_parameters': True,
                     'cache_result_in_memory': None,
                     'on_cancellation': None,
                     'on_crashed': None,
                     'on_running': None
                    }

DEFAULT_TASK_ARGS = {
                     'tags':['nanopore'],
                     'cache_key_fn': None,
                     'cache_expiration': None,
                     'retry_jitter_factor': 0.5,
                     'result_storage_key': NotSet,
                     'log_prints': True,
                     'refresh_cache': NotSet,
                     'retry_condition_fn': None,
                     'viz_return_value': None
                    }


# ИЗМЕНИТЬ ПРИ ИЗМЕНЕНИИ СПИСКОВ ЗАДАЧ
STAGE_DEPENDENCIES = {
                      'alignment':{
                                   'args':{'threads_per_alignment':THREADS_PER_ALIGNMENT},
                                   'prefect_flow_args': None,
                                   'prefect_task_args': {
                                                         'name':"alignment_nanopore",
                                                         'description': 'Выравнивание .fastq файлов ONT',
                                                         'timeout_seconds': ALIGNMENT_TIMEOUT,
                                                         'tags': ['nanopore', 'alignment', 'cpu', 'nextflow', 'long']                                                        
                                                        },
                                    'prefect_shell_block':'nextflow-v1',
                                    'prefect_tag_limit':{
                                                         'nanopore_alignment_cpu': {'cpu':CPUS_ALIGNMENT},
                                                         'nanopore_alignment_gpu': {'gpu':None},
                                                         'nanopore_alignment_ram': {'ram':None},
                                                        },
                                    'handler': alignment,
                                    'arg_factory': alignment_arg_factory
                                  }
                     }

# Формирование полного набора стандартных аргументов для флоу/тасок
full_default_task_args = DEFAULT_COMMON_ARGS | DEFAULT_TASK_ARGS
full_default_flow_args = DEFAULT_COMMON_ARGS | DEFAULT_FLOW_ARGS
for stage, stage_options in STAGE_DEPENDENCIES.items():
    for arg_type, arg_value in stage_options.items():
        match arg_type:
            case 'prefect_flow_args':
                match arg_value:
                  case None | {}:
                    STAGE_DEPENDENCIES[stage].update({arg_type:None})
                  case dict():
                      new_args = full_default_flow_args.copy()
                      new_args.update(arg_value)
                      STAGE_DEPENDENCIES[stage].update({arg_type:new_args})
            case 'prefect_task_args':
                match arg_value:
                  case None | {}:
                    STAGE_DEPENDENCIES[stage].update({arg_type:full_default_task_args})
                  case dict():
                      new_args = full_default_task_args.copy()
                      new_args.update(arg_value)
                      STAGE_DEPENDENCIES[stage].update({arg_type:new_args})
            case _:
              continue
