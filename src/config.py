from datetime import datetime
from pathlib import Path
from typing import Any, Dict
from yaml import safe_load

from prefect.task_runners import ThreadPoolTaskRunner
from prefect.utilities.annotations import NotSet

from tasks.alignment import alignment, alignment_arg_factory

SAMPLE_CSV = Path('/mnt/cephfs8_rw/nanopore2/service/github/neurology/cyp2d6/result/CYP2D6_samples.tsv')

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

# Настройки главного флоу пайплайна. Основные изменения проводить в main_flow_options.yaml
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
                       'tags':['nanopore', 'cyp2d6_cnv']
                      }

# Базовые аргументы для запуска сабфлоу из других деплойментов
DEFAULT_SUBFLOW_ARGS = {
                        'as_subflow': True
                       }

DEFAULT_TASK_ARGS = {
                     'cache_policy': NotSet,
                     'cache_key_fn': None,
                     'cache_expiration': None,
                     'cache_result_in_memory':None,
                     'retries': 3,
                     'retry_condition_fn': None,
                     'retry_delay_seconds': 10,
                     'retry_jitter_factor': 0.5,
                     'persist_result': NotSet,
                     'result_storage': NotSet,
                     'result_storage_key': NotSet,
                     'log_prints': True,
                     'on_completion': NotSet,
                     'on_failure': NotSet,
                     'refresh_cache': NotSet,
                     'timeout_seconds':None,
                     'viz_return_value': None
                    }


# ИЗМЕНИТЬ ПРИ ИЗМЕНЕНИИ СПИСКОВ ЗАДАЧ
STAGE_DEPENDENCIES = {
                      'alignment':{
                                   'args':{'threads_per_alignment':THREADS_PER_ALIGNMENT},
                                   'prefect_subflow_args': None,
                                   'prefect_task_args': {
                                                         'name':"alignment_nanopore",
                                                         'description': 'Выравнивание .fastq файлов ONT',
                                                         'timeout_seconds': ALIGNMENT_TIMEOUT,
                                                         'tags': ['nanopore', 'alignment', 'cpu', 'nextflow', 'long']                                                        
                                                        },
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
full_default_subflow_args = DEFAULT_COMMON_ARGS | DEFAULT_SUBFLOW_ARGS
for stage, stage_options in STAGE_DEPENDENCIES.items():
    for arg_type, arg_value in stage_options.items():
        match arg_type:
            case 'prefect_subflow_args':
                match arg_value:
                  case None | {}:
                    STAGE_DEPENDENCIES[stage].update({arg_type:full_default_subflow_args})
                  case dict():
                      new_args = full_default_subflow_args.copy()
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
