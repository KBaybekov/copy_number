from asyncio import run as arun
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from yaml import safe_load

from prefect.task_runners import ThreadPoolTaskRunner
#from prefect.task_runners import ConcurrentTaskRunner
from prefect.utilities.annotations import NotSet

from modules.prefect import create_prefect_run_name

from tasks.alignment import alignment, alignment_arg_factory
from tasks.cnv_calling import cnv_calling, cnv_calling_arg_factory
from tasks.cnv_calling_no_subflow import cnv_calling_no_subflow, cnv_calling_no_subflow_arg_factory

def form_stage_data(
                    template:Dict[str, Dict[str, Any]],
                    subflow_args:Dict[str, Any],
                    task_args:Dict[str, Any],
                    active_tasks:List[str]
                   ) -> Dict[str, Dict[str, Any]]:
    STAGE_DEPENDENCIES = {}
    for stage_name, stage_opts in template.items():
        if stage_name not in active_tasks:
            continue
        # Копируем все поля, которые не требуют специальной обработки
        new_stage = stage_opts.copy()

        # --- Обработка аргументов для подпотоков (subflow) ---
        stage_subflow_args = stage_opts.get('prefect_subflow_args')
        if stage_subflow_args is None:
            new_stage['prefect_subflow_args'] = subflow_args
        else:
            # Базовые аргументы из DEFAULT_SUBFLOW_ARGS
            merged = DEFAULT_SUBFLOW_ARGS.copy()
            # Обновляем явно указанными значениями из стадии (теги обработаем отдельно)
            for key, value in stage_subflow_args.items():
                if key != 'tags':
                    merged[key] = value
            # Собираем теги из трёх источников: базовые, из конфига стадии, из prefect_tag_limit
            base_tags = DEFAULT_SUBFLOW_ARGS.get('tags', [])
            stage_tags = subflow_args.get('tags', [])
            limit_tags = list(stage_opts.get('prefect_tag_limit', {}).keys())
            merged['tags'] = list(set(base_tags + stage_tags + limit_tags))
            new_stage['prefect_subflow_args'] = merged

        # --- Обработка аргументов для задач (task) ---
        stage_task_args = stage_opts.get('prefect_task_args')
        if stage_task_args is None:
            new_stage['prefect_task_args'] = task_args
        else:
            merged = DEFAULT_TASK_ARGS.copy()
            for key, value in stage_task_args.items():
                if key != 'tags':
                    merged[key] = value
            base_tags = DEFAULT_TASK_ARGS.get('tags', [])
            stage_tags = stage_task_args.get('tags', [])
            limit_tags = list(stage_opts.get('prefect_tag_limit', {}).keys())
            merged['tags'] = list(set(base_tags + stage_tags + limit_tags))
            new_stage['prefect_task_args'] = merged

        STAGE_DEPENDENCIES[stage_name] = new_stage
    return STAGE_DEPENDENCIES

SAMPLE_CSV = Path('/mnt/cephfs8_rw/nanopore2/service/code/github/neurology/cyp2d6/result/CYP2D6_samples.tsv')

# Настройки ограничений
  # CPU
# максимальная загрузка CPU
CPUS_PER_WORKER = 256
CPUS_MAX_LOAD_PERC = 90

CPUS_ALIGNMENT = 14
CPUS_CNV_CALLING = 14
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
ALIGNMENT_TIMEOUT = 60*60*24

THREADS_PER_CNV_CALLING = 16
CNV_CALLING_TIMEOUT = 60*60*12

# Настройки главного флоу пайплайна. Основные изменения проводить в main_flow_options.yaml
now = datetime.now()
formatted_now = now.strftime("%d-%m-%Y_%H:%M:%S")
with open((Path(__file__).resolve().parents[1] / 'main_flow_options.yaml'), 'r') as file:
    main_flow_options: Dict[str, Any] = safe_load(file)
# Additional options for customizing main flow
main_flow_options.update({
                          "flow_run_name": arun(create_prefect_run_name(
                                                                   type="Pipeline",
                                                                   name=main_flow_options['name'],
                                                                   timestamp=formatted_now
                                                                  )),
                          "task_runner": ThreadPoolTaskRunner()
                         })

# Аргументы по умолчанию для флоу/тасок заданий
DEFAULT_SUBFLOW_ARGS = {
                        'as_subflow': True,
                        'tags':['nanopore', 'cyp2d6_cnv']
                       }

DEFAULT_TASK_ARGS = {}
"""DEFAULT_TASK_ARGS = {
                     'cache_policy': None,
                     'cache_key_fn': None,
                     'cache_expiration': None,
                     'cache_result_in_memory':None,
                     'retries': 0,
                     'retry_condition_fn': None,
                     'retry_delay_seconds': 10,
                     'retry_jitter_factor': 0.5,
                     'tags':['nanopore', 'cyp2d6_cnv'],
                     'persist_result': False,
                     'result_storage': None,
                     'result_storage_key': None,
                     'log_prints': True,
                     'on_completion': None,
                     'on_failure': None,
                     'refresh_cache': None,
                     'timeout_seconds':None,
                     'viz_return_value': None
                    }"""


# ИЗМЕНИТЬ ПРИ ИЗМЕНЕНИИ СПИСКОВ ЗАДАЧ
active_tasks = [
    #'cnv_calling_no_subflow',
      'blank'
    ]
PRE_STAGE_DEPENDIES = {
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
                                    'handler': 'src/tasks/alignment.py:alignment',
                                    'arg_factory': 'src/tasks/alignment.py:alignment_arg_factory'
                                  },
                      'cnv_calling':{
                                   'args':{'threads_per_cnv_calling':THREADS_PER_CNV_CALLING},
                                   'prefect_subflow_args': None,
                                   'prefect_task_args': {
                                                         'name':"cnv_calling_nanopore",
                                                         'description': 'Поиск CNV ONT',
                                                         'timeout_seconds': CNV_CALLING_TIMEOUT,
                                                         'tags': ['nanopore', 'cnv_calling', 'cpu', 'nextflow', 'long']                                                        
                                                        },
                                    'prefect_tag_limit':{
                                                         'nanopore_cnv_calling_cpu': {'cpu':CPUS_CNV_CALLING},
                                                         'nanopore_cnv_calling_gpu': {'gpu':None},
                                                         'nanopore_cnv_calling_ram': {'ram':None},
                                                        },
                                    'handler': 'src/tasks/cnv_calling.py:cnv_calling',
                                    'arg_factory': 'src/tasks/cnv_calling.py:cnv_calling_arg_factory'
                                  },
                      'cnv_calling_no_subflow':{
                                   'args':{'threads_per_cnv_calling':THREADS_PER_CNV_CALLING},
                                   'prefect_subflow_args': None,
                                   'prefect_task_args': {
                                                         'name':"cnv_calling_no_subflow_nanopore",
                                                         'description': 'Поиск CNV ONT без subflow',
                                                         'timeout_seconds': CNV_CALLING_TIMEOUT,
                                                         'tags': ['nanopore', 'cnv_calling', 'cpu', 'nextflow', 'long']                                                        
                                                        },
                                    'prefect_tag_limit':{
                                                         'nanopore_cnv_calling_cpu': {'cpu':CPUS_CNV_CALLING},
                                                         'nanopore_cnv_calling_gpu': {'gpu':None},
                                                         'nanopore_cnv_calling_ram': {'ram':None},
                                                        },
                                    'handler': 'src/tasks/cnv_calling_no_subflow.py:cnv_calling_no_subflow',
                                    'arg_factory': 'src/tasks/cnv_calling_no_subflow.py:cnv_calling_no_subflow_arg_factory'
                                  },
                      'blank':{
                                   'args':{'threads_per_cnv_calling':THREADS_PER_CNV_CALLING},
                                   'prefect_subflow_args': None,
                                   'prefect_task_args': None,
                                    'prefect_tag_limit':{
                                                         
                                                        },
                                    'handler': 'src/tasks/blank.py:blank',
                                    'arg_factory': 'src/tasks/blank.py:blank_arg_factory'
                                  }
                     }
# Финальный цикл обновления конфигурации стадий
STAGE_DEPENDENCIES = form_stage_data(
                                     template=PRE_STAGE_DEPENDIES,
                                     subflow_args=DEFAULT_SUBFLOW_ARGS,
                                     task_args=DEFAULT_TASK_ARGS,
                                     active_tasks=active_tasks
                                    )

