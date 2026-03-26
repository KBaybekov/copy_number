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
DEFAULT_SUBFLOW_ARGS = {
                        'as_subflow': True,
                        'tags':['nanopore', 'cyp2d6_cnv']
                       }

DEFAULT_TASK_ARGS = {
                     'cache_policy': NotSet(),
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
                    }


# ИЗМЕНИТЬ ПРИ ИЗМЕНЕНИИ СПИСКОВ ЗАДАЧ
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
                                    'handler': alignment,
                                    'arg_factory': alignment_arg_factory
                                  }
                     }


"""for stage, stage_options in STAGE_DEPENDENCIES.items():
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
  """         
"""
for stage, stage_options in PRE_STAGE_DEPENDIES.items():
    for arg_type in stage_options.keys():
        match arg_type:
            case 'prefect_subflow_args':
                stage_subflow_args = PRE_STAGE_DEPENDIES[stage][arg_type]
                match stage_subflow_args:
                  case None | {}:
                    PRE_STAGE_DEPENDIES[stage].update({arg_type:DEFAULT_SUBFLOW_ARGS})
                  case dict():
                      new_args = DEFAULT_SUBFLOW_ARGS.copy()
                      new_args.update(stage_subflow_args)
                      if 'tags' not in new_args.keys():
                         new_args['tags'] = []
                      new_args['tags'].extend(DEFAULT_SUBFLOW_ARGS.get('tags', []))
                      new_args['tags'].extend([tag for tag, val in PRE_STAGE_DEPENDIES[stage]['prefect_tag_limit'].items() if val is not None])
                      PRE_STAGE_DEPENDIES[stage].update({arg_type:new_args})
                      
            case 'prefect_task_args':
                stage_task_args = PRE_STAGE_DEPENDIES[stage][arg_type]
                match stage_task_args:
                  case None | {}:
                    PRE_STAGE_DEPENDIES[stage].update({arg_type:DEFAULT_TASK_ARGS})
                  case dict():
                      new_args = DEFAULT_TASK_ARGS.copy()
                      new_args.update(stage_task_args)
                      if 'tags' not in new_args.keys():
                         new_args['tags'] = []
                      new_args['tags'].extend(stage_task_args.get('tags', []))
                      new_args['tags'].extend(list(PRE_STAGE_DEPENDIES[stage]['prefect_tag_limit'].keys()))
                      new_args['tags'] = list(set(new_args['tags']))
                      PRE_STAGE_DEPENDIES[stage].update({arg_type:new_args})
            case _:
              continue
"""
# Финальный цикл обновления конфигурации стадий
STAGE_DEPENDENCIES = {}
for stage_name, stage_opts in PRE_STAGE_DEPENDIES.items():
    # Копируем все поля, которые не требуют специальной обработки
    new_stage = stage_opts.copy()

    # --- Обработка аргументов для подпотоков (subflow) ---
    subflow_args = stage_opts.get('prefect_subflow_args')
    if subflow_args is None:
        new_stage['prefect_subflow_args'] = DEFAULT_SUBFLOW_ARGS
    else:
        # Базовые аргументы из DEFAULT_SUBFLOW_ARGS
        merged = DEFAULT_SUBFLOW_ARGS.copy()
        # Обновляем явно указанными значениями из стадии (теги обработаем отдельно)
        for key, value in subflow_args.items():
            if key != 'tags':
                merged[key] = value
        # Собираем теги из трёх источников: базовые, из конфига стадии, из prefect_tag_limit
        base_tags = DEFAULT_SUBFLOW_ARGS.get('tags', [])
        stage_tags = subflow_args.get('tags', [])
        limit_tags = list(stage_opts.get('prefect_tag_limit', {}).keys())
        merged['tags'] = list(set(base_tags + stage_tags + limit_tags))
        new_stage['prefect_subflow_args'] = merged

    # --- Обработка аргументов для задач (task) ---
    task_args = stage_opts.get('prefect_task_args')
    if task_args is None:
        new_stage['prefect_task_args'] = DEFAULT_TASK_ARGS
    else:
        merged = DEFAULT_TASK_ARGS.copy()
        for key, value in task_args.items():
            if key != 'tags':
                merged[key] = value
        base_tags = DEFAULT_TASK_ARGS.get('tags', [])
        stage_tags = task_args.get('tags', [])
        limit_tags = list(stage_opts.get('prefect_tag_limit', {}).keys())
        merged['tags'] = list(set(base_tags + stage_tags + limit_tags))
        new_stage['prefect_task_args'] = merged

    STAGE_DEPENDENCIES[stage_name] = new_stage
