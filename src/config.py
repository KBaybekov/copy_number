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
# Простые константы для теста
THREADS_PER_ALIGNMENT = 4
ALIGNMENT_TIMEOUT = 60

# Мок-стадии (вместо реального alignment)
STAGE_DEPENDENCIES = {
    'stage1': {
        'args': {},
        'prefect_task_args': {
            'description': 'Test stage 1',
            'timeout': ALIGNMENT_TIMEOUT,
            'retries': 1,
            'tags': ['test'],
        },
        'handler': alignment,   # используем тот же handler, который мы переопределим в tasks/alignment.py
        'arg_factory': alignment_arg_factory,
    },
    'stage2': {
        'args': {},
        'prefect_task_args': {
            'description': 'Test stage 2',
            'timeout': ALIGNMENT_TIMEOUT,
            'retries': 1,
            'tags': ['test'],
        },
        'handler': alignment,   # переопределим в tasks/alignment.py
        'arg_factory': alignment_arg_factory
    },
}