from classes.sample import Sample
from time import sleep
from typing import Any, Awaitable, Callable, Dict, List, cast, Coroutine, Tuple, TypeAlias


from prefect import task


async def blank_arg_factory(
                          sample: Sample,
                          flow_id: str,
                          **kwargs
                         ) -> Dict[str, Dict[str, Any]]:
    """
    Генерация наборов аргументов для параллельных задач коллинга CNV.
    Добавление в набор аргументов обязательных stage_dirs.
    Ключ к набору аргументов - произвольный и уникальный task_name
    """
    
    arg_sets = {f'task_1_{sample.id}':{'x':1}, f'task_2_{sample.id}':{'x':2}}
    
    return arg_sets

@task
def blank(
                x: int,
                **subflow_params
               ) -> None:
    print('start')
    sleep(60)
    print(f"x = {x}")
    