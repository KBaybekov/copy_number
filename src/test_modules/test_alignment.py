from classes.sample import Sample, compute_diff
from typing import Any, Dict, List, Tuple
from pathlib import Path
from prefect import task

def alignment_arg_factory(sample: Sample, stage_dirs: List[Path], **kwargs) -> Dict[str, Dict[str, Any]]:
    """Генерирует тестовые задачи."""
    # Пример: если в sample есть поле value1, используем его для генерации задач
    # Для простоты всегда возвращаем одну задачу
    if sample.value1 > 5:
        return {}
    return {
        "test_task": {
            "stage_dirs": stage_dirs,
            "multiplier": 2,
            "message": "hello"
        }
    }

@task(name="test_stage")
async def alignment(sample: Sample, stage_dirs: List[Path], multiplier: int, message: str) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    """Мок-обработчик, который увеличивает value1 (если есть) и добавляет сообщение в value2."""
    # Создаём копию sample (если у Sample есть поле value1 и value2, иначе используем task_channels)
    new_sample = sample.copy()
    # Для теста предположим, что в sample есть поля value1 и value2 (мы добавим их через наследование в тесте)
    if hasattr(new_sample, 'value1'):
        new_sample.value1 = getattr(sample, 'value1', 0) + multiplier
        new_sample.value2 = getattr(sample, 'value2', '') + message + " "
        new_sample.items.add("stage1_done")
    else:
        # Если нет — используем task_channels для передачи данных на следующую стадию
        new_sample.task_channels.setdefault('stage2', {})['next_task'] = {
            'stage_dirs': stage_dirs,
            'factor': 3
        }
    diff = compute_diff(sample, new_sample)
    return diff, True