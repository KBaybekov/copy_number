from __future__ import annotations
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, TypeVar
from threading import RLock
from dataclasses import is_dataclass, fields

T = TypeVar('T')

# ------------------- compute_diff / apply_changes -------------------
def compute_diff(base: T, new: T) -> Dict[str, Dict[str, Any]]:
    protected_fields = {'processed_tasks', 'task_channels', 'task_statuses', 'finished'}
    diff = {}

    def _compute(base_obj: Any, new_obj: Any, field_name: Optional[str] = None) -> Dict[str, Any]:
        if base_obj is None and new_obj is None:
            return {}
        if base_obj is None or new_obj is None:
            return {"set": new_obj}
        if base_obj != new_obj:
            match base_obj:
                case _ if is_dataclass(base_obj):
                    ops = {}
                    for f in fields(base_obj):
                        if field_name is None and f.name in protected_fields:
                            continue
                        base_val = getattr(base_obj, f.name)
                        new_val = getattr(new_obj, f.name)
                        sub_ops = _compute(base_val, new_val, f.name)
                        if sub_ops:
                            ops[f.name] = sub_ops
                    return ops
                case bool() | int() | float() | list() | Path() | str():
                    if field_name == 'success' and base_obj == False:
                        return {}
                    return {"set": new_obj}
                case set():
                    return {'add': new_obj - base_obj, 'remove': base_obj - new_obj}
                case dict():
                    update = {}
                    remove = set()
                    for k, v in new_obj.items():
                        if k not in base_obj or base_obj[k] != v:
                            update[k] = v
                    for k in base_obj:
                        if k not in new_obj:
                            remove.add(k)
                    ops = {}
                    if update:
                        ops["update"] = update
                    if remove:
                        ops["remove"] = remove
                    return ops
                case _:
                    return {"set": new_obj}
        return {}

    if is_dataclass(base) and is_dataclass(new):
        for field in fields(base):
            field_name = field.name
            if field_name in protected_fields:
                continue
            base_val = getattr(base, field_name)
            new_val = getattr(new, field_name)
            field_diff = _compute(base_val, new_val, field_name)
            if field_diff:
                diff[field_name] = field_diff
    return diff

def apply_changes(obj: Any, operations: Dict[str, Dict[str, Any]]) -> None:
    if not operations:
        return
    for field_name, field_ops in operations.items():
        if not hasattr(obj, field_name):
            continue
        current = getattr(obj, field_name)
        if is_dataclass(current) and isinstance(field_ops, dict):
            apply_changes(current, field_ops)
            continue
        match current:
            case set():
                if "add" in field_ops:
                    current.update(field_ops["add"])
                if "remove" in field_ops:
                    current.difference_update(field_ops["remove"])
            case dict():
                if "update" in field_ops:
                    current.update(field_ops["update"])
                if "remove" in field_ops:
                    for k in field_ops["remove"]:
                        current.pop(k, None)
            case _:
                if "set" in field_ops:
                    setattr(obj, field_name, field_ops["set"])

# ------------------- Sample class -------------------
@dataclass(slots=True)
class Sample:
    # Обязательные атрибуты
    id: str
    res_folder: Optional[Path] = field(default=Path())
    work_folder: Optional[Path] = field(default=Path())
    fail_reason: str = field(default="")
    errors: Dict[str, str] = field(default_factory=dict)
    processed_tasks: List[str] = field(default_factory=list)
    task_channels: Dict[str, Dict[str, Dict[str, Any]]] = field(default_factory=dict)
    task_statuses: Dict[str, str] = field(default_factory=dict)
    stage_statuses: Dict[str, str] = field(default_factory=dict)
    status: str = field(default="")
    success: bool = field(default=True)
    finished: bool = field(default=False)
    _lock: RLock = field(default_factory=RLock, repr=False, compare=False)

    # Опциональные атрибуты (минимальный набор для тестов)
    group: str = field(default="")
    subgroup: str = field(default="")
    fq_folders: Set[Path] = field(default_factory=set)
    value1: int = 0
    value2: str = ""
    items: set = field(default_factory=set)

    def to_dict(self) -> Dict[str, Any]:
        # упрощённая версия для тестов
        return {}

    def log_sample_data(self, stage_name: str, sample_ok: bool, critical_error: bool = False, fail_reason: str = "") -> None:
        # заглушка
        pass

    def fail(self, stage_name: str = "Unmarked_stage", reason: str = "") -> None:
        self.success = False
        self.finished = True
        self.log_sample_data(stage_name, False, critical_error=True, fail_reason=reason)

    def copy(self) -> 'Sample':
        return Sample(**asdict(self))