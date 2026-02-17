from pathlib import Path

def is_file_exists_n_not_empty(file_path: Path) -> bool:
    if file_path.exists():
        return file_path.stat().st_size > 0
    else:
        return False