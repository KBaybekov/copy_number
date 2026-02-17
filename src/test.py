import os
import sys
import subprocess
from pathlib import Path
from prefect import flow, get_run_logger

@flow(name="File-Access-Check", log_prints=True)
async def check_file():
    logger = get_run_logger()
    # Тот самый путь из ошибки
    target_file = "/mnt/cephfs8_rw/nanopore2/service/code/github/neurology/cyp2d6/result/CYP2D6_samples.tsv"
    path_obj = Path(target_file)

    logger.info(f"Проверка файла: {target_file}")
    logger.info(f"Существует ли файл? {path_obj.exists()}")
    
    # Проверяем цепочку папок по пути
    current = Path("/")
    for part in path_obj.parts[1:-1]:
        current = current / part
        exists = current.exists()
        logger.info(f"Директория {current}: {'ОК' if exists else 'ОТСУТСТВУЕТ'}")
        if exists:
            # Проверяем, что внутри (может там пусто из-за маунта?)
            logger.info(f"  Содержимое {current.name}: {os.listdir(current)[:3]}...")

@flow(name="Infra-Inspector", log_prints=True)
async def inspect_infra():
    """
    Скрипт для глубокого аудита окружения внутри Docker-контейнера.
    """
    logger = get_run_logger()
    logger.info("--- НАЧАЛО ИНСПЕКЦИИ ИНФРАСТРУКТУРЫ ---")

    # 1. Проверка текущего процесса и путей
    logger.info(f"CWD: {os.getcwd()}")
    logger.info(f"PYTHONPATH: {os.environ.get('PYTHONPATH', 'NOT SET')}")
    logger.info(f"Executable: {sys.executable}")

    # 2. Проверка монтирования папки /app
    app_path = Path("/app")
    if app_path.exists():
        logger.info(f"Папка /app найдена. Содержимое: {os.listdir('/app')}")
        # Проверяем вложенность (есть ли там модули)
        for item in app_path.iterdir():
            if item.is_dir():
                logger.info(f"  [DIR] {item.name}: {os.listdir(item)[:5]}...")
    else:
        logger.error("КРИТИЧЕСКАЯ ОШИБКА: Папка /app отсутствует в контейнере!")

    # 3. Проверка системной папки Prefect
    opt_prefect = Path("/opt/prefect")
    if opt_prefect.exists():
        logger.info(f"Содержимое /opt/prefect: {os.listdir('/opt/prefect')}")
    
    # 4. Проверка прав доступа (от кого запущен процесс)
    whoami = subprocess.check_output(["whoami"]).decode().strip()
    logger.info(f"Контейнер запущен от пользователя: {whoami}")

    logger.info("--- КОНЕЦ ИНСПЕКЦИИ ---")

if __name__ == "__main__":
    # Вычисляем пути для деплоя
    current_script = Path(__file__).resolve()
    # Корень проекта (поднимаемся из src/)
    project_root = current_script.parents[1].resolve().as_posix()
    
    
"""
    check_file.deploy(
        name="Infra-Inspector-Deploy",
        work_pool_name="nanopore_docker_pool",
        work_queue_name='cpu_nodes',
        image="prefecthq/prefect:3.6.16-python3.13",
        build=False,
        push=False,
        job_variables={
            "command": f"/bin/bash -c 'cat entrypoint.sh'",
                       "working_dir": "/mnt/cephfs8_rw/nanopore2/service/code/github/neurology/cyp2d6"
                       
        }
    )
"""
"""
"command": f"/bin/bash -c 'echo $PWD'",
                       "working_dir": "/mnt/cephfs8_rw/nanopore2/service/code/github/neurology/cyp2d6"
f"python {__file__}"
,
        job_variables={
            "volumes": [
                f"{current_script.as_posix()}:/opt/prefect/test.py",
                f"{project_root}:/app",
                "/mnt/cephfs8_rw/nanopore2/service/code:/mnt/cephfs8_rw/nanopore2/service/code"
            ],
            "working_dir": "/opt/prefect",
            "env": {
                "PYTHONPATH": "/app"
            }
        }
"""
