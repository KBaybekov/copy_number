from classes.sample import Sample, compute_diff
from typing import Any, Dict, List, Tuple
from pathlib import Path
from modules.logger import get_logger
from prefect import task

from modules.prefect import get_result_from_subflow

logger = get_logger()

def alignment_arg_factory(
                          sample: Sample,
                          stage_dirs: List[Path],
                          threads_per_alignment: int
                         ) -> Dict[str, Dict[str, Any]]:
    """
    Генерация наборов аргументов для параллельных задач выравнивания.
    Добавление в набор аргументов обязательных stage_dirs.
    Ключ к набору аргументов - произвольный и уникальный task_name
    """
    # Проверяем, подходит ли вообще sample для выравнивания (ex-STAGE_CONDITIONS)
    if not all([
                len(sample.fq_folders) != len(sample.bams),
                sample.stage_statuses.get('basecalling') == "OK"
               ]):
        return {}
    # Формируем наборы аргументов
    arg_sets = {}
    for fq_dir in sample.fq_folders:
        if fq_dir.name == 'fastq_pass':
            batch_name = fq_dir.parents[0].name
        else:
            batch_name = fq_dir.name
        if not any(batch_name in s.name for s in sample.bams):
            task_name = f"Alignment ([{sample.id}] - [{batch_name}])"
            arg_sets.update({task_name: {
                                         'stage_dirs': stage_dirs,
                                         'threads_per_alignment':threads_per_alignment,
                                         'fq_dir': fq_dir,
                                         'batch_name': batch_name
                                        }})
    return arg_sets

@task
def alignment(
              sample: Sample,
              stage_dirs: List[Path],
              threads_per_alignment: int,
              fq_dir: Path,
              batch_name: str,
              **subflow_params
             ) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    pipeline = "epi2me-labs/wf-alignment"
    cfg_template = "nxf_cfg_alignment_v1"

    # Сохраняем исходное состояние экземпляра
    old_sample = sample.copy()
    is_processing_ok = False
    main_work_d, main_res_d = stage_dirs

    if fq_dir is not None and batch_name is not None:
        # Подготовка данных
        stage_name = f"alignment_{batch_name}"
        bam_dir = main_res_d / batch_name
        work_dir = main_work_d / batch_name
        for d in [bam_dir, work_dir]:
            d.mkdir(mode=755, exist_ok=True, parents=True)
        bam_id = f"{sample.id}_{batch_name}"
        cfg_file = bam_dir / f"nxf_alignment_{bam_id}.config"
        cfg_data = {
                    "fq_dir": fq_dir,
                    "bam_out_dir": bam_dir,
                    "prefix": f"{sample.id}_",
                    "alignment_threads": threads_per_alignment,
                    "sample_work_dir": work_dir,
                    # служебные данные для запуска деплоя Nextflow
                    "cfg_file": cfg_file,
                    "cfg_template": cfg_template,
                    "shell_working_dir": work_dir
                   }
        run_parameters = {
                          "pipeline":pipeline,
                          "log": work_dir / "nxf.log",
                          "configuration_parameters":cfg_data
                         }

        # Запуск пайплайна Nextflow и получение результата
        is_processing_ok, fail_desc = get_result_from_subflow(
                                                              deployment_name="nextflow_pipeline_cpu",
                                                              run_parameters=run_parameters,
                                                              subflow_parameters=subflow_params
                                                            )
        # Проверка результатов
        if is_processing_ok:
            bam = next((x for x in bam_dir.iterdir() if x.suffix == ".bam"), None)
            if bam is not None:
                # SUCCESS
                sample.bams.add(bam)
                logger.info(f"Sample {sample.id}: Alignment batch {batch_name}: success")
                sample.log_sample_data(stage_name=stage_name, sample_ok=True)
            else:
                reason = f"Batch {bam_dir.name}: Alignment {bam_dir.name}: finished successfully, but no BAM found."
                sample.fail(
                            stage_name=stage_name,
                            reason=reason
                           )
        else:
            sample.log_sample_data(
                                   stage_name=stage_name,
                                   sample_ok=True,
                                   critical_error=False,
                                   fail_reason=fail_desc
                                  )        
    else:
        sample.fail(stage_name='alignment_common', reason="not found appropriate fq_dir for alignment, but it has to be")
    # Формирование словаря изменений образца
    diffs = compute_diff(old_sample, sample)
    return (diffs, is_processing_ok)
