from classes.sample import Sample, compute_diff
from typing import Any, Dict, List, Tuple
from pathlib import Path
from modules.prefect import prepare_variable, run_nextflow_pipeline
from modules.logger import get_logger
from prefect import task

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

@task(name="alignment_nanopore", description="Выравнивание .fastq файлов ONT", )
def alignment(
              sample: Sample,
              stage_dirs: List[Path],
              threads_per_alignment: int,
              fq_dir: Path,
              batch_name: str
             ) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    nxf_asset = "epi2me-labs/wf-alignment"

    # Сохраняем исходное состояние экземпляра
    old_sample = sample.copy()
    is_processing_ok = False
    main_work_d, main_res_d = stage_dirs

    if fq_dir is not None and batch_name is not None:
        stage_name = f"alignment_{batch_name}"
        bam_dir = main_res_d / batch_name
        work_dir = main_work_d / batch_name
        for d in [bam_dir, work_dir]:
            d.mkdir(mode=755, exist_ok=True, parents=True)
        bam_id = f"{sample.id}_{batch_name}"
        cfg_data = {
                    "fq_dir": fq_dir,
                    "bam_out_dir": bam_dir,
                    "prefix": f"{sample.id}_",
                    "alignment_threads": threads_per_alignment,
                    "sample_work_dir": work_dir
                   }
        cfg_file = bam_dir / f"nxf_alignment_{bam_id}.config"
        # Формирование конфига для Nextflow
        with open(cfg_file, 'w') as f:
            config = prepare_variable(
                                      variable_name='nxf_cfg_alignment_v1',
                                      data=cfg_data
                                     )
            if config is not None:
                f.write(config)
            else:
                reason = f"Nextflow config for alignment of batch {batch_name} not created"
                sample.log_sample_data(
                                       stage_name=stage_name,
                                       sample_ok=False,
                                       critical_error=False,
                                       fail_reason=reason
                                      )
                diffs = compute_diff(old_sample, sample)
                return (diffs, is_processing_ok)
            
        # Формирование команды для запуска Nextflow
        log_path = work_dir / "nxf.log"
        shell_data = {'commands': {
                                   'log_path': log_path.as_posix(),
                                   'pipeline_path': nxf_asset,
                                   'nxf_config': cfg_file.as_posix(),
                                   'profile': 'docker'
                                  },
                      'working_dir': work_dir
                     }
        # Выполнение команды
        is_processing_ok, fail_desc = run_nextflow_pipeline(operation_data=shell_data)
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
    diffs = compute_diff(old_sample, sample)
    return (diffs, is_processing_ok)
