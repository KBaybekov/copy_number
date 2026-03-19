from classes.sample import Sample
from format_handlers.tsv_handler import form_nxf_tsv
from typing import List, Optional, Tuple
from pathlib import Path
from modules.prefect import prepare_variable, run_nextflow_pipeline
from modules.logger import get_logger

logger = get_logger()

def alignment(
              sample: Sample,
              stage_dirs: List[Path],
              threads_per_alignment: int
             ) -> Tuple[Sample, bool]:
    def define_src_dir_n_batch_name(
                                    sample:Sample
                                   ) -> Tuple[Optional[Path], Optional[str]]:
        # Поиск fq-папки, не участвовавшей в выравнивании, по названию батча, к которому она относится (он закодирован в названии .bam)
        for fq_dir in sample.fq_folders:
            if fq_dir.name == 'fastq_pass':
                batch_name = fq_dir.parents[0].name
            else:
                batch_name = fq_dir.name
            if not any(batch_name in s.name for s in sample.bams):
                return (fq_dir, batch_name)
        return (None, None)

    nxf_asset = "epi2me-labs/wf-alignment"

    is_processing_ok = False
    main_work_d, main_res_d = stage_dirs

    # Поиск fq-папки, не участвовавшей в выравнивании, по названию батча, к которому она относится (он закодирован в названии .bam)
    fq_dir, batch_name = define_src_dir_n_batch_name(sample)
    if fq_dir is not None and batch_name is not None:
        stage_name = f"alignment_{batch_name}"
        bam_dir = main_res_d / batch_name
        work_dir = main_work_d / batch_name
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
                return (sample, is_processing_ok)
            
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
                with sample._lock:
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
    return (sample, is_processing_ok)
