from classes.sample import Sample
from modules.logger import get_logger
from file_format_handlers.tsv_handler import form_nxf_tsv
from file_format_handlers.nxf_cfg_handler import form_nxf_cfg
import subprocess
from threading import Event
import time
from typing import Callable, Dict, List, Optional, Tuple
from pathlib import Path

def alignment(
              sample: Sample,
              stage_dirs: List[Path],
              stop_event: Event,
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

    is_processing_ok = False
    main_work_d, main_res_d = stage_dirs

    # Поиск fq-папки, не участвовавшей в выравнивании, по названию батча, к которому она относится (он закодирован в названии .bam)
    fq_dir, batch_name = define_src_dir_n_batch_name(sample)
    if fq_dir is not None and batch_name is not None:
        stage_name = f"alignment_{batch_name}"
        bam_dir = main_res_d / batch_name
        work_dir = main_work_d / batch_name
        bam_id = f"{sample.id}_{batch_name}"
        cmd_data = {
                    "FQ_DIR": fq_dir,
                    "BAM_OUT_DIR": bam_dir,
                    "PREFIX": f"{sample.id}_",
                    "ALIGNMENT_THREADS": threads_per_alignment,
                    "SAMPLE_WORK_DIR": work_dir
                   }
        nxf_cfg = form_nxf_cfg(
                               stage="alignment",
                               data=cmd_data,
                               output_filepath=bam_dir / f"nxf_alignment_{bam_id}.config"
                              )
        if nxf_cfg is not None:
            cmd_data = {"NXF_CFG": nxf_cfg, "RES_D_LOG": (work_dir / "nxf.log")}
            al_cmd = form_cmd(ALIGN_FQ_CMD.copy(), cmd_data)
            logger.info(f"Sample {sample.id}: Запуск выравнивания для батча {batch_name}")
            processing_ok, fail_desc = run_subprocess_with_stop(
                                                                stage_name=stage_name,
                                                                cmd=al_cmd,
                                                                stop_event=stop_event,
                                                                text=True,
                                                                cwd=work_dir,
                                                                timeout=ALIGNMENT_TIMEOUT
                                                               )
            if processing_ok:
                bam = next((x for x in bam_dir.iterdir() if x.suffix == ".bam"), None)
                if bam is not None:
                    # SUCCESS
                    with sample._lock:
                        sample.bams.add(bam)
                    logger.info(f"Sample {sample.id}: Alignment batch {batch_name}: success")
                    sample.log_sample_data(stage_name=stage_name, sample_ok=True)
                    is_processing_ok = True
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
            reason = f"cmd for alignment of batch {batch_name} not created"
            sample.log_sample_data(
                                   stage_name=stage_name,
                                   sample_ok=False,
                                   critical_error=False,
                                   fail_reason=reason
                                  )
    else:
        sample.fail(stage_name='alignment_common', reason="not found appropriate fq_dir for alignment, but it has to be")
    return (sample, is_processing_ok)
