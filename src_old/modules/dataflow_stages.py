from classes.sample import Sample
from modules.logger import get_logger
from file_format_handlers.tsv_handler import form_nxf_tsv
from file_format_handlers.nxf_cfg_handler import form_nxf_cfg
import subprocess
from threading import Event
import time
from typing import Callable, Dict, List, Optional, Tuple
from pathlib import Path
from config import (
                    BASECALLING_CMD_FAST5,
                    BASECALLING_CMD_POD5,
                    BASECALLED_DATA_THRESHOLD,
                    ALIGN_FQ_CMD,
                    ALIGNMENT_TIMEOUT
                   )

logger = get_logger(__name__)

def form_cmd(cmd_template: list[str], data2replace: dict) -> list:
    formed_cmd = []
    for word in cmd_template:
        for template_mask, value in data2replace.items():
            if isinstance(value, Path):
                word = word.replace(template_mask, value.as_posix())
            elif isinstance(value, str):
                word = word.replace(template_mask, value)
            elif isinstance(value, (int, float)):
                word = word.replace(template_mask, str(value))
        formed_cmd.append(word)

    return formed_cmd

def run_subprocess_with_stop(
                             cmd: list,
                             stop_event: Event,
                             cwd:Path,
                             stage_name:str,
                             timeout: Optional[float] = None,
                             **kwargs
                            ) -> Tuple[bool, str]:
    """
    Запускает subprocess, прерывается при stop_event.set().
    Реагирует на stop_event быстрее, чем за 1 сек.
    """
    cwd.mkdir(parents=True, exist_ok=True)
    out_file = cwd / f"{stage_name}.out"
    err_file = cwd / f"{stage_name}.err"
    with open(out_file, "w") as f_out, open(err_file, "w") as f_err:
        proc = subprocess.Popen(
                                cmd,
                                encoding="utf-8",
                                cwd=cwd,
                                stdout=f_out,
                                stderr=f_err,
                                **kwargs
                               )
    if timeout:
        start_time = time.time()

    while proc.poll() is None:
        # Проверяем stop_event быстро
        if stop_event.is_set():
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            #return subprocess.CompletedProcess(cmd, returncode=-1)
            return (False, 'Process terminated by keyboard interrupt')

        if timeout:
            if (time.time() - start_time) > timeout: # type: ignore
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                #return subprocess.CompletedProcess(cmd, returncode=-2)
                return (False, 'Process terminated due timeout')

        # Короткий sleep, чтобы не грузить CPU
        if stop_event.wait(timeout=0.1):  # ← 100 мс вместо 1 сек
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            #return subprocess.CompletedProcess(cmd, returncode=-1)
            return (False, 'Process terminated by keyboard interrupt')

    result = subprocess.CompletedProcess(cmd, proc.returncode)
    if result.returncode == 0:
        #return subprocess.CompletedProcess(cmd, proc.returncode)
        return (True, '')
    elif result.returncode == 127:
        return (False, 'Command not found')
    else:
        return (False, f'Processing failed, exitcode: {result.returncode}')

'''
def batch_basecalling_no_nxf(sample: Sample, stop_event: Event, gpu_id: int) -> Sample:
    """!!! добавить оценку размера бейзколленных данных в конце
    !!! запуск процесса - через run_subprocess_with_stop"""
    """
    cmd = ["ping", "8.8.8.8"]  # бесконечный ping
stop_event = threading.Event()

result = run_subprocess_with_stop(cmd, stop_event, text=True, capture_output=True)
    """
    if stop_event.is_set():
        logger.debug("Basecalling прерван до начала.")
        return sample
    batch_dir = Path(".")
    try:
        print(1)
        print(sample.unbasecalled_batches, sample.id)
        batch_dir = sample.unbasecalled_batches.pop()
        print(batch_dir.resolve(), [x for x in batch_dir.iterdir()])
    except IndexError:
        print(-1)
    finally:
        if batch_dir == Path("."):
            logger.error(f"Sample: {sample.id}: no batches for basecalling. Skipping")
            sample.log_sample_data(
                "basecalling: fail", False, "Нет батчей для бейсколлинга"
            )
            return sample

    sample.log_sample_data(f"basecalling {batch_dir.name}: start", True)
    # basecall batch
    try:
        print(2)
        src_dir = next(
            (
                x
                for x in batch_dir.iterdir()
                if all(
                    [
                        x.is_dir(),
                        x.name in ["fast5", "fast5_pass", "pod5", "pod5_pass"],
                        x.stat().st_size > 0,
                    ]
                )
            ),
            Path(),
        )
    except StopIteration:
        print(-2)
        logger.error(
            f"NOT FOUND DIR WITH SRC FILES. sample: {sample.id}, batch_dir: {batch_dir}"
        )
        sample.log_sample_data(
            f"basecalling {batch_dir.name}: fail",
            False,
            f"НЕ НАЙДЕНА ДИРЕКТОРИЯ С ИСХОДНЫМИ ДАННЫМИ, батч: {batch_dir}",
        )
        sample.basecalled_batches.add(batch_dir)
        return sample
    if src_dir:
        print(src_dir.name)
        print(3)
        bc_cmd = []
        if "fast5" in src_dir.name:
            bc_cmd.extend(BASECALLING_CMD_FAST5)
        elif "pod5" in src_dir.name:
            bc_cmd.extend(BASECALLING_CMD_POD5)
        else:
            print("error!!!")
        print("!!!", bc_cmd)
        fq_dir = (sample.res_folder / "basecalling" / batch_dir.name).resolve()
        fastq_path = (fq_dir / f"{fq_dir.name}.fastq").resolve()
        fq_dir.mkdir(parents=True, exist_ok=True)
        for i, word in enumerate(bc_cmd):
            bc_cmd[i] = word.replace("GPU_ID", str(gpu_id)).replace(
                "SRC_DIR", src_dir.as_posix()
            )
        with (
            open(fastq_path, "w") as fq,
            open((fq_dir / "basecalling.err"), "w") as ferr,
        ):
            logger.debug(f"Sample {sample.id}. Запуск dorado: {' '.join(bc_cmd)}")
            result = run_subprocess_with_stop(
                cmd=bc_cmd,
                stop_event=stop_event,
                text=True,
                cwd=fq_dir,
                stdout=fq,
                stderr=ferr,
            )
        status_msg = ""
        fail_reason = ""
        is_ok = False
        if result.returncode == 0:
            is_ok = True
            status_msg = f"basecalling batch {batch_dir.name}: success"
            sample.fq_folders.add(fq_dir)
            sample.basecalled_batches.add(batch_dir)
            sample.fq_pass_size_Gb += fastq_path.stat().st_size / 1024**3
            if sample.fq_pass_size_Gb >= BASECALLED_DATA_THRESHOLD:
                status_msg = "basecalling: success"
                sample.enough_basecalled_data = True
            sample.log_sample_data(status_msg, True)
        else:
            status_msg = f"basecalling batch {batch_dir.name}: fail"
            fail_reason = f"dorado failed with code {result.returncode}"
            if all(
                [
                    not sample.fq_pass_size_Gb > BASECALLED_DATA_THRESHOLD,
                    not sample.unbasecalled_batches,
                ]
            ):
                status_msg = "basecalling: fail"
            sample.unbasecalled_batches.add(batch_dir)  # вернуть в очередь?
        sample.log_sample_data(status_msg, is_ok, fail_reason)
    # add batch to basecalled if basecalling is successful
    return sample
'''

def run_stage(
              sample: Sample,
              stage_name: str,
              stop_event: Event,
              **kwargs
             ) -> Sample:
    stage_status = "FAIL"
    processing_succesful = False
    stage = STAGE_REGISTRY.get(stage_name)
    if stage is None:
        logger.error(
                     f"Stage '{stage_name}' not found in STAGE_REGISTRY. Skipping."
                    )
    else:
        sample_fits = STAGE_CONDITIONS.get(stage_name, lambda _: False)
        while sample.success:
            if sample_fits(sample):
                sample, processing_succesful = stage_wrapper(
                                                         sample=sample,
                                                         stage=stage,
                                                         stop_event=stop_event,
                                                         **kwargs
                                                         )
            else:
                logger.info(f"Sample {sample.id} doesn't meet conditions for {stage_name}")
                break
        else:
            logger.error(f'Sample {sample.id} is broken during "{stage_name}"')
        logger.debug(f'Sample {sample.id}: processed stage "{stage_name}"')
        if processing_succesful:
            stage_status = "OK"
        with sample._lock:
            sample.stage_statuses[stage_name] = stage_status
    return sample

def stage_wrapper(
                  sample: Sample,
                  stage: Callable[..., Tuple[Sample, bool]],
                  stop_event: Event,
                  **kwargs
                 ) -> Tuple[Sample, bool]:
    """
    Обертка для этапов обработки, формирующая вспомогательные папки и переменные
    
    :param sample: Описание
    :type sample: Sample
    :param stage: Описание
    :type stage: Callable
    :param stop_event: Описание
    :type stop_event: Event
    :param kwargs: Описание
    :return: Описание
    :rtype: Tuple[Sample, bool]
    """
    is_processing_ok = False
    if stop_event.is_set():
        logger.debug(f"Sample {sample.id}: stage '{stage.__name__}' interrupted before beginning")
    else: 
        if all([
                sample.res_folder is not None,
                sample.work_folder is not None
               ]):
            stage_dirs = []
            for d in [sample.work_folder, sample.res_folder]:
                f = d / stage.__name__ # type: ignore
                f.mkdir(parents=True, exist_ok=True)
                stage_dirs.append(f)
            sample, is_processing_ok = stage(
                                             sample=sample,
                                             stage_dirs=stage_dirs,
                                             stop_event=stop_event,
                                             **kwargs
                                             )
        else:
            sample.fail(
                        stage_name=stage.__name__,
                        reason="Not set: res_folder/work_folder"
                       )
    return (sample, is_processing_ok)

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

"""
def _align_batch(
    sample: Sample, stop_event: Event, threads_per_alignment: int
) -> Sample:
    def update_bam_stats(sample: Sample, bam_dir: Path):
        bam_stat_f = next(
            (x for x in bam_dir.iterdir() if x.name.endswith(".readstats.tsv.gz")), None
        )
        if bam_stat_f is None:
            logger.error(f"Sample {sample.id}: not found bamstat file in {bam_dir}")
            return sample
        else:
            if bam_stat_f.stat().st_size > 0:
                pass
            else:
                logger.error(f"Sample {sample.id}: bamstat file is empty: {bam_stat_f}")

    stage = "alignment"
    is_ok = False

    if stop_event.is_set():
        logger.debug("Выравнивание прервано до начала.")
        return sample
    # Поиск fq-папки, не участвовавшей в выравнивании, по названию батча, к которому она относится (он закодирован в названии .bam)
    fq_dir = next(
        (
            x
            for x in sample.fq_folders
            if not any(x.parents[0].name in s.name for s in sample.bams)
        ),
        None,
    )
    if fq_dir is not None:
        bam_dir = sample.res_folder / "alignment" / fq_dir.parents[0].name
        cmd_data = {
            "FQ_DIR": fq_dir,
            "BAM_OUT_DIR": bam_dir,
            "PREFIX": f"{sample.id}_{bam_dir.name}",
            "ALIGNMENT_THREADS": threads_per_alignment,
            "SAMPLE_WORK_DIR": sample.work_folder / "alignment",
        }
        nxf_cfg = form_nxf_cfg(
            stage=stage,
            data=cmd_data,
            output_filepath=bam_dir
            / f"nxf_alignment_{sample.id}_{bam_dir.name}.config",
        )
        if nxf_cfg is not None:
            cmd_data = {"NXF_CFG": nxf_cfg, "RES_D_LOG": (bam_dir / "nxf.log")}
            al_cmd = form_cmd(ALIGN_FQ_CMD.copy(), cmd_data)
        else:
            reason = f"cmd for alignment of batch {bam_dir.name} not created."
            logger.error(
                f"Sample {sample.id}, batch {fq_dir.parents[0].name}: {stage} failed ({reason})"
            )
            sample.log_sample_data(f"{stage}: fail", is_ok, fail_reason=reason)
            return sample

    else:
        reason = "not found appropriate fq_dir for alignment."
        logger.error(f"Sample {sample.id}: {stage} failed ({reason})")
        sample.log_sample_data(f"{stage}: fail", is_ok, fail_reason=reason)
        return sample

    logger.debug(f"Sample {sample.id}. Запуск выравнивания: {' '.join(al_cmd)}")
    logger.info(f"Sample {sample.id}: Запуск выравнивания для батча {bam_dir.name}")
    with (
        open((bam_dir / "alignment.out"), "w") as f_out,
        open((bam_dir / "alignment.err"), "w") as f_err,
    ):
        result = run_subprocess_with_stop(
            cmd=al_cmd,
            stop_event=stop_event,
            text=True,
            cwd=bam_dir,
            stdout=f_out,
            stderr=f_err,
            timeout=ALIGNMENT_TIMEOUT
        )
        status_msg = ""
        fail_reason = ""
        is_ok = False
        if result.returncode == 0:
            bam = next((x for x in bam_dir.iterdir() if x.suffix == ".bam"), None)
            if bam is not None:
                is_ok = True
                status_msg = f"Aligning batch {bam_dir.name}: success"
                logger.info(f"Sample {sample.id}: {status_msg}")
                sample.bams.add(bam)
                # TODO: update_bam_stats(sample, bam_dir)
                status_msg = f"alignment of batch {bam_dir.name}: success"
                sample.log_sample_data(status_msg, is_ok)
                return sample
            else:
                reason = f"Batch {bam_dir.name}: Alignment {bam_dir.name}: finished, but no BAM found."
                logger.error(
                    f"Sample {sample.id}: {stage} of batch {bam_dir.name} failed ({reason})"
                )
                sample.log_sample_data(
                    f"{stage} of batch {bam_dir.name}: fail", is_ok, fail_reason
                )
                return sample
            
        # timeout
        elif result.returncode == -2:
            reason = f"{bam_dir.name}: Process interrupted due timeout."
            logger.error(
                        f"Sample {sample.id}: {stage} failed for batch {reason}"
                        )
            sample.log_sample_data(
                f"{stage} of batch {bam_dir.name}: fail", is_ok, fail_reason
            )
            return sample
        
        else:
            reason = (
                f"Batch {bam_dir.name}: Alignment failed with code {result.returncode}"
            )
            logger.error(
                f"Sample {sample.id}: {stage} of batch {bam_dir.name} failed ({reason})"
            )
            sample.log_sample_data(
                f"{stage} of batch {bam_dir.name}: fail", is_ok, fail_reason
            )
            return sample
"""

def merge_bams(sample: Sample) -> Sample:
    return sample


def qc_bam(sample: Sample) -> Sample:
    return sample


def sv_cnv_call(sample: Sample) -> Sample:
    return sample

STAGE_REGISTRY:Dict[str, Callable[..., Tuple[Sample, bool]]] = {
                                                                'alignment': alignment
                                                               }

# Реестр условий входа для этапов
STAGE_CONDITIONS: Dict[str, Callable[[Sample], bool]] = {
                                                         'alignment': lambda s: all([
                                                                                     len(s.fq_folders) != len(s.bams),
                                                                                     s.stage_statuses.get('basecalling') == "OK"
                                                                                    ])
                                                        }
