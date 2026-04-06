from asyncio import create_task as create_atask, gather, as_completed
from typing import Any, Dict, List, Tuple
from pathlib import Path

from prefect import task
from prefect_shell import ShellOperation

from classes.sample import Sample, compute_diff
from modules.logger import get_logger
from flows.nextflow_pipeline_cpu import get_prefect_variable, interpret_exit_code, render_text
from modules.prefect import create_prefect_run_name, get_result_from_subflow

async def cnv_calling_no_subflow_arg_factory(
                          sample: Sample,
                          parent_flow_id:str,
                          stage_dirs: List[Path],
                          threads_per_cnv_calling: int
                         ) -> Dict[str, Dict[str, Any]]:
    """
    Генерация наборов аргументов для параллельных задач коллинга CNV.
    Добавление в набор аргументов обязательных stage_dirs.
    Ключ к набору аргументов - произвольный и уникальный task_name
    """
    async def define_basecalling_model(bam_id:str) -> str:
        """
        Определение модели бейсколлинга по типу исходных данных
        """
        batch_d = next((x for x in sample.basecalled_batches if x.name == bam_id), None)
        if batch_d is not None:
            is_r10 = any('pod5' in x.name for x in batch_d.iterdir())
            if is_r10:
                return "dna_r10.4.1_e8.2_400bps_hac@v5.2.0"
            return  "dna_r9.4.1_e8_hac@v3.3" #"dna_r9.4.1_e8.2_400bps_hac"
        return ""

    # Проверяем, подходит ли вообще sample для коллинга CNV (ex-STAGE_CONDITIONS)
    if not all([
                sample.bams,
                sample.stage_statuses.get('alignment') == "OK",
                sample.stage_statuses.get('cnv_calling', "") != "OK"
               ]):
        return {}
    # Формируем наборы аргументов
    arg_sets = {}
    print(f"basecalled_batches: {sample.basecalled_batches}")
    for bam in sample.bams:
        bam_id = bam.name.removesuffix(''.join(bam.suffixes)).split('-')[-1]
        if bam_id == 'fastq_pass':
            bam_id = bam.name.removesuffix(''.join(bam.suffixes)).split('-')[0].removeprefix(f"{sample.id}_")
        print(f"bam_id: {bam_id}")
        basecalling_model = await define_basecalling_model(bam_id)
        if all([
                basecalling_model,
                not any(bam_id in s.name for s in sample.cnv)
               ]):
            task_name = create_prefect_run_name(
                                                type='Task',
                                                name=f"CNV Calling: batch {bam_id}",
                                                parent_id=parent_flow_id,
                                                sample_id=sample.id
                                               )
            arg_sets.update({task_name: {
                                        'stage_dirs': stage_dirs,
                                        'threads_per_cnv_calling':threads_per_cnv_calling,
                                        'bam': bam,
                                        'bam_id': bam_id,
                                        'basecalling_model': basecalling_model
                                        }})
    return arg_sets

@task
async def cnv_calling_no_subflow(
                sample: Sample,
                stage_dirs: List[Path],
                threads_per_cnv_calling: int,
                bam: Path,
                bam_id: str,
                basecalling_model: str,
                **subflow_params
               ) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    print("Initializing CNV Calling with no subflow")
    diffs = {}

    logger = await get_logger()
    print("Logger Initialized")
    
    is_processing_ok = False
    is_sample_ok = sample.success
    fail_reason = ""    

    try:
        pipeline = "epi2me-labs/wf-human-variation"
        cfg_template = "nxf_cfg_human_cnv_v1"
        stage_name = "cnv_calling"

        # Сохраняем исходное состояние экземпляра
        old_sample = sample.copy()
        
        main_work_d, main_res_d = stage_dirs

        match (bam, bam_id):
            case (None, _):
                fail_reason = "Не указан .BAM"
                is_sample_ok = False
                logger.error("Не указан .BAM")
                sample.fail(stage_name=stage_name, reason="not any bams found")
            case (_, None):
                fail_reason = "Не указан ID .BAM"
                is_sample_ok = False
                logger.error("Не указан ID .BAM")
                sample.fail(stage_name=stage_name, reason="not any bams found")
            case (Path(), str()):
                # Подготовка данных
                work_dir = main_work_d / bam_id
                res_dir = main_res_d / bam_id
                for d in [work_dir, res_dir]:
                    d.mkdir(mode=755, exist_ok=True, parents=True)
                # Копируем BAM-файлы в all_bams
                cfg_file = work_dir / "cnv_calling.config"
                cfg_data = {
                            "bam": bam,
                            "cnv_out_dir": res_dir,
                            "prefix": f"{sample.id}_",
                            "threads_per_cnv_calling": threads_per_cnv_calling,
                            "basecalling_model": basecalling_model,
                            "sample_work_dir": work_dir,
                            "report_file": res_dir / f"{sample.id}_{bam_id}_cnv_calling_report_nxf.html",
                            "trace_file": res_dir / f"{sample.id}_{bam_id}_cnv_calling_trace_nxf.tsv",
                            "timeline_file": res_dir / f"{sample.id}_{bam_id}_cnv_calling_timeline_nxf.html",
                        }

                print("Starting Nexflow subflow...")
                
                # Формируем файл конфигурации
                with open(cfg_file, 'w') as f:
                    config = await render_text(
                                            template=await get_prefect_variable(cfg_template),
                                            data=cfg_data
                                            )
                    f.write(config)

                # Формируем данные для заполнения шаблона
                cmd_data = {
                            "log_path": work_dir / "nxf.log",
                            "pipeline":pipeline,
                            "nxf_cfg": cfg_file
                        }

                # Формируем shell-команду
                nextflow_command = [await render_text(
                                                    template=await get_prefect_variable("nxf_cmd_docker"),
                                                    data=cmd_data
                                                    )]
                # Добавляем подготовительные и постпроцессинговые команды
                nextflow_prep_cmds:List[str] = ['curl -fsSL https://get.nextflow.io | bash && mv nextflow /usr/local/bin/']
                shell_cmds:List[str] = nextflow_command
                
                # Запуск пайплайна Nextflow и получение результата
                async with ShellOperation(
                                        commands=shell_cmds,
                                        env={"NXF_HOME":"/mnt/cephfs8_rw/nanopore2/service/nextflow/"},
                                        working_dir=work_dir,
                                        stream_output=True
                                        ) as shell_op:
                    # Запускаем процесс
                    process = await shell_op.atrigger()
                    # Ждем завершения (заблокирует выполнение потока до конца пайплайна)
                    await process.await_for_completion()
                    return_code:int = process.return_code # type: ignore
                is_processing_ok, fail_desc = await interpret_exit_code(return_code)

                # Проверка результатов
                match is_processing_ok:
                    case False:
                        fail_reason = fail_desc
                    case True:
                        cnv_vcf = next((x for x in res_dir.iterdir() if x.name.endswith('.wf_cnv.vcf.gz')), None)
                        match cnv_vcf:
                            case None:
                                fail_reason = f"CNV calling for {bam_id}: finished successfully, but no VCF found."
                            case Path():
                                # SUCCESS
                                sample.cnv.add(cnv_vcf)
        match (is_processing_ok, is_sample_ok):
            case (True, True):
                logger.info("CNV Calling finished successfully!")
                create_atask(sample.log_sample_data(stage_name=stage_name, sample_ok=True))
            case (_, False):
                logger.error(f"Sample is broken! Reason: {fail_reason}")
                create_atask(sample.fail(
                                         stage_name=stage_name,
                                         reason=fail_reason
                                        ))
            case (False, True):
                logger.error(f"Processing unsuccessful! Reason: {fail_reason}")
                create_atask(sample.log_sample_data(
                                                    stage_name=stage_name,
                                                    sample_ok=True,
                                                    critical_error=False,
                                                    fail_reason=fail_reason
                                                   ))
        # Формирование словаря изменений образца
        diffs = compute_diff(old_sample, sample)
    except Exception as e:
        logger.error(f"Произошло страшное:\n{e}")
        raise e
    return (diffs, is_processing_ok)

'''
def cnv_calling_arg_factory(
                          sample: Sample,
                          parent_flow_id:str,
                          stage_dirs: List[Path],
                          threads_per_cnv_calling: int
                         ) -> Dict[str, Dict[str, Any]]:
    """
    Генерация наборов аргументов для параллельных задач коллинга CNV.
    Добавление в набор аргументов обязательных stage_dirs.
    Ключ к набору аргументов - произвольный и уникальный task_name
    """
    def define_basecalling_model(bam_id:str) -> str:
        """
        Определение модели бейсколлинга по типу исходных данных
        """
        batch_d = next((x for x in sample.basecalled_batches if x.name == bam_id), None)
        if batch_d is not None:
            is_r10 = any('pod5' in x.name for x in batch_d.iterdir())
            if is_r10:
                return "dna_r10.4.1_e8.2_400bps_hac@v5.2.0"
            return  "dna_r9.4.1_e8_hac@v3.3" #"dna_r9.4.1_e8.2_400bps_hac"
        return ""

    # Проверяем, подходит ли вообще sample для коллинга CNV (ex-STAGE_CONDITIONS)
    if not all([
                sample.bams,
                sample.stage_statuses.get('alignment') == "OK",
                sample.stage_statuses.get('cnv_calling', "") != "OK"
               ]):
        return {}
    # Формируем наборы аргументов
    arg_sets = {}
    print(f"basecalled_batches: {sample.basecalled_batches}")
    for bam in sample.bams:
        bam_id = bam.name.removesuffix(''.join(bam.suffixes)).split('-')[-1]
        if bam_id == 'fastq_pass':
            bam_id = bam.name.removesuffix(''.join(bam.suffixes)).split('-')[0].removeprefix(f"{sample.id}_")
        print(f"bam_id: {bam_id}")
        basecalling_model = define_basecalling_model(bam_id)
        if all([
                basecalling_model,
                not any(bam_id in s.name for s in sample.cnv)
               ]):
            task_name = create_prefect_run_name(
                                                type='Task',
                                                name=f"CNV Calling: batch {bam_id}",
                                                parent_id=parent_flow_id,
                                                sample_id=sample.id
                                               )
            arg_sets.update({task_name: {
                                        'stage_dirs': stage_dirs,
                                        'threads_per_cnv_calling':threads_per_cnv_calling,
                                        'bam': bam,
                                        'bam_id': bam_id,
                                        'basecalling_model': basecalling_model
                                        }})
    return arg_sets

@task
def cnv_calling(
                sample: Sample,
                stage_dirs: List[Path],
                threads_per_cnv_calling: int,
                bam: Path,
                bam_id: str,
                basecalling_model: str,
                **subflow_params
               ) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    print("Initializing CNV Calling")
    from asyncio import run as arun
    logger = arun(get_logger())
    print("Logger Initialized")
    is_processing_ok = False
    diffs = {}

    try:
        pipeline = "epi2me-labs/wf-human-variation"
        cfg_template = "nxf_cfg_human_cnv_v1"
        stage_name = "cnv_calling"

        # Сохраняем исходное состояние экземпляра
        old_sample = sample.copy()
        
        main_work_d, main_res_d = stage_dirs

        if bam is not None and bam_id is not None:
            # Подготовка данных
            work_dir = main_work_d / bam_id
            res_dir = main_res_d / bam_id
            for d in [work_dir, res_dir]:
                d.mkdir(mode=755, exist_ok=True, parents=True)
            # Копируем BAM-файлы в all_bams
            cfg_file = work_dir / "cnv_calling.config"
            cfg_data = {
                        "bam": bam,
                        "cnv_out_dir": res_dir,
                        "prefix": f"{sample.id}_",
                        "threads_per_cnv_calling": threads_per_cnv_calling,
                        "basecalling_model": basecalling_model,
                        "sample_work_dir": work_dir,
                        "report_file": res_dir / f"{sample.id}_{bam_id}_cnv_calling_report_nxf.html",
                        "trace_file": res_dir / f"{sample.id}_{bam_id}_cnv_calling_trace_nxf.tsv",
                        "timeline_file": res_dir / f"{sample.id}_{bam_id}_cnv_calling_timeline_nxf.html",
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

            print("Starting Nexflow subflow...")
            # Запуск пайплайна Nextflow и получение результата
            is_processing_ok, fail_desc = get_result_from_subflow(
                                                                        deployment_name="nextflow-pipeline-cpu/nextflow_pipeline_cpu",
                                                                        run_parameters=run_parameters,
                                                                        subflow_parameters=subflow_params
                                                                       )
            # Проверка результатов
            if is_processing_ok:
                cnv_vcf = next((x for x in res_dir.iterdir() if x.name.endswith('.wf_cnv.vcf.gz')), None)
                if cnv_vcf is not None:
                    # SUCCESS
                    sample.cnv.add(cnv_vcf)
                    logger.info(f"Sample {sample.id}: CNV calling of {bam_id}: success")
                    arun(sample.log_sample_data(stage_name=stage_name, sample_ok=True))
                else:
                    reason = f"CNV calling for {bam_id}: finished successfully, but no BAM found."
                    arun(sample.fail(
                                stage_name=stage_name,
                                reason=reason
                            ))
            else:
                arun(sample.log_sample_data(
                                    stage_name=stage_name,
                                    sample_ok=True,
                                    critical_error=False,
                                    fail_reason=fail_desc
                                    ))
        else:
            sample.fail(stage_name=stage_name, reason="not any bams found")
        # Формирование словаря изменений образца
        diffs = compute_diff(old_sample, sample)
    except Exception as e:
        logger.error(f"Произошло страшное:\n{e}")
        raise e
    return (diffs, is_processing_ok)
'''
