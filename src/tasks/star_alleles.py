from asyncio import create_task as create_atask, gather, as_completed
from pysam import VariantFile, VariantRecord

from typing import Any, Dict, List, Tuple
from pathlib import Path

from prefect import task
from prefect_shell import ShellOperation

from classes.sample import Sample, compute_diff
from modules.logger import get_logger
from format_handlers.tsv_handler import extract_value_from_tsv
from flows.nextflow_pipeline_cpu import get_prefect_variable, interpret_exit_code, render_text
from modules.prefect import create_prefect_run_name, get_result_from_subflow

async def star_alleles_arg_factory(
                          sample: Sample,
                          parent_flow_id:str,
                          stage_dirs: List[Path],
                          gene: str,
                          control_gene: str,
                          genome:str,
                          data_type:str
                         ) -> Dict[str, Dict[str, Any]]:
    """
    Генерация наборов аргументов для параллельных задач звёздных аллелей заданного гена.
    Добавление в набор аргументов обязательных stage_dirs.
    Ключ к набору аргументов - произвольный и уникальный task_name
    """
    async def extract_id_from_vcf(vcf_file:Path) -> str:
        return VariantFile(vcf_file.as_posix()).header.samples[0]

    # Проверяем, подходит ли вообще sample для коллинга CNV (ex-STAGE_CONDITIONS)
    if any([
            not sample.cnv,
            sample.stage_statuses.get('star_alleles') == "OK"
           ]):
        return {}
    # Формируем наборы аргументов
    arg_sets = {}
    snp_vcfs = ([
                 next(
                      (f for f in cnv.parent.iterdir() if f.name.endswith('.wf_snp.vcf.gz')),
                      None)
                 for cnv in sample.cnv])
    print(f"snp_vcfs: {'\n\t'.join([c.as_posix() for c in snp_vcfs])}") # type: ignore
    for vcf in snp_vcfs:
        if vcf is None:
            continue
        
        vcf_id = await extract_id_from_vcf(vcf)
        #cnv_id = cnv.name.removesuffix(''.join(cnv.suffixes)).removeprefix(f"{sample.id}_").removeprefix('basecalling-')
        if vcf_id not in sample.star_alleles:
            # Пробуем найти SV & SNP
            task_name = await create_prefect_run_name(
                                                type='Task',
                                                name=f"Star alleles generation: batch {vcf_id}",
                                                parent_id=parent_flow_id,
                                                sample_id=sample.id
                                               )
            arg_sets.update({task_name: {
                                        'stage_dirs': stage_dirs,
                                        'vcf_id': vcf_id,
                                        'vcf': vcf,
                                        'gene': gene,
                                        'control_gene': control_gene,
                                        'genome': genome,
                                        'data_type': data_type
                                        }})
    return arg_sets

@task
async def star_alleles(
                sample: Sample,
                stage_dirs: List[Path],
                vcf: Path,
                vcf_id: str,
                gene: str,
                control_gene: str,
                genome: str,
                data_type: str,
                **subflow_params
               ) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    print("Initializing star alleles generation")
    diffs = {}

    logger = await get_logger()
    print("Logger Initialized")
    
    is_processing_ok = False
    is_sample_ok = sample.success
    fail_reason = ""    

    try:
        stargazer_d = "/mnt/cephfs8_rw/nanopore2/service/programms/stargazer-grc38-2.0.3/stargazer/"
        stage_name = "star_alleles"

        # Сохраняем исходное состояние экземпляра
        old_sample = sample.copy()
        
        main_work_d, main_res_d = stage_dirs

        match (vcf, vcf_id):
            case (None, _):
                fail_reason = "Не указан .VCF"
                is_sample_ok = False
                logger.error("Не указан .VCF")
                sample.fail(stage_name=stage_name, reason="not any VCFs found")
            case (_, None):
                fail_reason = "Не указан ID .VCF"
                is_sample_ok = False
                logger.error("Не указан ID .VCF")
                sample.fail(stage_name=stage_name, reason="not any VCFs found")
            case (Path(), str()):
                # Подготовка данных
                work_dir = main_work_d / vcf_id
                res_dir = main_res_d / vcf_id
                for d in [work_dir, res_dir]:
                    d.mkdir(mode=755, exist_ok=True, parents=True)
                
                print("Starting Stargazer...")
                # Формируем shell-команду
                cmd = [f"python3 {stargazer_d} --vcf-file {vcf}  --target-gene {gene} --genome {genome} --data-type {data_type} --control-gene {control_gene} --output-dir {res_dir}"]
                # Добавляем подготовительные и постпроцессинговые команды
                shell_cmds:List[str] = cmd
                
                # Запуск пайплайна Nextflow и получение результата
                async with ShellOperation(
                                        commands=shell_cmds,
                                        working_dir=res_dir,
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
                        report_tsv = next((x for x in res_dir.iterdir() if x.name == 'report.tsv'), None)
                        match report_tsv:
                            case None:
                                fail_reason = f"Start allele generation for {vcf_id}: finished successfully, but no report.tsv found."
                            case Path():
                                star_allele = extract_value_from_tsv(
                                                                     file_path=report_tsv,
                                                                     row_index=0,
                                                                     col_index=2
                                                                    )
                                if "*" not in star_allele:
                                    is_processing_ok = False
                                    fail_reason = f"Start allele generation for {vcf_id}: finished successfully, but star allele looks strange: ''"
                                else:
                                    sample.star_alleles.update({vcf_id:star_allele})
        match (is_processing_ok, is_sample_ok):
            case (True, True):
                logger.info("Start allele generation finished successfully!")
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
