from asyncio import create_task as create_atask
from pysam import VariantFile, VariantRecord
from typing import Any, Dict, List, Tuple
from pathlib import Path

from prefect import task

from classes.sample import Sample, compute_diff
from modules.logger import get_logger
from modules.prefect import create_prefect_run_name

async def phased_cnv_arg_factory(
                          sample: Sample,
                          parent_flow_id:str,
                          stage_dirs: List[Path],
                          chr: str,
                          pos_start: int,
                          pos_end: int,
                          vicinity: int # Какое расстояние взять по сторонам от области интереса
                         ) -> Dict[str, Dict[str, Any]]:
    """
    Генерация наборов аргументов для параллельных задач экстракции CNV, относящихся к заданному региону.
    Добавление в набор аргументов обязательных stage_dirs.
    Ключ к набору аргументов - произвольный и уникальный task_name
    """
    async def extract_id_from_vcf(vcf_file:Path) -> str:
        return VariantFile(vcf_file.as_posix()).header.samples[0]
    # Проверяем, подходит ли вообще sample для коллинга CNV (ex-STAGE_CONDITIONS)
    if any([
            not sample.cnv,
            sample.stage_statuses.get('phased_cnv') == "OK"
           ]):
        return {}
    # Формируем наборы аргументов
    arg_sets = {}
    print(f"cnvs: {'\n\t'.join([c.as_posix() for c in sample.cnv])}") # type: ignore
    for cnv in sample.cnv:
        cnv_id = await extract_id_from_vcf(cnv)
        #cnv_id = cnv.name.removesuffix(''.join(cnv.suffixes)).removeprefix(f"{sample.id}_").removeprefix('basecalling-')
        if cnv_id not in sample.cnv_copies:
            # Пробуем найти SV & SNP
            task_name = await create_prefect_run_name(
                                                type='Task',
                                                name=f"Phased CNV extraction: batch {cnv_id}",
                                                parent_id=parent_flow_id,
                                                sample_id=sample.id
                                               )
            arg_sets.update({task_name: {
                                        'stage_dirs': stage_dirs,
                                        'cnv_id': cnv_id,
                                        'cnv': cnv,
                                        'chr': chr,
                                        'pos_start': pos_start,
                                        'pos_end': pos_end,
                                        'vicinity': vicinity
                                        }})
    return arg_sets

@task
async def phased_cnv_no_subflow(
                sample: Sample,
                stage_dirs: List[Path],
                cnv_id: str,
                cnv: Path,
                chr: str,
                pos_start: int,
                pos_end: int,
                vicinity: int,
                **subflow_params
               ) -> Tuple[Dict[str, Dict[str, Any]], bool]:
    
    def cnvs_not_found(
                       is_processing_ok:bool,
                       is_sample_ok:bool,
                       sample:Sample
                      ) -> Tuple[bool, bool, Sample]:
        logger.info(f"CNV не обнаружены для '{cnv_id}'")
        is_processing_ok = True
        is_sample_ok = True
        sample.cnv_copies[cnv_id] = "2"
        return is_processing_ok, is_sample_ok, sample
    
    def get_cn_from_cnv(cnvs_w_overlap:List[Tuple[VariantRecord, int]]) -> Tuple[VariantRecord|None, int|str|None]:
        best_cnv = None
        cn:int|str|None = None
        while all([
                   best_cnv is None,
                   cnvs_w_overlap
                  ]):
            cnv = cnvs_w_overlap.pop(0)[0]
            cn:int|str|None = cnv.info.get("CN")
            if cn is not None:
                best_cnv = cnv
        return best_cnv, cn
    
    async def extract_region_related_variants(
                                              vcf_file:Path,
                                              chr:str,
                                              pos_start:int,
                                              pos_end:int
                                             ) -> List[VariantRecord]:
        related_variants = []
        vcf = VariantFile(vcf_file.as_posix())
        for rec in vcf.fetch(
                             contig=chr,
                             start=pos_start,
                             end=pos_end
                            ):
            related_variants.append(rec)
        return related_variants

    print("Initializing phased CNV extraction")
    diffs = {}

    logger = await get_logger()
    print("Logger Initialized")
    
    is_processing_ok = False
    is_sample_ok = sample.success
    fail_reason = ""    

    try:
        stage_name = "phased_cnv"

        # Сохраняем исходное состояние экземпляра
        old_sample = sample.copy()
        
        main_work_d, main_res_d = stage_dirs

        match (cnv, cnv_id):
            case (None, _):
                fail_reason = "Не указан .cnv.vcf"
                is_sample_ok = False
                logger.error(fail_reason)
                sample.fail(stage_name=stage_name, reason="not any vcfs found")
            case (_, None):
                fail_reason = "Не указан ID .cnv.vcf"
                is_sample_ok = False
                logger.error(fail_reason)
                sample.fail(stage_name=stage_name, reason="not any vcfs found")
            case (Path(), str()):
                # Подготовка данных
                work_dir = main_work_d / cnv_id
                res_dir = main_res_d / cnv_id
                for d in [work_dir, res_dir]:
                    d.mkdir(mode=755, exist_ok=True, parents=True)
                
                #region = f"{chr}:{pos_start}-{pos_end}"
                start_w_vicinity = pos_start - vicinity
                end_w_vicinity = pos_end + vicinity
                coords_w_vicinity = {
                          'chr':chr,
                          'pos_start':start_w_vicinity,
                          'pos_end':end_w_vicinity
                         }
                cnvs = await extract_region_related_variants(cnv, **coords_w_vicinity)
                # Если отсутствуют CNV - завершаем
                if not cnvs:
                    is_processing_ok, is_sample_ok, sample = cnvs_not_found(
                                                                            is_processing_ok,
                                                                            is_sample_ok,
                                                                            sample
                                                                           )
                else:
                    # Ищем CNV, который больше других покрывает регион
                    cnvs_w_overlap:List[Tuple[VariantRecord, int]] = []
                    for rec in cnvs:
                        end = rec.info.get("END")
                        if end is not None:
                            max_start = max(start_w_vicinity, pos_start)
                            min_end = min(end_w_vicinity, pos_end)
                            # проверяем, пересекается ли CNV с регионом интереса
                            is_overlap = max_start <= min_end
                            if is_overlap:
                                overlap = max_start + min_end
                                cnvs_w_overlap.append((rec, overlap))
                    if not cnvs_w_overlap:
                        is_processing_ok, is_sample_ok, sample = cnvs_not_found(
                                                                                is_processing_ok,
                                                                                is_sample_ok,
                                                                                sample
                                                                                )
                    else:
                        cnvs_w_overlap = sorted(cnvs_w_overlap, key=lambda x: x[1], reverse=True)
                        cnv_rec, cn = get_cn_from_cnv(cnvs_w_overlap)
                        if cnv_rec is None or cn is None:
                            fail_reason = f"Not found any records with CN in '{cnv.as_posix()}'"
                            logger.error(fail_reason)
                            is_processing_ok = False
                            is_sample_ok = True
                        else:
                            # записываем полученный CN
                            sample.cnv_copies[cnv_id] = str(cn)

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
