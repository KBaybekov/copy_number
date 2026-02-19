import asyncio
from threading import Event
from typing import Dict, List, Optional, Set
from classes.sample import Sample
#from modules.dataflow_stages import batch_basecalling_no_nxf, align_batch, merge_bams, qc_bam, sv_cnv_call
from modules.dataflow_stages import run_stage, STAGE_CONDITIONS
from modules.logger import get_logger
from config import AVAILABLE_GPU_IDS, STAGE_DEPENDENCIES

logger = get_logger()

# id для процессов на GPU
id_lock = asyncio.Lock()
used_ids: Set[int] = set()

async def get_id() -> Optional[int]:
    """
    Функция для получения id свободной gpu
    
    :return: id свободной GPU, если таковая в наличии; иначе None 
    :rtype: int | None
    """
    async with id_lock:
        for i in AVAILABLE_GPU_IDS:
            if i not in used_ids:
                used_ids.add(i)
                return i
    return None

async def release_id(id_: int):
    async with id_lock:
        used_ids.discard(id_)

async def run_stage_with_semaphore(semaphore:Optional[asyncio.Semaphore], **kwargs):
    if semaphore is None:
        return await asyncio.to_thread(run_stage, **kwargs)
    else:
        async with semaphore: # Очередь здесь, но она не блокирует основной цикл
            return await asyncio.to_thread(run_stage, **kwargs)

async def run_universal_flow(sample:Sample, stop_event:Event) -> None:
    """
    Основной цикл обработки образца
    """
    # Список запущенных в данный момент задач
    running_stages:Dict[str, asyncio.Task] = {}
    try:
        logger.debug(f"Sample {sample.id}: start processing")
        # Копия списка стадий, которые нам в принципе нужно выполнить
        pending_stages = list(STAGE_DEPENDENCIES.keys())
        # main flow
        while (pending_stages or running_stages) and sample.success:
            if stop_event.is_set():
                logger.warning(f"Sample {sample.id} interrupted by stop_event")
                sample.log_sample_data(
                                       stage_name='Main_flow',
                                       sample_ok=True,
                                       fail_reason='Interrupted by user'
                                      )
                return None
            # 1. Проверяем, какие стадии можно запустить
            for stage_name in list(pending_stages):
                sample_suitable_for_stage = STAGE_CONDITIONS.get(stage_name, lambda _: False)
                if sample_suitable_for_stage(sample) and stage_name not in running_stages:
                    logger.debug(f"Sample {sample.id}: creating task for {stage_name}")
                    stage_args = STAGE_DEPENDENCIES.get(stage_name, {}).get('args', {})
                    semaphore = STAGE_DEPENDENCIES.get(stage_name, {}).get('semaphore')
                    
                    # Запускаем стадию в отдельном потоке, чтобы не блокировать цикл
                    task = asyncio.create_task(run_stage_with_semaphore(
                                                                        semaphore,
                                                                        sample=sample,
                                                                        stage_name=stage_name,
                                                                        stop_event=stop_event,
                                                                        **stage_args
                                                                       ))
 
                    running_stages[stage_name] = task
            
            if not running_stages:
                # Если ничего не запущено и ничего не готово к запуску — ждем или выходим
                await asyncio.sleep(1) 
                if all(not STAGE_CONDITIONS.get(s, lambda _: False)(sample) for s in pending_stages):
                    break
                continue

            # 2. Ждем завершения любой из запущенных задач
            done, _ = await asyncio.wait(
                                         running_stages.values(), 
                                         return_when=asyncio.FIRST_COMPLETED
                                        )

            for task in done:
                # Находим имя стадии по завершенной задаче
                stage_name = next(
                                  k for k, v in running_stages.items()
                                  if v == task
                                 )
                sample_suitable_for_stage = STAGE_CONDITIONS.get(stage_name, lambda _: False)
                try:
                    sample = await task  # Получаем обновленный sample
                    logger.info(f"Stage {stage_name} finished for {sample.id}")
                except Exception as e:
                    logger.error(f"Critical error in stage {stage_name}: {e}")
                    sample.success = False
                finally:
                    del running_stages[stage_name]
                    if sample.success and sample_suitable_for_stage(sample):
                        pending_stages.append(stage_name)

        sample.finished = True
        for task in running_stages.values():
            task.cancel()
        sample.log_sample_data(
                               stage_name='Main_flow',
                               sample_ok=sample.success,
                               fail_reason='End of processing'
                              )
        return None

    except Exception as e:
        logger.exception(f"Internal error: {e}")
        for task in running_stages.values():
            task.cancel()
        sample.log_sample_data(
                               stage_name='Main_flow',
                               sample_ok=True,
                               fail_reason=f'Internal error during processing: {e}'
                              )
        return None

async def async_process_samples(samples: List[Sample], stop_event: Event):
    logger.info("Starting async processing...")
    tasks = []

    try:
        # Создаём задачи
        for s in samples:
            if not s.finished:
                logger.debug(f"Sample {s.id} will be processed")
                task = asyncio.create_task(run_universal_flow(s, stop_event))
                tasks.append(task)
        logger.info(f"Сформированы потоки обработки для {len(tasks)} образцов.")

        # Ждём — как только ОДНА упадёт с исключением — отменяем все
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

        # Проверяем, есть ли упавшие
        for task in done:
            if task.exception():
                logger.error(f"Task failed with exception: {task.exception()}")
                stop_event.set()  # триггерим глобальную остановку

        # Отменяем оставшиеся
        for task in pending:
            task.cancel()

        # Дожидаемся отмены
        await asyncio.gather(*tasks, return_exceptions=True)

    except Exception as e:
        logger.error(f"Critical error in async_process_samples: {e}", exc_info=True)
        stop_event.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    logger.info("Async processing finished.")

"""
async def _run_universal_flow(sample:Sample, stop_event:Event) -> None:
    
    try:
        logger.debug(f"Sample {sample.id}: start processing")
        stages = list(STAGE_DEPENDENCIES.keys())
        # main flow
        if sample.success:
            if stop_event.is_set():
                logger.warning(f"Sample {sample.id} interrupted by stop_event")
                sample.log_sample_data(
                                       stage_name='Main_flow',
                                       sample_ok=True,
                                       fail_reason='Interrupted by user'
                                      )
                return None
            while stages:
                stage_name = stages.pop(0)
                logger.debug
                if stage_name not in sample.processed_tasks:
                    semaphore = STAGE_DEPENDENCIES.get(stage_name, {}).get('semaphore', asyncio.Semaphore(1))
                    stage_args = STAGE_DEPENDENCIES.get(stage_name, {}).get('args', {})
                    async with semaphore:
                        try:
                            sample = await asyncio.to_thread(
                                                             run_stage,
                                                             sample=sample,
                                                             stage_name=stage_name,
                                                             stop_event=stop_event,
                                                             **stage_args
                                                            )
                        except asyncio.CancelledError:
                            logger.debug(f"Sample {sample.id}: stage {stage_name} was interrupted.")
                            raise
                else:
                    logger.debug(f"Sample {sample.id}: pass {stage_name}")
                # ПРОВЕРКА УСПЕХА ПОСЛЕ КАЖДОГО ВЫЗОВА
                if not sample.success:
                    logger.warning(f"Sample {sample.id} failed during processing. Exiting sample main flow...")
                    return None
            
            sample.finished = True
            sample.log_sample_data(
                                   stage_name='Main_flow',
                                   sample_ok=True,
                                   fail_reason='End of processing'
                                  )
            return None
        else:
            logger.warning(f"Sample {sample.id} marked as failed; no processing")
            return None
                
    except Exception as e:
        sample.log_sample_data(
                               stage_name='Main_flow',
                               sample_ok=True,
                               fail_reason=f'Internal error during processing: {e}'
                              )
        return None

async def _run_universal_flow(sample:Sample, stop_event:Event) -> None:
    try:
        # main flow
        while sample.success:
            logger.debug(f"Sample {sample.id} will be processed")
            if stop_event.is_set():
                logger.debug(f"Sample {sample.id} interrupted by stop_event")
                sample.log_sample_data(
                                       stage_name='Main_flow',
                                       sample_ok=True,
                                       fail_reason='Interrupted by user')
                return None
            # === 1. Basecalling ===
            while not sample.enough_basecalled_data:
                logger.info(f"Sample {sample.id} now in basecalling stage")
                # have any unbasecalled batches?
                if sample.unbasecalled_batches:
                    async with basecall_semaphore:
                        gpu_id = await get_id()
                        if gpu_id is None:
                            logger.warning(f"No free GPU ID for basecalling sample {sample.id}. Retrying...")
                            await asyncio.sleep(10)
                            continue
                        try:
                            print(0)
                            #sample = run_with_stop_event(batch_basecalling_no_nxf, stop_event, sample, gpu_id, stop_event)
                            sample = await asyncio.to_thread(
                                                            batch_basecalling_no_nxf,
                                                            sample,
                                                            gpu_id,
                                                            stop_event
                                                        )
                        except asyncio.CancelledError:
                            logger.debug(f"Basecalling for sample {sample.id} was cancelled.")
                            raise  # проброс CancelledError дальше
                        finally:
                            await release_id(gpu_id) # гарантируем возврат ID
                # no unbasecalled batches == sample failed
                else:
                    fail_msg = f"Sample {sample.id} doesn't have enough basecalled data. Skipping"
                    logger.warning(fail_msg)
                    return await finalize_flow(sample, 'Failed', fail_msg)
            
            # === 2. Alignment ===
            while len(sample.fq_folders) != len(sample.bams):
                logger.debug(f"Sample {sample.id} now in aligment stage")
                logger.debug(f"Sample {sample.id}: fq: {len(sample.fq_folders)}, bams: {len(sample.bams)}")
                async with align_semaphore:
                    try:
                    #sample = run_with_stop_event(align_batch, stop_event, sample)
                        sample = await asyncio.to_thread(
                                                        align_batch,
                                                        sample,
                                                        stop_event,
                                                        THREADS_PER_ALIGNMENT
                                                        )
                    except asyncio.CancelledError:
                        logger.debug(f"Aligning for sample {sample.id} was cancelled.")
                        raise
                # ПРОВЕРКА УСПЕХА ПОСЛЕ КАЖДОГО ВЫЗОВА
                if not sample.success:
                    return await finalize_flow(sample, 'Failed', sample.fail_reason)


            sample.finished = True
            sample.log_sample_data(
                                   stage_name='Main_flow',
                                   sample_ok=True,
                                   fail_reason='End of processing'
                                  )
            return None
        else:
            # Предполагается, что при sample.success = False где-то на обработке
            # метаданные фейла будут записаны сразу 
            return None
                
    except Exception as e:
        sample.log_sample_data(
                               stage_name='Main_flow',
                               sample_ok=True,
                               fail_reason=f'Internal error during processing: {e}'
                              )
        return None
"""
