from prefect import flow, task
from prefect.futures import as_completed 
from asyncio import sleep, gather as agather, run as arun
from prefect.utilities.asyncutils import gather

@task
def run_subflow_task(k: str):
    # Вызов subflow внутри задачи гарантирует, что он будет записан как дочерний
    return arun(subflow(k))

@task
async def test_task(k:str, t:int):
    print(f"k: {k}, t: {t}")
    await sleep(t)
    print('done!')


@flow
async def subflow(k:str):
    ts = [30, 35]
    futs = []
    for t in ts:
        fut = test_task.with_options(task_run_name=f"{k}_{t}").submit(k, t)
        futs.append(fut)
    while futs:
        try:
            for fut in as_completed(futs):
                if fut.state.is_final():
                    futs.remove(fut)
        except TimeoutError:
            await sleep(1)
        


@flow(task_runner=T)
async def test_main():
    print("Start main ppl")
    ks = ['f', 'g', 'h']
    # Отправляем задачи-обёртки на выполнение. Они будут выполняться параллельно.
    subflow_futures = [run_subflow_task.submit(k) for k in ks]
    
    # Ожидаем завершения всех и собираем результаты
    results = [fut.result() for fut in subflow_futures]
    print('main - all')

if __name__ == "__main__":
    arun(test_main())