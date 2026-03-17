import asyncio
from prefect.client.orchestration import get_client
from prefect.states import Completed

async def monitoring_parent_flow():
    # Проверка наличия активных сабфлоу
    async with get_client() as client:
        # Получаем ID текущего флоу-рана
        from prefect.context import FlowRunContext
        ctx = FlowRunContext.get()
        if ctx is not None:
            flow_run = ctx.flow_run
            if flow_run is not None:
                run_id = str(flow_run.id)
                # Фильтруем дочерние запуски, которые НЕ завершены (Pending, Running, Retrying)
                child_runs = await client.read_flow_runs(
                    flow_run_filter={
                        "parent_flow_run_id": {"any_": [run_id]},
                        "state": {"type": {"not_in": ["COMPLETED", "FAILED", "CANCELLED"]}}
                    }
                )

        if ctx is not None:
        

        if not child_runs:
            logger.info("Активных сабфлоу не обнаружено. Завершаю родительский флоу.")
            return Completed(message="Все сабфлоу завершены, выхожу.")
        
        logger.info(f"Найдено {len(child_runs)} активных сабфлоу.")
