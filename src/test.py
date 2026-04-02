import asyncio

async def fetch_data():
    print("Начинаем загрузку...")
    await asyncio.sleep(5)  # Имитация долгой операции (например, I/O)
    return "Данные загружены"

async def main():
    # Программа ждет здесь 2 секунды, не блокируя всё приложение
    result = await fetch_data()
    print(result)
    print("End")

asyncio.run(main())