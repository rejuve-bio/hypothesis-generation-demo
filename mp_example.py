import asyncio
import multiprocessing
import time

def worker(num):
    """Thread worker function"""
    print(f'Worker: {num}')
    time.sleep(2)
    return f'Worker {num} done'

async def async_worker(num):
    loop = asyncio.get_event_loop()
    with multiprocessing.Pool(processes=4) as pool:
        result = await loop.run_in_executor(None, pool.apply, worker, (num,))
    return result

async def main():
    tasks = [async_worker(i) for i in range(4)]
    results = await asyncio.gather(*tasks)
    for result in results:
        print(result)

def run():
    asyncio.run(main())

