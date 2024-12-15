from pathlib import Path
from queue import Queue
from random import randint
from threading import Thread
from typing import Any, Callable

import requests

NUM_WORKERS = 5
URL = "https://httpbin.org"


class ClosableQueue(Queue):
    SENTINEL = object()

    def close(self):
        self.put(self.SENTINEL)

    def __iter__(self):
        while True:
            item = self.get()
            try:
                if item is self.SENTINEL:
                    return
                yield item
            finally:
                self.task_done()


class Worker(Thread):
    def __init__(self, func: Callable, path: Path, queue: ClosableQueue):
        super().__init__()
        self.func = func
        self.path = path
        self.queue = queue

    def run(self):
        with open(self.path, "w", encoding="utf-8") as f:
            for args, kwargs in self.queue:
                result = self.func(*args, **kwargs)
                f.write(f"{result}\n")
                f.flush()


def work(url: str, params: dict[str, Any]) -> str:
    response = requests.get(url, params=params)
    response.raise_for_status()
    result = response.json()["args"]["output"]
    return str(result)


queue = ClosableQueue()
path = Path("output/http-requests")
path.mkdir(parents=True, exist_ok=True)
workers = []
for i in range(NUM_WORKERS):
    worker_path = path / f"{i}"
    worker_path.touch()
    worker = Worker(func=work, path=worker_path, queue=queue)
    worker.start()
    workers.append(worker)

for i in range(1000):
    url = f"{URL}/get"
    params = {"output": str(randint(0, i))}
    item = ((url,), {"params": params})
    queue.put(item)

for _ in workers:
    queue.close()

queue.join()

for worker in workers:
    worker.join()

print("done")
