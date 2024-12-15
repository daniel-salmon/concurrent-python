import logging
import sys
from functools import wraps
from pathlib import Path
from queue import Queue
from random import randint
from threading import Thread
from time import sleep
from typing import Any, Callable

import requests

NUM_WORKERS = 5
NUM_REQUESTS = 1_000
URL = "https://httpbin.org"


logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s %(asctime)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

logger.info(f"GIL enabled?: {sys._is_gil_enabled()}")  # type: ignore


class ExceededRetriesError(Exception):
    pass


def retry_with_backoff(attempts: int, duration_seconds: int = 1, multiplier: int = 2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal duration_seconds
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    logger.exception(
                        f"Error occurred during attempt {attempt}/{attempts} of running function '{func.__name__}' on {(args, kwargs)}"
                    )
                sleep(duration_seconds)
                duration_seconds *= multiplier
            raise ExceededRetriesError(
                f"Failed after {attempts} attempts to run function '{func.__name__}' on process {(args, kwargs)}"
            )

        return wrapper

    return decorator


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
    def __init__(
        self,
        worker_num: int,
        func: Callable,
        path: Path,
        queue: ClosableQueue,
        exception_queue: ClosableQueue,
    ):
        super().__init__()
        self.worker_num = worker_num
        self.func = func
        self.path = path
        self.queue = queue
        self.exception_queue = exception_queue

    def run(self):
        counter = 0
        with open(self.path, "w", encoding="utf-8") as f:
            for args, kwargs in self.queue:
                try:
                    result = self.func(*args, **kwargs)
                except Exception:
                    exception_queue.put((args, kwargs))
                    logger.exception("Error processing {(arg, kwarg)}")
                    continue
                f.write(f"{result}\n")
                f.flush()
                if (counter := counter + 1) % 50 == 0:
                    logger.info(f"Worker {self.worker_num} processed {counter} items")


@retry_with_backoff(attempts=3)
def work(url: str, params: dict[str, Any]) -> str:
    response = requests.get(url, params=params)
    response.raise_for_status()
    result = response.json()["args"]["output"]
    return str(result)


queue = ClosableQueue()
exception_queue = ClosableQueue()
path = Path("output/http-requests")
path.mkdir(parents=True, exist_ok=True)
workers = []
for i in range(NUM_WORKERS):
    worker_path = path / f"{i}"
    worker_path.touch()
    worker = Worker(
        worker_num=i,
        func=work,
        path=worker_path,
        queue=queue,
        exception_queue=exception_queue,
    )
    worker.start()
    workers.append(worker)

for i in range(NUM_REQUESTS):
    url = f"{URL}/get"
    params = {"output": str(randint(0, i))}
    item = ((url,), {"params": params})
    queue.put(item)

for _ in workers:
    queue.close()

queue.join()

for worker in workers:
    worker.join()

exception_queue.close()
for item in exception_queue:
    logger.info(f"Could not process {item}")
exception_queue.join()

logger.info("Done")
