from queue import Queue
from threading import Thread
from typing import Callable


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


def work(func: Callable, in_queue: ClosableQueue, out_queue: ClosableQueue):
    for item in in_queue:
        result = func(item)
        out_queue.put(result)


square_queue = ClosableQueue()
cube_queue = ClosableQueue()
done_queue = ClosableQueue()

square_thread = Thread(target=work, args=(lambda x: x**2, square_queue, cube_queue))
cube_thread = Thread(target=work, args=(lambda x: x**3, cube_queue, done_queue))

square_thread.start()
cube_thread.start()

for i in range(100):
    square_queue.put(i)
square_queue.close()
square_queue.join()
cube_queue.close()
cube_queue.join()
square_thread.join()
cube_thread.join()

done_queue.close()
for item in done_queue:
    print(item)
done_queue.join()
