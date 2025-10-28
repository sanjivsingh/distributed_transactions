import threading
import asyncio
import time
from abc import ABC

class InMemoryCounter(ABC):

    def __init__(self) -> None:
        super().__init__()
        self.global_lock = threading.RLock()
        self.key_locks  = {}
        self.counter = {}

    def get(self, key : str) -> int:
        return self.counter.get(key, 0)

    def incr(self, key :str) -> int:
        lock = self._get_or_create_lock(key)
        with lock:
            if key not in self.counter:
                self.counter[key] = 0
            self.counter[key]  +=1
            return self.counter[key]

    def _get_or_create_lock(self, key: str) -> threading.RLock:
        with self.global_lock:
            if key not in self.key_locks:
                self.key_locks[key] = threading.RLock()
            return self.key_locks[key]

if __name__ == "__main__":

    counter  = InMemoryCounter()

    def sync_incr():
        counter.incr(key='c1')

    threads = []
    for _ in range(1000):
        t = threading.Thread(target=sync_incr)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    c1_count = counter.get('c1')
    print(c1_count)
    assert c1_count == 1000

    async def async_incr():
        await asyncio.to_thread(counter.incr, key='c2')

    async def main():
        tasks = [async_incr() for _ in range(1000)]
        await asyncio.gather(*tasks)

    asyncio.run(main())

    c2_count = counter.get('c2')
    print(c2_count)
    assert c2_count == 1000
