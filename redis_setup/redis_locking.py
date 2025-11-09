import time
from redis import Redis
from redis.exceptions import LockError
import redis
import concurrent.futures

redis   = redis.Redis(host='localhost', port=6379, db=0)

def process_critical_section(index :int):
    """
    Acquires a lock named 'resource_lock' with a timeout of 10 seconds.
    The lock automatically expires after 10 seconds to prevent deadlocks.
    """
    lock = redis.lock("resource_lock", timeout=10)
    try:
        # Attempt to acquire the lock, wait for up to 5 seconds if another process holds it
        acquired = lock.acquire(blocking=True, blocking_timeout=5)
        if acquired:
            print("Lock acquired; performing critical operation...{index}".format(index=index))
            time.sleep(3)  # Simulate some operation
            print("Critical operation completed. {index}".format(index))
        else:
            print("Failed to acquire lock within 5 seconds.")
    except LockError:
        print("A LockError occurred, possibly releasing already released lock.")
    finally:
        # Always release the lock in a finally block to ensure cleanup
        lock.release()
        print("Lock released.")

# Usage demonstration
execute_count = concurrent.futures.ThreadPoolExecutor(max_workers=5)
futures = [execute_count.submit(process_critical_section, i) for i in range(5)]
concurrent.futures.wait(futures)

if redis:
    redis.close()   