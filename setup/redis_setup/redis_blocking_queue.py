import redis
import time
import threading
from setup.redis_setup import config as redis_config, constants as redis_constants

# Redis connection
def get_redis_connection():
    return redis.Redis(
        host=redis_config.configurations[redis_constants.REDIS_SERVER],
        port=redis_config.configurations[redis_constants.REDIS_PORT],
        db=0
    )

# Producer: Adds items to the queue
def producer(queue_name: str, num_items: int):
    r = get_redis_connection()
    for i in range(num_items):
        item = f"item-{i}"
        r.lpush(queue_name, item)  # Push to left (enqueue)
        print(f"Produced: {item}")
        time.sleep(0.1)  # Simulate production delay
    print("Producer finished.")

# Consumer: Blocks and waits for items, with atomic dequeue and failure recovery
def consumer(queue_name: str, index: int, max_retries: int = 3):
    r = get_redis_connection()
    processing_queue = f"{queue_name}_processing"  # Temporary queue for items being processed
    retry_queue = f"{queue_name}_retry"
    
    while True:
        # Atomically move item from main/retry queue to processing queue
        # Use BRPOPLPUSH equivalent: RPOPLPUSH with blocking
        # Since BRPOPLPUSH isn't direct, use a loop or Lua script for atomicity
        item_bytes = r.brpoplpush(queue_name, processing_queue, timeout=10)  # Moves to processing
        if not item_bytes:
            item_bytes = r.brpoplpush(retry_queue, processing_queue, timeout=10)
        if not item_bytes:
            print(f"Consumer {index}: No item received (timeout).")
            break
        
        item = item_bytes.decode('utf-8')
        retry_count = 0
        
        # Extract retry count if present
        if ':retry:' in item:
            parts = item.split(':retry:')
            item = parts[0]
            retry_count = int(parts[1])
        
        try:
            # Simulate processing
            print(f"Consumer {index} processing: {item}")
            if "fail" in item:  # Simulate failure
                raise Exception("Simulated processing failure")
            time.sleep(0.5)
            print(f"Consumer {index} successfully processed: {item}")
            # On success, remove from processing queue
            r.lrem(processing_queue, 1, item_bytes)  # Remove the item
        
        except Exception as e:
            print(f"Consumer {index} failed to process {item}: {e}")
            retry_count += 1
            if retry_count < max_retries:
                # Move back to retry queue
                r.lpush(retry_queue, f"{item}:retry:{retry_count}")
                print(f"Re-queued {item} for retry (attempt {retry_count})")
            else:
                # Move to DLQ
                dlq = f"{queue_name}_dlq"
                r.lpush(dlq, f"{item}:failed")
                print(f"Moved {item} to DLQ after {max_retries} retries")
            # Remove from processing queue (even on failure, to avoid duplicates)
            r.lrem(processing_queue, 1, item_bytes)

if __name__ == "__main__":
    queue_name = "my_blocking_queue"
    
    # Clear queues
    r = get_redis_connection()
    r.delete(queue_name, f"{queue_name}_processing", f"{queue_name}_retry", f"{queue_name}_dlq")
    
    # Start consumers
    consumer_thread1 = threading.Thread(target=consumer, args=(queue_name, 1))
    consumer_thread1.daemon = True
    consumer_thread1.start()
    
    consumer_thread2 = threading.Thread(target=consumer, args=(queue_name, 2))
    consumer_thread2.daemon = True
    consumer_thread2.start()
    
    # Start producer
    producer(queue_name, 10)
    
    # Wait
    time.sleep(20)
    print("Main thread exiting.")