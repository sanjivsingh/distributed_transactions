import time
import threading
from commons import logger

# Setup logger
logger = logger.setup_logger(__name__)

class SnowflakeIDGenerator:
    """
    Generates unique, sortable 64-bit integer IDs.

    The ID is composed of:
    - 41 bits for a timestamp (milliseconds since a custom epoch)
    - 10 bits for a worker ID (0-1023)
    - 12 bits for a sequence number (0-4095)
    """

    def __init__(self, worker_id):
        # A custom epoch to prevent the 41-bit timestamp from overflowing too soon.
        # This is the timestamp of the first second of 2020 UTC.
        self.custom_epoch = 1577836800000

        # Bit masks and shifts for ID components
        self.worker_id_bits = 10
        self.sequence_bits = 12

        self.max_worker_id = (1 << self.worker_id_bits) - 1
        self.max_sequence = (1 << self.sequence_bits) - 1

        self.worker_id_shift = self.sequence_bits
        self.timestamp_shift = self.worker_id_bits + self.sequence_bits

        # Validate the worker ID
        if worker_id < 0 or worker_id > self.max_worker_id:
            raise ValueError(f"Worker ID must be between 0 and {self.max_worker_id}")
        self.worker_id = worker_id

        # Internal state
        self.last_timestamp = -1
        self.sequence = 0
        
        # Add a lock to ensure thread safety
        self.lock = threading.Lock()

    def generate_id(self) -> int:
        """
        Generates and returns a new 64-bit Snowflake ID.
        This method is thread-safe due to the use of a lock.
        """
        # Acquire the lock to ensure atomic execution
        with self.lock:
            current_timestamp = int(time.time() * 1000)

            # Case 1: The system clock moved backward.
            if current_timestamp < self.last_timestamp:
                raise RuntimeError("Clock moved backward, refusing to generate ID.")

            # Case 2: IDs are being generated in the same millisecond.
            if current_timestamp == self.last_timestamp:
                self.sequence = (self.sequence + 1) & self.max_sequence

                # If the sequence number overflows, wait for the next millisecond.
                if self.sequence == 0:
                    while current_timestamp <= self.last_timestamp:
                        current_timestamp = int(time.time() * 1000)
            
            # Case 3: A new millisecond has started.
            else:
                self.sequence = 0
                self.last_timestamp = current_timestamp

            # Combine all parts using bitwise operations
            timestamp_part = (current_timestamp - self.custom_epoch) << self.timestamp_shift
            worker_part = self.worker_id << self.worker_id_shift
            sequence_part = self.sequence

            return timestamp_part | worker_part | sequence_part

# Example Usage with multiple threads
if __name__ == "__main__":
    from concurrent.futures import ThreadPoolExecutor

    def generate_and_collect_ids(generator, num_ids):
        return [generator.generate_id() for _ in range(num_ids)]

    try:
        generator = SnowflakeIDGenerator(worker_id=1)
        num_threads = 5
        ids_per_thread = 100

        logger.info(f"Generating {num_threads * ids_per_thread} IDs with {num_threads} threads...")

        all_ids = []
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(generate_and_collect_ids, generator, ids_per_thread) for _ in range(num_threads)]
            for future in futures:
                all_ids.extend(future.result())

        # Check for uniqueness
        print(f"\nTotal IDs generated: {len(all_ids)}")
        print(f"Total unique IDs: {len(set(all_ids))}")

        # If these two numbers are the same, the IDs are unique.
        if len(all_ids) == len(set(all_ids)):
            print("\nSUCCESS: All generated IDs are unique.")
        else:
            print("\nFAILURE: Duplicate IDs were found.")

    except ValueError as e:
        print(f"Error: {e}")
    except RuntimeError as e:
        print(f"Error: {e}")
