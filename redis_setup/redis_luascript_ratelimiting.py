import redis
from concurrent.futures import ThreadPoolExecutor, wait
import time

class TotekBucket:
    def __init__(self, limit: int, refill_interval: int):
        self.limit = limit
        self.refill_interval = refill_interval
        self.last_refill = time.time()
  
        from redis_setup import config as redis_config, constants as redis_constants
        self.connection =  redis.Redis(
            host=redis_config.configurations[redis_constants.REDIS_SERVER],
            port=redis_config.configurations[redis_constants.REDIS_PORT],
            db=0,
        )

        # Define the Lua script (as a string)
        lua_script = """
        local current_value = redis.call('GET', KEYS[1])
        local new_value = tonumber(current_value) - tonumber(ARGV[1])
        redis.call('SET', KEYS[1], new_value)
        return new_value
        """
        # Register the script with Redis (optional, but good for performance)
        # This returns a SHA1 hash of the script, which can be used to execute it later.
        self.script_sha = self.connection.script_load(lua_script)

    def get_token(self,key: str) -> int:
        current_time = time.time()
        elapsed = current_time - self.last_refill

        if elapsed > self.refill_interval:
            self.connection.set(f"rate_limit:{key}", self.limit)
            self.last_refill = current_time

        return self.connection.evalsha(self.script_sha, 1, f"rate_limit:{key}", 1)

class FixedWindowRateLimiter:

    def __init__(self, limit=10, window_seconds=60):
        self.limit = limit
        self.window_seconds = window_seconds
        from redis_setup import config as redis_config, constants as redis_constants
        self.redis =  redis.Redis(
            host=redis_config.configurations[redis_constants.REDIS_SERVER],
            port=redis_config.configurations[redis_constants.REDIS_PORT],
            db=0,
        )

    def is_allowed(self, key):
        # Generate a unique key for the current window and client
        # The key should change with each window to ensure proper resetting
        current_window_start = int(time.time() // self.window_seconds) * self.window_seconds
        key = f"rate_limit:{key}:{current_window_start}"

        # Use a Redis pipeline for atomicity
        pipe = self.redis.pipeline()
        pipe.incr(key)  # Increment the counter for the current window
        pipe.expire(key, self.window_seconds) # Set expiration for the key

        # Execute the pipeline and get the result of INCR
        current_count, _ = pipe.execute()

        return current_count <= self.limit

limit  = 100
def order():


    token_bucket = TotekBucket(limit=limit, refill_interval=10)

    while True:
        # Execute the script using EVALSHA (preferred if script is registered)
        token = token_bucket.get_token()
        if token >= 0:
            print("Order placed, remaining tokens:", token - 1)
        else:
            print("No tokens available")
        time.sleep(0.2 + 0.1 * (time.time() % 1))  # Simulate variable order placement time



def token_bucket_rate_limit():
    connection = None
    try:
        with ThreadPoolExecutor() as executor:
            futures = []
            futures.append(executor.submit(order))
            wait(futures)

    except RuntimeError as e:
        print(f"redis close failed : {e}")
    finally:
        if connection:
                connection.close()

if __name__ == "__main__":
    token_bucket_rate_limit()

