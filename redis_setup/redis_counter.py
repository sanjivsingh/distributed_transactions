import redis
from concurrent.futures import ThreadPoolExecutor, wait
import time
def connect():
    try:
        from redis_setup import config as redis_config, constants as redis_constants
        return redis.Redis(
            host=redis_config.configurations[redis_constants.REDIS_SERVER],
            port=redis_config.configurations[redis_constants.REDIS_PORT],
            db=0,
        )
    except RuntimeError as e:
        raise RuntimeError("redis connection failed", e)



def increment(connection):
    connection.incr("order_count")

def parallel_increment(number_of_threads: int):
    connection = None
    try:

        start_time = time.time()
        connection = connect()
        connection.set('order_count', 0)
        with ThreadPoolExecutor(max_workers=number_of_threads) as executor:
            futures = [executor.submit(increment, connection) for _ in range(100000)]
            wait(futures)

        actual_count = connection.get('order_count')
        if actual_count:
            assert int(actual_count.decode('utf-8')) == 100000

        end_time = time.time()
        print(f"Total time taken: {end_time - start_time} seconds : with {number_of_threads} threads")

    except RuntimeError as e:
        print(f"redis close failed : {e}")
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    parallel_increment(1)
    parallel_increment(2)
    parallel_increment(5)
    parallel_increment(10)
    parallel_increment(12)
    parallel_increment(20)
    parallel_increment(50)
    parallel_increment(100)
    parallel_increment(200)
    parallel_increment(500)
    parallel_increment(1000)
    parallel_increment(2000)


