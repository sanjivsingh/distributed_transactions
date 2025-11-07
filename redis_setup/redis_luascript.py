import redis
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


if __name__ == "__main__":
    connection = None
    try:

        start_time = time.time()
        connection = connect()


        end_time = time.time()
        print(f"Total time taken: {end_time - start_time} seconds")

    except RuntimeError as e:
        print(f"redis close failed : {e}")
    finally:
        if connection:
            connection.close()
