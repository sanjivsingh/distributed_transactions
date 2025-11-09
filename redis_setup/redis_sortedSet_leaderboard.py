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


def update_player_score(connection, player_id):

    while True:
        import random
        connection.zadd("leaderboard", {f"player_{player_id}": random.randint(1,100)})
        time.sleep(1)


from concurrent.futures import ThreadPoolExecutor, wait
if __name__ == "__main__":
    connection = None
    try:

        connection = connect()

        executor = ThreadPoolExecutor(max_workers=10)
        for i in range(10):  # Submit multiple for concurrency
            executor.submit(update_player_score, connection, i)
        executor.shutdown(wait=False) 

        print("Top 10 players:")
        while True:
            top_players = connection.zrange("leaderboard", 0, 9, withscores=True, desc=True)
            for rank, (player, score) in enumerate(top_players, start=1):
                print(f"{rank}. {player.decode('utf-8')} - {int(score)}")
            print("-----------------")
            time.sleep(5)

    except RuntimeError as e:
        print(f"redis close failed : {e}")
    finally:
        if connection:
            connection.close()
