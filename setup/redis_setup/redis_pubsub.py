import redis
import threading
import time

# Sample Redis PubSub program
# Assumes Redis is running locally on default port
from commons import logger

log = logger.setup_logger(__name__)

ORDER_CHANNEL = "order_channel"


def connect():
    try:
        from setup.redis_setup import config as redis_config, constants as redis_constants
        return redis.Redis(
            host=redis_config.configurations[redis_constants.REDIS_SERVER],
            port=redis_config.configurations[redis_constants.REDIS_PORT],
            db=0,
        )
    except RuntimeError as e:
        raise RuntimeError("redis connection failed", e)


class OrderEvents:

    def __init__(self) -> None:
        self.redis = connect()

    def __del__(self):
        self.close()

    def close(self):
        if self.redis:
            self.redis.close()

    def publisher(self):
        for i in range(100):
            message = f"Order {i+1}"
            try:
                self.redis.publish(ORDER_CHANNEL, message)
                log.info(f"Published: {message}")
            except RuntimeError as e:
                log.error("redis publish msg failed", e)
            time.sleep(1)


class OrderEmailNotification:

    def __init__(self) -> None:
        self.redis = connect()

    def __del__(self):
        self.close()

    def close(self):
        try:
            if self.redis:
                self.redis.close()
        except RuntimeError as e:
            log.error("redis connection close failed", e)

    def subscriber(self):
        p = self.redis.pubsub()
        p.subscribe(ORDER_CHANNEL)
        log.info(f"Subscribed to {ORDER_CHANNEL}")
        for message in p.listen():
            if message["type"] == "message":
                log.info(
                    f"Email Notification for msg sent : {message['data'].decode('utf-8')}"
                )


class OrderTextMsgNotification:

    def __init__(self) -> None:
        self.redis = connect()

    def __del__(self):
        self.close()

    def close(self):
        if self.redis:
            self.redis.close()

    def subscriber(self):
        p = self.redis.pubsub()
        p.subscribe(ORDER_CHANNEL)
        log.info(f"Subscribed to {ORDER_CHANNEL}")
        for message in p.listen():
            if message["type"] == "message":
                log.info(
                    f"TextMsg Notification for msg sent : {message['data'].decode('utf-8')}"
                )


if __name__ == "__main__":
    # Run subscriber for email
    email = OrderEmailNotification()
    sub_thread1 = threading.Thread(target=email.subscriber)
    sub_thread1.start()

    # Run subscriber for textMsg
    textMsg = OrderTextMsgNotification()
    sub_thread2 = threading.Thread(target=textMsg.subscriber)
    sub_thread2.start()

    # Wait a bit for subscriber to start
    time.sleep(1)

    # Publish messages
    orders = OrderEvents()
    orders.publisher()
    orders.close()

    # Wait for subscriber to process
    time.sleep(10)

    log.info("Done")
