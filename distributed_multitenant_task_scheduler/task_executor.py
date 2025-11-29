import json
import config
from typing import Dict
import pymysql
from pymysql.cursors import DictCursor
from confluent_kafka import Consumer as KafkaConsumer
from scheduler_orchestrator import ShardManager
import kazoo.client

class TaskExecutor:
    def __init__(self, priority: str):
        super().__init__()
        self.priority = priority
        self.consumer = KafkaConsumer(
            {
                "bootstrap.servers": config.KAFKA_BROKER,
                "group.id": f"executor-{priority}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self.consumer.subscribe([config.PRIORITY_TOPICS[priority]])
        self.shard_manager = ShardManager()

        self.zk = kazoo.client.KazooClient(hosts=config.ZOOKEEPER_CONN)
        self.zk.start()
        self.zk.create(f"{config.EXECUTOR_PATH}/{priority}/active/{}", b"some data", ephemeral=True)

    def run(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            task = json.loads(msg.value().decode())
            self.execute_task(task)
            self.consumer.commit(message=msg)

            tenant_id = task['tenant_id']
            shard_uri = self.shard_manager.get_shard_uri(tenant_id)
            host, port, user, password, db = shard_uri.split(":")
            conn = pymysql.connect(
                host=host,
                port=int(port),
                user=user,
                password=password,
                database=db,
                cursorclass=DictCursor,
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE tasks SET status='Completed' WHERE id=%s",
                        (task["task_id"],),
                    )
                conn.commit()
            finally:
                conn.close()

    def execute_task(self, task: Dict):
        print(f"Executing {task['task_id']} for tenant {task['tenant_id']}")


