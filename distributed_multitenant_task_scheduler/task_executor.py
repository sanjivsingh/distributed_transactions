import json
from typing import Dict
import pymysql
from pymysql.cursors import DictCursor
from confluent_kafka import Consumer as KafkaConsumer
import kazoo.client
import time

from setup.zookeeper_setup import config as zk_config, constants as zk_constants    
from setup.kafka_setup import config as kafka_config, constants as kafka_constants

from distributed_multitenant_task_scheduler import config
from distributed_multitenant_task_scheduler.shard_manager import ShardManager



from datetime import datetime, timezone

class TaskExecutor:
    def __init__(self, priority: str):
        super().__init__()
        self.priority = priority
        self.consumer = KafkaConsumer(
            {
                "bootstrap.servers": kafka_config.configurations[kafka_constants.KAFKA_BROKER],
                "group.id": f"executor-{priority}",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest",
            }
        )
        self.consumer.subscribe([config.PRIORITY_TOPICS[priority]])
        self.shard_manager = ShardManager()

        self.zk = kazoo.client.KazooClient(hosts=zk_config.configurations[zk_constants.ZOOKEEPER_CONN])
        self.zk.start()
        self.active_path = f"{config.EXECUTOR_PATH}/{priority}/active/{datetime.now(timezone.utc)}"
        self.zk.create(self.active_path, b"some data", ephemeral=True)
        

    def run(self):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            print(f"Polling for tasks : {self.active_path}")
            task = json.loads(msg.value().decode())
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

            print(f"Executor {self.active_path} fetched task {task['task_id']} for tenant {task['tenant_id']}")
            self.update_status(conn, task['task_id'], 'running')
            execution_status = 'done'
            try:
                self.execute_task(task)
            except Exception as exc:
                print(f"Error executing task : priority {self.priority} active_path {self.active_path} : {exc}")
                execution_status = 'failed'
            finally:
                self.consumer.commit(message=msg)

            try:
                self.update_status(conn, task['task_id'], execution_status)
            except Exception as exc:
                print(f"Error update executed task status : priority {self.priority} active_path {self.active_path} : {exc}")    
            finally:
                conn.close()
            
    def update_status(self, conn, task_id, status: str):
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE executable_tasks SET status='{status}' ,  updated_at=UTC_TIMESTAMP() WHERE task_id=%s",
                (task_id,),
            )
            conn.commit()

    def execute_task(self, task: Dict):
        print(f"Executing {task['task_id']} for tenant {task['tenant_id']} ...{task['payload']}")


