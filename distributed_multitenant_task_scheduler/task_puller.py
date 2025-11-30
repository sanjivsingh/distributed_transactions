import asyncio
import json
import time
from typing import Dict
import pymysql
from pymysql.cursors import DictCursor

import kazoo.client
from confluent_kafka import Producer as KafkaProducer

from distributed_multitenant_task_scheduler.shard_manager import ShardManager
from distributed_multitenant_task_scheduler import config

from setup.zookeeper_setup import config as zk_config, constants as zk_constants
from setup.kafka_setup import config as kafka_config, constants as kafka_constants

from datetime import datetime

class TaskPuller:
    def __init__(self, tenant_id: str):
        super().__init__()
        self.tenant_id = tenant_id
        self.zk = kazoo.client.KazooClient(hosts=zk_config.configurations[zk_constants.ZOOKEEPER_CONN])
        self.zk.start()
        self.active_path = f"{config.TENANT_BASE_PATH}/{tenant_id}/active/{datetime.now()}"
        self.zk.create(self.active_path, b"some data", ephemeral=True)

        self.producer = KafkaProducer({"bootstrap.servers": kafka_config.configurations[kafka_constants.KAFKA_BROKER]})
        self.shard_manager = ShardManager()

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.poll_loop())

    async def poll_loop(self):
        while True:
            shard_uri = self.shard_manager.get_shard_uri(self.tenant_id)
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
                        """
                        SELECT task_id, payload, priority FROM executable_tasks
                        WHERE tenant_id=%s AND status='pending' AND schedule_time<=NOW()
                        ORDER BY schedule_time ASC
                        LIMIT 2
                        FOR UPDATE SKIP LOCKED
                        """,
                        (self.tenant_id,),
                    )
                    rows = cur.fetchall()
                    if rows:
                        print(f"Fetched Tasks from database for tasks : {self.active_path}")
                        for row in rows:
                            print(f"Puller {self.active_path} fetched task {row['task_id']} for tenant {self.tenant_id}")
                            topic = config.PRIORITY_TOPICS[row["priority"]]
                            payload = json.dumps(
                                {
                                    "task_id": row["task_id"],
                                    "tenant_id": self.tenant_id,
                                    "payload": json.loads(row["payload"]),
                                }
                            ).encode()
                            try:
                                self.producer.produce(topic, value=payload)
                                self.producer.poll(0)
                                cur.execute(
                                    "UPDATE executable_tasks SET status='queued', updated_at=NOW() WHERE task_id=%s",
                                    (row["task_id"],),
                                )
                            except Exception as exc:
                                print(f"Error producing task to Kafka : {exc}")

                conn.commit()
            except Exception as exc:
                print(f"Error pulling tasks for tenant {self.tenant_id}: {exc}")
            finally:
                conn.close()
            

if __name__ == "__main__":
    puller = TaskPuller("tenantA")
    puller.run()
