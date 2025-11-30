import asyncio
import json
import time
from typing import Dict
import pymysql
from pymysql.cursors import DictCursor

import kazoo.client
from confluent_kafka import Producer as KafkaProducer
import config
from shard_manager import ShardManager
from setup.mysql_setup import config as mysql_config, constants as mysql_constants

class TaskPuller:
    def __init__(self, tenant_id: str):
        super().__init__()
        self.tenant_id = tenant_id
        self.zk = kazoo.client.KazooClient(hosts=config.ZOOKEEPER_CONN)
        self.zk.start()
        self.producer = KafkaProducer({"bootstrap.servers": config.KAFKA_BROKER})
        self.shard_manager = ShardManager()

    def run(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.poll_loop())

    async def poll_loop(self):
        while True:
            shard_uri = self.shard_manager .get_shard_uri(self.tenant_id)
            host, port, user, password, db = shard_uri.split(":")

            conn = pymysql.connect(
                host=mysql_config.configurations[mysql_constants.HOST],
                port=int(mysql_config.configurations[mysql_constants.PORT]),
                user=mysql_config.configurations[mysql_constants.USER],
                password=mysql_config.configurations[mysql_constants.PASSWORD],
                database=db,
                cursorclass=DictCursor,
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, payload, priority FROM tasks
                        WHERE tenant_id=%s AND status='pending' AND schedule_time<=NOW()
                        LIMIT 50
                        """,
                        (self.tenant_id,),
                    )
                    rows = cur.fetchall()
                    for row in rows:
                        topic = config.PRIORITY_TOPICS[row["priority"]]
                        payload = json.dumps(
                            {
                                "task_id": row["id"],
                                "tenant_id": self.tenant_id,
                                "payload": json.loads(row["payload"]),
                            }
                        ).encode()
                        self.producer.produce(topic, value=payload)
                        self.producer.poll(0)
                        cur.execute(
                            "UPDATE tasks SET status='queued' WHERE id=%s",
                            (row["id"],),
                        )
                conn.commit()
            finally:
                conn.close()
            time.sleep(2)
