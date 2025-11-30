import json
import pymysql
import kazoo.client

from distributed_multitenant_task_scheduler import config

from setup.mysql_setup import config as mysql_config, constants as mysql_constants
from setup.zookeeper_setup import config as zk_config, constants as zk_constants

class ShardManager:
    def __init__(self):
        self.zk = kazoo.client.KazooClient(hosts=zk_config.configurations[zk_constants.ZOOKEEPER_CONN])
        self.zk.start()

    def prepare_shards(self):
        with open("distributed_multitenant_task_scheduler/metadata.json", "r") as file:
            data = json.load(file)
        for shard_name, shard_data in data["shards"].items():

            conn = pymysql.connect(
                host=mysql_config.configurations[mysql_constants.HOST],
                port=int(mysql_config.configurations[mysql_constants.PORT]),
                user=mysql_config.configurations[mysql_constants.USER],
                password=mysql_config.configurations[mysql_constants.PASSWORD],
            )
            try:
                with conn.cursor() as cur:
                    try:    
                        cur.execute(f'CREATE DATABASE IF NOT EXISTS {shard_data["database"]}')
                        # tenant_id, payload, priority, schedule_time, status
                        # cur.execute(f"""DROP TABLE IF EXISTS {shard_data["database"]}.executable_tasks;""")   
                        # cur.execute(f"""DROP TABLE IF EXISTS {shard_data["database"]}.scheduled_tasks;""")

                        cur.execute(f"""CREATE TABLE IF NOT EXISTS {shard_data["database"]}.executable_tasks (
                            task_id BIGINT UNSIGNED PRIMARY KEY,
                            tenant_id VARCHAR(64) NOT NULL,
                            payload JSON NOT NULL,
                            priority ENUM('high', 'medium', 'low') NOT NULL DEFAULT 'medium',
                            schedule_time TIMESTAMP NOT NULL,
                            status ENUM('pending', 'queued', 'running', 'done', 'failed') NOT NULL DEFAULT 'pending',
                            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                        """)
                        cur.execute(f"""CREATE TABLE IF NOT EXISTS {shard_data["database"]}.scheduled_tasks (
                            task_id BIGINT UNSIGNED PRIMARY KEY,
                            tenant_id VARCHAR(64) NOT NULL,
                            payload JSON NOT NULL,
                            priority ENUM('high', 'medium', 'low') NOT NULL DEFAULT 'medium',
                            cron_schedule VARCHAR(128) NOT NULL
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                        """)
                    except Exception as exc:
                        print(f"Error creating database or tables for shard {shard_name}: {exc}")
                conn.commit()   
            finally:
                conn.close()

    def get_shard_uri(self, tenant_id: str) -> str:
        path = f"{config.TENANT_BASE_PATH}/{tenant_id}"
        if not self.zk.exists(path):
            raise KeyError(f"No shard mapping for {tenant_id}")
        data, _ = self.zk.get(path)
        return data.decode()

    def close(self):
        self.zk.stop()