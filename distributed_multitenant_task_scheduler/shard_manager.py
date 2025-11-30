import json
import pymysql
import kazoo.client
import config

from setup.mysql_setup import config as mysql_config, constants as mysql_constants
class ShardManager:
    def __init__(self):
        self.zk = kazoo.client.KazooClient(hosts=config.ZOOKEEPER_CONN)
        self.zk.start()

    def prepare_shards(self):
        with open("metadata.json", "r") as file:
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
                    cur.execute(f'CREATE DATABASE IF NOT EXISTS {shard_data["database"]}')
            finally:
                conn.close()

    def get_shard_uri(self, tenant_id: str) -> str:
        path = f"{config.SHARD_BASE_PATH}/{tenant_id}"
        if not self.zk.exists(path):
            raise KeyError(f"No shard mapping for {tenant_id}")
        data, _ = self.zk.get(path)
        return data.decode()

    def close(self):
        self.zk.stop()