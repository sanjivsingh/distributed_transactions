import time
from typing import Dict
import pymysql
from pymysql.cursors import DictCursor
import kazoo.client
from confluent_kafka import Producer as KafkaProducer, Consumer as KafkaConsumer
import config
import json
from task_puller import TaskPuller

class ShardManager:
    def __init__(self):
        self.zk = kazoo.client.KazooClient(hosts=config.ZOOKEEPER_CONN)
        self.zk.start()

    def  prepare_shards(self):
        try:
            with open("metadata.json", "r") as file:
                data = json.load(file)
            shards = data["shards"]
            for shard_name, shard_data in shards.items():
                conn = pymysql.connect(
                    host=shard_data["host"],
                    port=int(shard_data["port"]),
                    user=shard_data["user"],
                    password=shard_data["password"]
                )
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"""
                            CREATE DATABASE IF NOT EXISTS {shard_data["database"]}
                            """
                        )
                        print(f"Database {shard_data['database']} ensured on shard {shard_name}")
                finally:
                    conn.close()

        
        except FileNotFoundError:
            print("Error: The file 'your_file.json' was not found.")
        except json.JSONDecodeError:
            print("Error: Failed to decode JSON from the file.")

    def get_shard_uri(self, tenant_id: str) -> str:
        path = f"{config.SHARD_BASE_PATH}/{tenant_id}"
        if not self.zk.exists(path):
            raise KeyError(f"No shard mapping for {tenant_id}")
        data, _ = self.zk.get(path)
        return data.decode()

    def close(self):
        self.zk.stop()

class SchedulerOrchestrator:

    def __init__(self):
        super().__init__()
        self.zk = kazoo.client.KazooClient(hosts=config.ZOOKEEPER_CONN)
        self.zk.start()
        self.shard_manager = ShardManager()
        self.shard_manager.prepare_shards()

    def run(self):
        with open("metadata.json", "r") as file:
            data = json.load(file)
            tenants_metadata = data["tenants"]
            shards = data["shards"]
            for tenant in tenants_metadata:

                desired_pullers = tenants_metadata[tenant]["desired_pullers"]
                desired_pullers_path = f"{config.TENANT_PULLER_PATH}/{tenant}/desired_pullers"
                self.zk.create(desired_pullers_path, str(desired_pullers).encode(), makepath=True)
                print(f"Set desired pullers for {tenant} to {desired_pullers}")

                shard = tenants_metadata[tenant]["shard"]
                shard_metadata = shards[shard]
                tenant_shard_path = f"{config.SHARD_BASE_PATH}/{tenant}"
                self.zk.create(tenant_shard_path, f"""{shard_metadata["host"]}:{shard_metadata["port"]}:{shard_metadata["user"]}:{shard_metadata["password"]}:{shard_metadata["database"]}""".encode(), makepath=True)
                print(f"Mapped {tenant} to shard {shard}")
                
        while True:
            tenants = self.zk.get_children(config.SHARD_BASE_PATH)
            for tenant in tenants:
                desired = int(
                    self.zk.get(f"{config.TENANT_PULLER_PATH}/{tenant}/desired_pullers")[0].decode()
                )
                active = self.zk.get_children(
                    f"{config.TENANT_PULLER_PATH}/{tenant}/active"
                )
                if len(active) < desired:
                    # trigger provisioning hook (external system)
                    print(f"Provision puller for {tenant}")
                    puller = TaskPuller(tenant)
                    puller.run()
            time.sleep(5)

