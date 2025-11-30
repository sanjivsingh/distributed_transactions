import time
import kazoo.client
import config
import json
from task_puller import TaskPuller
from shard_manager import ShardManager

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

    def close(self):
        if self.shard_manager:
            self.shard_manager.close()
        if self.zk:
            self.zk.stop()

if __name__ == "__main__":
    try:
        orchestrator = SchedulerOrchestrator()
        orchestrator.run()
    except KeyboardInterrupt:
        print("Shutting down scheduler orchestrator...")
        orchestrator.close()