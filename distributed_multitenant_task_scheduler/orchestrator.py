import time
import kazoo.client
import json

import threading

from distributed_multitenant_task_scheduler.task_puller import TaskPuller
from distributed_multitenant_task_scheduler.task_executor import TaskExecutor
from distributed_multitenant_task_scheduler.shard_manager import ShardManager
from distributed_multitenant_task_scheduler import config

from setup.zookeeper_setup import config as zk_config, constants as zk_constants
from setup.mysql_setup import config as mysql_config, constants as mysql_constants


class SchedulerOrchestrator:

    def __init__(self):
        super().__init__()
        self.zk = kazoo.client.KazooClient(
            hosts=zk_config.configurations[zk_constants.ZOOKEEPER_CONN]
        )
        self.zk.start()
        self.shard_manager = ShardManager()
        self.shard_manager.prepare_shards()

    def run(self):
        with open("distributed_multitenant_task_scheduler/metadata.json", "r") as file:
            data = json.load(file)
            tenant_configurations = data["tenant_configurations"]
            shards = data["shards"]

            for shard in shards:
                print(f"Preparing shard: {shard}")
                shard_metadata = shards[shard]
                shard_path = f"{config.SHARD_BASE_PATH}/{shard}"
                self.zk.ensure_path(shard_path)
                self.zk.set(
                    shard_path,
                    f"""{mysql_config.configurations[mysql_constants.HOST]}:{mysql_config.configurations[mysql_constants.PORT]}:{mysql_config.configurations[mysql_constants.USER]}:{mysql_config.configurations[mysql_constants.PASSWORD]}:{shard_metadata["database"]}""".encode(),
                )
                print(f"ZK : Mapped configuration {shard} to {shard_metadata['database']}")

            # Setup Zookeeper paths and initial Puller settings
            for tenant in tenant_configurations:

                desired_pullers = tenant_configurations[tenant]["desired_pullers"]
                desired_pullers_path = (
                    f"{config.TENANT_BASE_PATH}/{tenant}/desired_pullers"
                )
                self.zk.ensure_path(desired_pullers_path)
                self.zk.set(
                    desired_pullers_path, str(desired_pullers).encode() 
                )
                print(f"ZK : Set desired pullers for {tenant} to {desired_pullers}")

                shard = tenant_configurations[tenant]["shard"]
                shard_metadata = shards[shard]
                tenant_shard_path = f"{config.TENANT_BASE_PATH}/{tenant}"
                self.zk.ensure_path(tenant_shard_path)
                self.zk.set(
                    tenant_shard_path,
                    f"""{mysql_config.configurations[mysql_constants.HOST]}:{mysql_config.configurations[mysql_constants.PORT]}:{mysql_config.configurations[mysql_constants.USER]}:{mysql_config.configurations[mysql_constants.PASSWORD]}:{shard_metadata["database"]}""".encode(),
                )
                print(f"ZK : Mapped {tenant} to shard {shard}")

                tenant_puller_active_path = f"{config.TENANT_BASE_PATH}/{tenant}/active"
                self.zk.ensure_path(tenant_puller_active_path)
                print(f"ZK : Active puller path for {tenant}: {tenant_puller_active_path}")

            # Setup Zookeeper paths and initial Executor settings
            task_executors = data["task_executors"]
            for priority in task_executors:
                desired_executors = task_executors[priority]
                desired_executors_path = (
                    f"{config.EXECUTOR_PATH}/{priority}/desired_executors"
                )
                self.zk.ensure_path(desired_executors_path)
                self.zk.set(
                    desired_executors_path, str(desired_executors).encode()
                )
                print(f"Set desired executors for {priority} to {desired_executors_path} = {desired_executors}")

                priority_executor_active_path = f"{config.EXECUTOR_PATH}/{priority}/active"
                self.zk.ensure_path(priority_executor_active_path)
                print(f"Active executor path for {priority}: {priority_executor_active_path}")

        while True:

            tenants = self.zk.get_children(config.TENANT_BASE_PATH)
            for tenant in tenants:
                desired_path = f"{config.TENANT_BASE_PATH}/{tenant}/desired_pullers"
                self.zk.ensure_path(desired_path)
                desired_data, _ = self.zk.get(desired_path)
                desired = int(desired_data.decode() or "0")
                
                tenant_active_pullars = self.zk.get_children(
                    f"{config.TENANT_BASE_PATH}/{tenant}/active"
                )
                while len(tenant_active_pullars) < desired:
                    # trigger provisioning hook (external system)
                    print(f"Provision puller for {tenant}")
                    puller = TaskPuller(tenant)
                    threading.Thread(target=puller.run).start()

                    tenant_active_pullars = self.zk.get_children(
                        f"{config.TENANT_BASE_PATH}/{tenant}/active"
                    )

            priority_types = self.zk.get_children(config.EXECUTOR_PATH)
            for priority in priority_types:
                desired_path = f"{config.EXECUTOR_PATH}/{priority}/desired_executors"
                self.zk.ensure_path(desired_path)
                desired_data, _ = self.zk.get(desired_path)
                desired = int(desired_data.decode() or "0")

                try:

                    priority_active_executers = self.zk.get_children(
                        f"{config.EXECUTOR_PATH}/{priority}/active"
                    )
                    while len(priority_active_executers) < desired:
                        # trigger provisioning hook (external system)
                        print(f"Provision executor for {priority} priority")
                        puller = TaskExecutor(priority)
                        threading.Thread(target=puller.run).start()

                        priority_active_executers = self.zk.get_children(
                            f"{config.EXECUTOR_PATH}/{priority}/active"
                        ) 
                except Exception as exc:
                    print(f"Error managing executors for priority {priority}: {exc}")
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
