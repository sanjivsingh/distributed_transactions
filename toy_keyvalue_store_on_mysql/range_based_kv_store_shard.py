
from toy_KV_storeon_mysql.kv_store import  ToyKVStore
from commons import logger
from typing import List

from setup.mysql_setup import config as mysql_config, constants as mysql_constants
class RangeBasedShardManager:

    def __init__(self, mapping : List[tuple]) -> None:
        self.log = logger.setup_logger(__name__)
        self.mapping  = [
            ("a" , "e"),
            ("e" , "f"),
            ("f" , "l"),
            ("l" , "m"),
            ("m" , "n"),
            ("n" , "p"),
            ("p" , "z")
        ]
        self.shards = []
        for i in range(5):  
            try:
                shard = ToyKVStore(
                    host=mysql_config.configurations[mysql_constants.HOST],
                    user=mysql_config.configurations[mysql_constants.USER],
                    password=mysql_config.configurations[mysql_constants.PASSWORD],
                    port=mysql_config.configurations[mysql_constants.PORT],
                    database= "kv_store", 
                    table = f"shard{i}"
                )
                shard.init_db()
                self.shards.append(shard)
            except Exception as e:
                self.log.error(f"error connecting shard {i} : {e}")

        if not self.shards:
            raise RuntimeError("No shards available")

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        for shard in self.shards:
            try:
                shard.disconnect()
            except Exception as e:
                self.log.error(f"error disconnect shard: {e}")

    def __get_shard(self, key : str) -> ToyKVStore:
        if not self.shards:
            raise RuntimeError("No shards available")

        if not isinstance(key, str): 
            raise ValueError("Key must be a string")

        for i in range(len(self.mapping)):
            start_end = self.mapping[i]
            if start_end[0] <= key and key < start_end[1]:
                self.log.info(f"mapping found for key {key}: {start_end}")
                shard_index  = i % len(self.shards)
                return self.shards[shard_index]

        self.log.info("no mapping found going to default shard")
        shard_index  = len(self.mapping) % len(self.shards)
        return self.shards[shard_index]
        
    def delete(self, key: str):
        try:
            self.__get_shard(key).delete(key)
        except Exception as e:
            self.log.error(f"error in delete {key} : {e} ")

    def get(self, key: str):
        try:
            return self.__get_shard(key).get(key)
        except Exception as e:
            self.log.error(f"error in get {key} : {e} ")
            return None
    def set(self, key: str, value):
        try:
            self.__get_shard(key).set(key,value)
            self.log.info(f"set key {key} ")
        except Exception as e:
            self.log.error(f"error in set {key} : {e} ")

if __name__ == "__main__":
    mapping  = [
                ("a" , "e"),
                ("e" , "f"),
                ("f" , "l"),
                ("l" , "m"),
                ("m" , "n"),
                ("n" , "p"),
                ("p" , "z")
            ]
    manager = RangeBasedShardManager(mapping)
    
    manager.set("user:101", {"name": "Alice", "email": "alice@example.com"})
    manager.set("app:settings", {"theme": "dark", "version": 1.5})
    
    # Retrieve a value
    user_data = manager.get("user:101")
    print(f"Retrieved user_data: {user_data}")
    
    # Update a value
    manager.set("app:settings", {"theme": "light", "version": 2.0})
    app_settings = manager.get("app:settings")
    print(f"Retrieved app_settings: {app_settings}")

    # Delete a key
    manager.delete("user:101")
    
    # Attempt to retrieve a deleted key
    user_data_after_delete = manager.get("user:101")
    print(f"Retrieved user_data after deletion: {user_data_after_delete}")
    
    manager.set("_user:101", {"name": "Alice", "email": "alice@example.com"})

    manager.disconnect()