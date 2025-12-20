import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import json
from commons import logger
from dataclasses import dataclass
import config
from typing import List, Optional 

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError

log = logger.setup_logger(__name__)

@dataclass
class ShardMapping:
    prefix: str
    node: str
    data_base: str
    collection: str

@dataclass
class ShardConfig:
    shard_version: str
    mappings: List[ShardMapping]


class ShardManager:
    """Manages shard configuration in Zookeeper"""
    
    def __init__(self):
        self.zk : Optional[KazooClient] = None
        self.shard_config : Optional[ShardConfig] = None
        self.__initialise()

    def __initialise(self):
        """Connect to Zookeeper"""
        try:
            self.zk = KazooClient(hosts=config.ZOOKEEPER_HOSTS)
            self.zk.start(timeout=10)
            log.info("Connected to Zookeeper")
            
            # Ensure path exists
            self.zk.ensure_path(config.SHARD_CONFIG_PATH)
            
        except Exception as e:
            log.error(f"Failed to connect to Zookeeper: {e}")
            raise RuntimeError(f"Failed to connect to Zookeeper: {e}")
        self.refresh_shard_config()
    
    def get_shard_config(self) -> Optional[ShardConfig]:
        return self.shard_config
    
    def save_shard_config(self) -> bool:
        """Save shard configuration to Zookeeper"""
        try:
            config_dict = {
                "shard_version": self.shard_config.shard_version,
                "mappings": [{"prefix": m.prefix, "node": m.node, "data_base": m.data_base, "collection": m.collection} for m in self.shard_config.mappings]
            }
            
            config_json = json.dumps(config_dict, indent=2).encode('utf-8')
            
            try:
                self.zk.create(config.SHARD_CONFIG_PATH, config_json)
            except NodeExistsError:
                self.zk.set(config.SHARD_CONFIG_PATH, config_json)
            
            log.info(f"Saved shard config version: {self.shard_config.shard_version}")
            return True
            
        except Exception as e:
            log.error(f"Error saving shard config: {e}")
            return False
    
    def watch_shard_config(self, callback):
        """Watch for shard configuration changes"""
        @self.zk.DataWatch(config.SHARD_CONFIG_PATH)
        def watch_config(data, stat):
            if data:
                try:
                    config_json = json.loads(data.decode('utf-8'))
                    mappings = [ShardMapping(m['prefix'], m['node'], m['data_base'], m['collection']) for m in config_json['mappings']]
                    config = ShardConfig(
                        shard_version=config_json['shard_version'],
                        mappings=mappings
                    )
                    callback(config)
                    log.info(f"Shard config updated: {config.shard_version}")
                except Exception as e:
                    log.error(f"Error processing config update: {e}")
    
    def close(self):
        """Close Zookeeper connection"""
        if self.zk:
            self.zk.stop()
            self.zk.close()

    def refresh_shard_config(self):
        """Update shard configuration version"""
        try:
            load = False
            if not self.shard_config:
                print("No existing configuration found")
                self.shard_config  = self.__load_shard_config_from_file()
                load = True
            else:
                current_shard_config  = self.__load_shard_config_from_file()
                if current_shard_config and current_shard_config.shard_version > self.shard_config.shard_version:
                    self.shard_config = current_shard_config
                    load = True
                
            if load:
                updated = self.save_shard_config()
                if updated:
                    log.info(f"Shard config version updated to: {self.shard_config.shard_version}")
        except Exception as e:
            log.error(f"Error updating config: {e}")
            print(f"✗ Error: {e}")

    def __load_shard_config_from_file(self) -> ShardConfig:
        """Load shard configuration from JSON file"""
        try:
            config_file = config.SHARD_CONFIG_FILE
            
            # Try to find config file in current directory or app directory
            config_paths = [
                config_file,
                os.path.join(os.path.dirname(__file__), config_file),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), config_file)
            ]
            config_path = None
            for path in config_paths:
                if os.path.exists(path):
                    config_path = path
                    break
            
            if not config_path:
                log.error(f"Shard configuration file not found: {config_file}")
                raise FileNotFoundError(f"Shard configuration file not found: {config_file}")
            
            log.info(f"Loading shard configuration from: {config_path}")
            
            with open(config_path, 'r', encoding='utf-8') as f:
                config_json = json.load(f)
            
            # Validate required fields
            if 'shard_version' not in config_json or 'mappings' not in config_json:
                log.error("Invalid shard configuration format: missing required fields")
                raise ValueError("Invalid shard configuration format: missing required fields")
            
            # Parse mappings
            mappings = []
            for mapping_data in config_json['mappings']:
                if 'prefix' not in mapping_data or 'node' not in mapping_data or 'data_base' not in mapping_data or 'collection' not in mapping_data:
                    log.error(f"Invalid mapping format: {mapping_data}")
                    continue
                mappings.append(ShardMapping(mapping_data['prefix'], mapping_data['node'], mapping_data['data_base'], mapping_data['collection']))
            
            if not mappings:
                log.error("No valid mappings found in configuration file")
                raise ValueError("No valid mappings found in configuration file")
            
            shard_config = ShardConfig(
                shard_version=config_json['shard_version'],
                mappings=mappings
            )
            
            log.info(f"Loaded shard config from file: version={shard_config.shard_version}, mappings={len(mappings)}")
            return shard_config
            
        except json.JSONDecodeError as e:
            log.error(f"Invalid JSON in shard configuration file: {e}")
            raise ValueError(f"Invalid JSON in shard configuration file: {e}")
        except Exception as e:
            log.error(f"Error loading shard configuration from file: {e}")
            raise RuntimeError(f"Error loading shard configuration from file: {e}")

    def find_shard_for_prefix(self, query: str) -> dict[str, str]:
        """Find the appropriate shard for a given query prefix"""
        try:
            search_term = query.lower()
            
            # 1. Use Binary Search (bisect_right) to find the insertion point
            # This finds the first element > search_term
            self.shard_config.mappings.sort(key=lambda m: m.prefix.lower())
            shard_map = [{"prefix": m.prefix.lower(), "node": m.node , "data_base": m.data_base, "collection": m.collection} for m in self.shard_config.mappings] 
            low = 0
            high = len(shard_map)
            
            while low < high:
                mid = (low + high) // 2
                if search_term < shard_map[mid]["prefix"]:
                    high = mid
                else:
                    low = mid + 1
                    
            # 2. The target is the element right before the insertion point
            # If index is 0, the prefix comes before 'a' (handle as error or default)
            target_index = low - 1
            
            if target_index < 0:
                raise RuntimeError(f"No shard found for prefix '{query}'")

            return shard_map[target_index]
        except Exception as e:
            log.error(f"Error finding shard for prefix '{query}': {e}")
            raise RuntimeError(f"Error finding shard for prefix '{query}': {e}")

if __name__ == "__main__":
    shardManager = ShardManager()
    while True:
        shardManager.refresh_shard_config()
        import time
        time.sleep(60)