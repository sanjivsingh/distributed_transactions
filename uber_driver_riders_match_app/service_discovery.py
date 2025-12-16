import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import traceback
import json
import time
import random
import threading
import hashlib
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, asdict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from kazoo.protocol.states import KazooState
from commons import logger

from config import REDIS_CITY_SHARDS, ZOOKEEPER_HOSTS, SERVICE_REGISTRY_PATH, REDIS_SHARDS_PATH, HEALTH_CHECK_PATH , SERVICE_DISCOVERY_PORT

app = FastAPI()
log = logger.setup_logger(__name__)


@dataclass
class ServiceInstance:
    service_name: str
    instance_id: str
    host: str
    port: int
    health_check_url: str
    metadata: Dict = None
    last_heartbeat: int = 0
    status: str = "healthy"  # healthy, unhealthy, down

@dataclass
class RedisShardInfo:
    shard_id: str
    host: str
    port: int
    weight: int = 100
    status: str = "healthy"
    last_check: int = 0

class ServiceRequest(BaseModel):
    service_name: str
    instance_id: str
    host: str
    port: int
    health_check_url: str
    metadata: Optional[Dict] = None

class ShardRequest(BaseModel):
    shard_id: str
    host: str
    port: int
    weight: Optional[int] = 100

class ZookeeperServiceDiscovery:
    def __init__(self):
        self.zk = None
        self.services: Dict[str, Dict[str, ServiceInstance]] = {}
        self.redis_shards: Dict[str, RedisShardInfo] = {}
        self.consistent_hash_ring = {}
        self.is_leader = False
        self._connect_to_zookeeper()
        self._initialize_paths()
        self._load_from_zookeeper()
        self._start_health_check_thread()
    
    def _connect_to_zookeeper(self):
        """Initialize Zookeeper connection"""
        try:
            self.zk = KazooClient(hosts=ZOOKEEPER_HOSTS)
            self.zk.add_listener(self._connection_listener)
            self.zk.start(timeout=10)
            log.info("Connected to Zookeeper")
        except Exception as e:
            log.error(f"Failed to connect to Zookeeper: {e}")
            raise
    
    def _connection_listener(self, state):
        """Handle Zookeeper connection state changes"""
        if state == KazooState.LOST:
            log.warning("Zookeeper connection lost")
        elif state == KazooState.SUSPENDED:
            log.warning("Zookeeper connection suspended")
        elif state == KazooState.CONNECTED:
            log.info("Zookeeper connection established")
    
    def _initialize_paths(self):
        """Create base paths in Zookeeper"""
        try:
            paths = [SERVICE_REGISTRY_PATH, REDIS_SHARDS_PATH, HEALTH_CHECK_PATH]
            for path in paths:
                self.zk.ensure_path(path)
            log.info("Zookeeper paths initialized")
        except Exception as e:
            log.error(f"Error initializing Zookeeper paths: {e}")
            raise
    
    def _load_from_zookeeper(self):
        """Load existing services and shards from Zookeeper"""
        try:
            # Load services
            try:
                service_types = self.zk.get_children(SERVICE_REGISTRY_PATH)
                for service_type in service_types:
                    service_path = f"{SERVICE_REGISTRY_PATH}/{service_type}"
                    instances = self.zk.get_children(service_path)
                    
                    if service_type not in self.services:
                        self.services[service_type] = {}
                    
                    for instance_id in instances:
                        instance_path = f"{service_path}/{instance_id}"
                        try:
                            data, _ = self.zk.get(instance_path)
                            service_data = json.loads(data.decode('utf-8'))
                            instance = ServiceInstance(**service_data)
                            self.services[service_type][instance_id] = instance
                        except Exception as e:
                            log.warning(f"Error loading service instance {instance_path}: {e}")
            except NoNodeError:
                log.info("No existing services found in Zookeeper")
            
            # Load Redis shards
            try:
                shards = self.zk.get_children(REDIS_SHARDS_PATH)
                for shard_id in shards:
                    shard_path = f"{REDIS_SHARDS_PATH}/{shard_id}"
                    try:
                        data, _ = self.zk.get(shard_path)
                        shard_data = json.loads(data.decode('utf-8'))
                        shard = RedisShardInfo(**shard_data)
                        self.redis_shards[shard_id] = shard
                    except Exception as e:
                        log.warning(f"Error loading Redis shard {shard_path}: {e}")
            except NoNodeError:
                log.info("No existing Redis shards found in Zookeeper")
            
            self._rebuild_consistent_hash_ring()
            log.info(f"Loaded {len(self.services)} services and {len(self.redis_shards)} Redis shards from Zookeeper")
            
        except Exception as e:
            log.error(f"Error loading from Zookeeper: {e}")
    
    def _rebuild_consistent_hash_ring(self):
        """Rebuild consistent hash ring for Redis shards"""
        self.consistent_hash_ring = {}
        
        for shard_id, shard in self.redis_shards.items():
            if shard.status == "healthy":
                # Add multiple virtual nodes for better distribution
                for i in range(shard.weight):
                    virtual_node = f"{shard_id}:{i}"
                    hash_key = hashlib.md5(virtual_node.encode()).hexdigest()
                    self.consistent_hash_ring[hash_key] = shard_id
        
        log.info(f"Rebuilt consistent hash ring with {len(self.consistent_hash_ring)} virtual nodes")
    
    def register_service(self, service: ServiceInstance) -> bool:
        """Register a service instance in Zookeeper"""
        try:
            service_path = f"{SERVICE_REGISTRY_PATH}/{service.service_name}"
            instance_path = f"{service_path}/{service.instance_id}"
            
            # Ensure service path exists
            self.zk.ensure_path(service_path)
            
            # Create ephemeral sequential node for the instance
            service.last_heartbeat = int(time.time())
            service_data = json.dumps(asdict(service)).encode('utf-8')
            
            # Create or update the node
            try:
                self.zk.create(instance_path, service_data, ephemeral=True)
            except NodeExistsError:
                self.zk.set(instance_path, service_data)
            
            # Update local cache
            if service.service_name not in self.services:
                self.services[service.service_name] = {}
            self.services[service.service_name][service.instance_id] = service
            
            # Set up watcher for this service
            self._watch_service_node(instance_path, service.service_name, service.instance_id)
            
            log.info(f"Registered service in Zookeeper: {service.service_name}:{service.instance_id}")
            return True
            
        except Exception as e:
            log.error(f"Error registering service in Zookeeper: {e}")
            return False
    
    def _watch_service_node(self, path: str, service_name: str, instance_id: str):
        """Set up watcher for service node changes"""
        @self.zk.DataWatch(path)
        def watch_service(data, stat):
            if data is None:
                # Node was deleted
                if service_name in self.services and instance_id in self.services[service_name]:
                    del self.services[service_name][instance_id]
                    log.info(f"Service removed from cache: {service_name}:{instance_id}")
            else:
                # Node was updated
                try:
                    service_data = json.loads(data.decode('utf-8'))
                    instance = ServiceInstance(**service_data)
                    if service_name not in self.services:
                        self.services[service_name] = {}
                    self.services[service_name][instance_id] = instance
                    log.debug(f"Service updated in cache: {service_name}:{instance_id}")
                except Exception as e:
                    log.error(f"Error updating service in cache: {e}")
    
    def deregister_service(self, service_name: str, instance_id: str) -> bool:
        """Deregister a service instance from Zookeeper"""
        try:
            instance_path = f"{SERVICE_REGISTRY_PATH}/{service_name}/{instance_id}"
            
            # Delete from Zookeeper
            try:
                self.zk.delete(instance_path)
            except NoNodeError:
                log.warning(f"Service node not found in Zookeeper: {instance_path}")
            
            # Remove from local cache
            if service_name in self.services and instance_id in self.services[service_name]:
                del self.services[service_name][instance_id]
            
            log.info(f"Deregistered service from Zookeeper: {service_name}:{instance_id}")
            return True
            
        except Exception as e:
            log.error(f"Error deregistering service from Zookeeper: {e}")
            return False
    
    def get_service_instances(self, service_name: str) -> List[ServiceInstance]:
        """Get all healthy instances of a service"""
        try:
            # Refresh from Zookeeper to get latest data
            self._refresh_service_from_zookeeper(service_name)
            
            if service_name not in self.services:
                return []
            
            healthy_instances = []
            current_time = int(time.time())
            
            for instance in self.services[service_name].values():
                # Consider instance healthy if heartbeat is within 30 seconds
                if current_time - instance.last_heartbeat < 30 and instance.status == "healthy":
                    healthy_instances.append(instance)
            
            return healthy_instances
            
        except Exception as e:
            log.error(f"Error getting service instances: {e}")
            return []
    
    def _refresh_service_from_zookeeper(self, service_name: str):
        """Refresh service instances from Zookeeper"""
        try:
            service_path = f"{SERVICE_REGISTRY_PATH}/{service_name}"
            instances = self.zk.get_children(service_path)
            
            # Update local cache
            if service_name not in self.services:
                self.services[service_name] = {}
            
            # Remove instances not in Zookeeper
            local_instances = set(self.services[service_name].keys())
            zk_instances = set(instances)
            for instance_id in local_instances - zk_instances:
                del self.services[service_name][instance_id]
            
            # Add/update instances from Zookeeper
            for instance_id in instances:
                instance_path = f"{service_path}/{instance_id}"
                try:
                    data, _ = self.zk.get(instance_path)
                    service_data = json.loads(data.decode('utf-8'))
                    instance = ServiceInstance(**service_data)
                    self.services[service_name][instance_id] = instance
                except Exception as e:
                    log.warning(f"Error refreshing service instance {instance_path}: {e}")
                    
        except NoNodeError:
            # Service doesn't exist in Zookeeper
            if service_name in self.services:
                self.services[service_name] = {}
        except Exception as e:
            log.error(f"Error refreshing service from Zookeeper: {e}")
    
    def get_service_instance(self, service_name: str, load_balance: str = "round_robin") -> Optional[ServiceInstance]:
        """Get a single service instance using load balancing"""
        instances = self.get_service_instances(service_name)
        
        if not instances:
            return None
        
        if load_balance == "random":
            return random.choice(instances)
        elif load_balance == "round_robin":
            # Simple round-robin based on current time
            index = int(time.time()) % len(instances)
            return instances[index]
        else:
            return instances[0]
    
    def register_redis_shard(self, shard: RedisShardInfo) -> bool:
        """Register a Redis shard in Zookeeper"""
        try:
            shard_path = f"{REDIS_SHARDS_PATH}/{shard.shard_id}"
            
            shard.last_check = int(time.time())
            shard_data = json.dumps(asdict(shard)).encode('utf-8')
            
            # Create or update the shard node
            try:
                self.zk.create(shard_path, shard_data, ephemeral=True)
            except NodeExistsError:
                self.zk.set(shard_path, shard_data)
            
            # Update local cache
            self.redis_shards[shard.shard_id] = shard
            
            # Set up watcher for this shard
            self._watch_shard_node(shard_path, shard.shard_id)
            
            # Rebuild hash ring
            self._rebuild_consistent_hash_ring()
            
            log.info(f"Registered Redis shard in Zookeeper: {shard.shard_id}")
            return True
            
        except Exception as e:
            log.error(f"Error registering Redis shard in Zookeeper: {e}")
            return False
    
    def _watch_shard_node(self, path: str, shard_id: str):
        """Set up watcher for Redis shard node changes"""
        @self.zk.DataWatch(path)
        def watch_shard(data, stat):
            if data is None:
                # Shard was removed
                if shard_id in self.redis_shards:
                    del self.redis_shards[shard_id]
                    self._rebuild_consistent_hash_ring()
                    log.info(f"Redis shard removed from cache: {shard_id}")
            else:
                # Shard was updated
                try:
                    shard_data = json.loads(data.decode('utf-8'))
                    shard = RedisShardInfo(**shard_data)
                    self.redis_shards[shard_id] = shard
                    self._rebuild_consistent_hash_ring()
                    log.debug(f"Redis shard updated in cache: {shard_id}")
                except Exception as e:
                    log.error(f"Error updating Redis shard in cache: {e}")
    
    def get_redis_shard(self, key: str) -> Optional[RedisShardInfo]:
        """Get Redis shard for a given key using consistent hashing"""
        if not self.consistent_hash_ring:
            return None
        
        # Hash the key
        key_hash = hashlib.md5(key.encode()).hexdigest()
        
        # Find the first virtual node >= key_hash
        sorted_hashes = sorted(self.consistent_hash_ring.keys())
        for hash_key in sorted_hashes:
            if hash_key >= key_hash:
                shard_id = self.consistent_hash_ring[hash_key]
                return self.redis_shards.get(shard_id)
        
        # Wrap around to the first node
        if sorted_hashes:
            shard_id = self.consistent_hash_ring[sorted_hashes[0]]
            return self.redis_shards.get(shard_id)
        
        return None
    
    def get_all_redis_shards(self) -> List[RedisShardInfo]:
        """Get all healthy Redis shards"""
        current_time = int(time.time())
        healthy_shards = []
        
        for shard in self.redis_shards.values():
            if shard.status == "healthy" and current_time - shard.last_check < 60:
                healthy_shards.append(shard)
        
        return healthy_shards
    
    def heartbeat(self, service_name: str, instance_id: str) -> bool:
        """Update heartbeat for a service instance in Zookeeper"""
        try:
            instance_path = f"{SERVICE_REGISTRY_PATH}/{service_name}/{instance_id}"
            
            if service_name in self.services and instance_id in self.services[service_name]:
                instance = self.services[service_name][instance_id]
                instance.last_heartbeat = int(time.time())
                instance.status = "healthy"
                
                # Update in Zookeeper
                service_data = json.dumps(asdict(instance)).encode('utf-8')
                try:
                    self.zk.set(instance_path, service_data)
                except NoNodeError:
                    # Node doesn't exist, recreate it
                    self.zk.create(instance_path, service_data, ephemeral=True)
                
                return True
            
            return False
            
        except Exception as e:
            log.error(f"Error updating heartbeat in Zookeeper: {e}")
            return False
    
    def _start_health_check_thread(self):
        """Start background thread for health checking"""
        def health_checker():
            while True:
                try:
                    current_time = int(time.time())
                    
                    # Check service instances
                    for service_name, instances in self.services.items():
                        for instance_id, instance in list(instances.items()):
                            if current_time - instance.last_heartbeat > 60:  # 1 minute timeout
                                instance.status = "unhealthy"
                                log.warning(f"Service unhealthy: {service_name}:{instance_id}")
                                
                                # Remove if down for too long
                                if current_time - instance.last_heartbeat > 300:  # 5 minutes
                                    self.deregister_service(service_name, instance_id)
                    
                    # Check Redis shards
                    for shard_id, shard in list(self.redis_shards.items()):
                        if current_time - shard.last_check > 120:  # 2 minutes timeout
                            shard.status = "unhealthy"
                            log.warning(f"Redis shard unhealthy: {shard_id}")
                            self._rebuild_consistent_hash_ring()
                    
                    time.sleep(30)  # Check every 30 seconds
                    
                except Exception as e:
                    log.error(f"Error in health checker: {e}")
                    time.sleep(30)
        
        health_thread = threading.Thread(target=health_checker, daemon=True)
        health_thread.start()
    
    def close(self):
        """Close Zookeeper connection"""
        if self.zk:
            self.zk.stop()
            self.zk.close()

# Global service discovery manager
service_manager = ZookeeperServiceDiscovery()

@app.post("/register/service")
async def register_service(request: ServiceRequest):
    """Register a service instance"""
    try:
        service = ServiceInstance(
            service_name=request.service_name,
            instance_id=request.instance_id,
            host=request.host,
            port=request.port,
            health_check_url=request.health_check_url,
            metadata=request.metadata or {},
            last_heartbeat=int(time.time()),
            status="healthy"
        )
        
        success = service_manager.register_service(service)
        if success:
            return {"message": "Service registered successfully", "service": request.service_name}
        else:
            raise HTTPException(status_code=500, detail="Failed to register service")
            
    except Exception as e:
        log.error(f"Error in register_service: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/deregister/service/{service_name}/{instance_id}")
async def deregister_service(service_name: str, instance_id: str):
    """Deregister a service instance"""
    try:
        success = service_manager.deregister_service(service_name, instance_id)
        if success:
            return {"message": "Service deregistered successfully"}
        else:
            raise HTTPException(status_code=404, detail="Service instance not found")
            
    except Exception as e:
        log.error(f"Error in deregister_service: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/services/match")
async def get_match_services():
    """Get all match service instances"""
    try:
        instances = service_manager.get_service_instances("match_service")
        return {
            "service_name": "match_service",
            "instances": [asdict(instance) for instance in instances],
            "count": len(instances)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/services/location")
async def get_location_services():
    """Get all location update service instances"""
    try:
        instances = service_manager.get_service_instances("location_service")
        return {
            "service_name": "location_service", 
            "instances": [asdict(instance) for instance in instances],
            "count": len(instances)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/service/{service_name}")
async def get_service_instance(service_name: str, load_balance: str = "round_robin"):
    """Get a single service instance with load balancing"""
    try:
        instance = service_manager.get_service_instance(service_name, load_balance)
        if instance:
            return asdict(instance)
        else:
            raise HTTPException(status_code=404, detail=f"No healthy instances found for {service_name}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/register/redis_shard")
async def register_redis_shard(request: ShardRequest):
    """Register a Redis shard"""
    try:
        shard = RedisShardInfo(
            shard_id=request.shard_id,
            host=request.host,
            port=request.port,
            weight=request.weight or 100,
            status="healthy",
            last_check=int(time.time())
        )
        
        success = service_manager.register_redis_shard(shard)
        if success:
            return {"message": "Redis shard registered successfully", "shard_id": request.shard_id}
        else:
            raise HTTPException(status_code=500, detail="Failed to register Redis shard")
            
    except Exception as e:
        log.error(f"Error in register_redis_shard: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/redis_shard/{key}")
async def get_redis_shard_for_key(key: str):
    """Get Redis shard for a specific key using consistent hashing"""
    try:
        shard = service_manager.get_redis_shard(key)
        if shard:
            return {
                "key": key,
                "shard": asdict(shard),
                "connection_string": f"redis://{shard.host}:{shard.port}"
            }
        else:
            raise HTTPException(status_code=404, detail="No healthy Redis shard found")
    except Exception as e:
        traceback.print_exc() 
        log.error(f"Error in get_redis_shard_for_key: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/redis_shards")
async def get_all_redis_shards():
    """Get all Redis shards"""
    try:
        shards = service_manager.get_all_redis_shards()
        return {
            "shards": [asdict(shard) for shard in shards],
            "count": len(shards),
            "hash_ring_size": len(service_manager.consistent_hash_ring)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/heartbeat/{service_name}/{instance_id}")
async def service_heartbeat(service_name: str, instance_id: str):
    """Update service heartbeat"""
    try:
        success = service_manager.heartbeat(service_name, instance_id)
        if success:
            return {"message": "Heartbeat updated", "timestamp": int(time.time())}
        else:
            raise HTTPException(status_code=404, detail="Service instance not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Service discovery health check"""
    return {
        "status": "healthy",
        "timestamp": int(time.time()),
        "services_registered": len(service_manager.services),
        "redis_shards": len(service_manager.redis_shards),
        "zookeeper_connected": service_manager.zk.connected if service_manager.zk else False
    }

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    service_manager.close()


if __name__ == "__main__":
    try:
        log.info("Service Discovery starting with Zookeeper backend")
        uvicorn.run(app, host="0.0.0.0", port=SERVICE_DISCOVERY_PORT)
    except KeyboardInterrupt:
        log.info("Service Discovery shutting down")
        service_manager.close()