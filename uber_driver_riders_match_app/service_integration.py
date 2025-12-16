import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


import requests
import time
import threading
import uuid
from typing import Optional, Dict, Any
from commons import logger
from config import DEFAULT_REDIS_CONFIG, SERVICE_DISCOVERY_URL
from redis import Redis
log = logger.setup_logger(__name__)

class ServiceIntegration:
    def __init__(self, service_name: str, host: str, port: int, health_url: str = "/health"):
        self.service_name = service_name
        self.host = host
        self.port = port
        self.health_url = health_url
        self.instance_id = f"{service_name}_{uuid.uuid4().hex[:8]}"
        self.service_discovery_url = SERVICE_DISCOVERY_URL
        self.registered = False
        self.heartbeat_thread = None
        self.service_cache = {}
        self.cache_expiry = {}
        self.redis_shard_cache = {}  # Cache for city-based shard mapping
        
    def register_service(self, metadata: Optional[Dict] = None) -> bool:
        """Register this service with service discovery"""
        try:
            registration_data = {
                "service_name": self.service_name,
                "instance_id": self.instance_id,
                "host": self.host,
                "port": self.port,
                "health_check_url": f"http://{self.host}:{self.port}{self.health_url}",
                "metadata": metadata or {}
            }
            
            response = requests.post(
                f"{self.service_discovery_url}/register/service",
                json=registration_data,
                timeout=5
            )
            
            if response.status_code == 200:
                self.registered = True
                self._start_heartbeat()
                log.info(f"Service {self.service_name}:{self.instance_id} registered successfully")
                return True
            else:
                log.error(f"Failed to register service: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            log.error(f"Error registering service: {e}")
            return False
    
    def deregister_service(self) -> bool:
        """Deregister this service from service discovery"""
        try:
            if not self.registered:
                return True
            
            response = requests.delete(
                f"{self.service_discovery_url}/deregister/service/{self.service_name}/{self.instance_id}",
                timeout=5
            )
            
            if response.status_code == 200:
                self.registered = False
                if self.heartbeat_thread:
                    self.heartbeat_thread = None
                log.info(f"Service {self.service_name}:{self.instance_id} deregistered successfully")
                return True
            else:
                log.warning(f"Failed to deregister service: {response.status_code}")
                return False
                
        except Exception as e:
            log.error(f"Error deregistering service: {e}")
            return False
    
    def discover_service(self, target_service: str, load_balance: str = "round_robin") -> Optional[Dict[str, Any]]:
        """Discover a service instance with caching"""
        try:
            # Check cache first (5-second TTL)
            cache_key = f"{target_service}_{load_balance}"
            current_time = time.time()
            
            if cache_key in self.service_cache and current_time < self.cache_expiry.get(cache_key, 0):
                return self.service_cache[cache_key]
            
            # Fetch from service discovery
            response = requests.get(
                f"{self.service_discovery_url}/service/{target_service}",
                params={"load_balance": load_balance},
                timeout=5
            )
            
            if response.status_code == 200:
                service_info = response.json()
                # Cache the result
                self.service_cache[cache_key] = service_info
                self.cache_expiry[cache_key] = current_time + 5  # 5-second cache
                return service_info
            else:
                log.warning(f"Service {target_service} not found: {response.status_code}")
                return None
                
        except Exception as e:
            log.error(f"Error discovering service {target_service}: {e}")
            return None
    
    def get_service_url(self, target_service: str, load_balance: str = "round_robin") -> Optional[str]:
        """Get base URL for a service"""
        service_info = self.discover_service(target_service, load_balance)
        if service_info:
            return f"http://{service_info['host']}:{service_info['port']}"
        return None
    
    def discover_redis_shard_by_city(self, city: str) -> Optional[Dict[str, Any]]:
        """Discover Redis shard for a given city with caching"""
        try:
            # Check cache first (60-second TTL for city mappings)
            current_time = time.time()
            if city in self.redis_shard_cache and current_time < self.redis_shard_cache[city].get('expiry', 0):
                return self.redis_shard_cache[city]['data']
            
            # Normalize city name for consistent hashing
            normalized_city = city.lower().strip().replace(" ", "_")
            
            response = requests.get(
                f"{self.service_discovery_url}/redis_shard/{normalized_city}",
                timeout=5
            )
            
            if response.status_code == 200:
                shard_info = response.json()
                # Cache the result with longer TTL for city mappings
                self.redis_shard_cache[city] = {
                    'data': shard_info,
                    'expiry': current_time + 60  # 60-second cache for city mappings
                }
                log.debug(f"Redis shard for city {city}: {shard_info['shard']['shard_id']}")
                return shard_info
            else:
                log.warning(f"Redis shard for city {city} not found: {response.status_code}")
                return None
                
        except Exception as e:
            log.error(f"Error discovering Redis shard for city {city}: {e}")
            return None
    
    def discover_redis_shard_by_driver_location(self, driver_id: str, city: str) -> Optional[Dict[str, Any]]:
        """Discover Redis shard for a driver based on their city location"""
        try:
            # Use city-based sharding instead of driver_id
            return self.discover_redis_shard_by_city(city)
                
        except Exception as e:
            log.error(f"Error discovering Redis shard for driver {driver_id} in city {city}: {e}")
            return None
    
    def get_redis_connection_info(self, city: str) -> Optional[Dict[str, str]]:
        """Get Redis connection information for a specific city"""
        try:
            shard_info = self.discover_redis_shard_by_city(city)
            if shard_info and 'shard' in shard_info:
                shard = shard_info['shard']
                return {
                    'host': shard['host'],
                    'port': str(shard['port']),
                    'shard_id': shard['shard_id'],
                    'connection_string': f"redis://{shard['host']}:{shard['port']}"
                }

            return {
                    'host': DEFAULT_REDIS_CONFIG['host'],
                    'port': str(DEFAULT_REDIS_CONFIG['port']),
                    'shard_id': 'default',
                    'connection_string': f"redis://{DEFAULT_REDIS_CONFIG['host']}:{DEFAULT_REDIS_CONFIG['port']}"
                }
        except Exception as e:
            log.error(f"Error getting Redis connection info for city {city}: {e}")
            return None
    
    def get_redis_pubsub_connection_info(self) -> Optional[Dict[str, str]]:
        return {
            'host': DEFAULT_REDIS_CONFIG['host'],
            'port': str(DEFAULT_REDIS_CONFIG['port']),
            'shard_id': 'default',
            'connection_string': f"redis://{DEFAULT_REDIS_CONFIG['host']}:{DEFAULT_REDIS_CONFIG['port']}"
        }

    def  get_redis_pubsub_client(self) -> Redis :
        import redis
        conn_info  = self.get_redis_pubsub_connection_info()
        if conn_info:
            try:
                return redis.Redis(
                    host=conn_info['host'], 
                    port=int(conn_info['port']), 
                    db=0,
                    decode_responses=False
                )
            except Exception as e:
                log.error(f"Error creating Redis Pub/Sub client: {e}")
                return None
        return None

    def get_redis_client_for_city(self, city: str):
        """Get Redis client instance for a specific city"""
        try:
            import redis
            conn_info = self.get_redis_connection_info(city)
            if conn_info:
                return redis.Redis(
                    host=conn_info['host'], 
                    port=int(conn_info['port']), 
                    db=0,
                    decode_responses=False
                )
            return None
        except Exception as e:
            log.error(f"Error creating Redis client for city {city}: {e}")
            return None
    
    def get_city_from_coordinates(self, lat: float, lng: float) -> str:
        """Get city name from coordinates (simplified mapping)"""
        # This is a simplified implementation
        # In production, you might use reverse geocoding service
        
        # Example city mappings based on coordinate ranges
        city_mappings = [
            {"name": "ashburn", "lat_min": 38.9, "lat_max": 39.1, "lng_min": -77.6, "lng_max": -77.4},
            {"name": "herndon", "lat_min": 38.9, "lat_max": 39.0, "lng_min": -77.4, "lng_max": -77.3},
            {"name": "reston", "lat_min": 38.9, "lat_max": 39.0, "lng_min": -77.4, "lng_max": -77.3},
            {"name": "washington_dc", "lat_min": 38.8, "lat_max": 39.0, "lng_min": -77.2, "lng_max": -76.9},
            {"name": "alexandria", "lat_min": 38.7, "lat_max": 38.9, "lng_min": -77.2, "lng_max": -77.0},
        ]
        
        for city in city_mappings:
            if (city["lat_min"] <= lat <= city["lat_max"] and 
                city["lng_min"] <= lng <= city["lng_max"]):
                return city["name"]
        
        # Default to a generic region if no specific city found
        return "default_region"
    
    def get_redis_shard_for_driver_location(self, driver_id: str, lat: float, lng: float) -> Optional[Dict[str, Any]]:
        """Get Redis shard for driver based on their geographic location"""
        try:
            city = self.get_city_from_coordinates(lat, lng)
            log.debug(f"Driver {driver_id} at ({lat}, {lng}) mapped to city: {city}")
            return self.discover_redis_shard_by_city(city)
        except Exception as e:
            log.error(f"Error getting Redis shard for driver location: {e}")
            return None
    
    def clear_cache(self):
        """Clear all cached service and shard information"""
        self.service_cache.clear()
        self.cache_expiry.clear()
        self.redis_shard_cache.clear()
        log.info("Service discovery cache cleared")
    
    def _start_heartbeat(self):
        """Start heartbeat thread"""
        if self.heartbeat_thread is not None:
            return
            
        def heartbeat_loop():
            while self.registered:
                try:
                    response = requests.post(
                        f"{self.service_discovery_url}/heartbeat/{self.service_name}/{self.instance_id}",
                        timeout=5
                    )
                    
                    if response.status_code != 200:
                        log.warning(f"Heartbeat failed: {response.status_code}")
                        
                except Exception as e:
                    log.error(f"Error sending heartbeat: {e}")
                
                time.sleep(10)  # Send heartbeat every 10 seconds
        
        self.heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()
    
    def __del__(self):
        """Cleanup on object destruction"""
        if self.registered:
            self.deregister_service()

# City-based Redis shard utility functions
class LocationBasedRedisManager:
    def __init__(self, service_integration: ServiceIntegration):
        self.service_integration = service_integration
        self.redis_connections = {}  # Cache Redis connections per city
    
    def get_redis_for_city(self, city: str) -> Optional[Redis]:
        """Get or create Redis connection for a specific city"""
        if city not in self.redis_connections:
            redis_client = self.service_integration.get_redis_client_for_city(city)
            if redis_client:
                self.redis_connections[city] = redis_client
            else:
                log.error(f"Failed to create Redis connection for city: {city}")
                return None
        
        connection =  self.redis_connections.get(city)
        if connection:
            return connection
        else:
            log.error(f"No Redis connection found for city: {city}")
            return None

    
    def get_redis_for_location(self, lat: float, lng: float) -> Optional[Redis]:
        """Get Redis connection for a specific geographic location"""
        city = self.service_integration.get_city_from_coordinates(lat, lng)
        return self.get_redis_for_city(city)
    
    def update_driver_location_by_city(self, driver_id: str, lat: float, lng: float, city: str = None):
        """Update driver location in the appropriate city-based Redis shard"""
        try:
            if city is None:
                city = self.service_integration.get_city_from_coordinates(lat, lng)
            
            redis_client = self.get_redis_for_city(city)
            if redis_client:
                # Update location in city-specific shard
                redis_client.geoadd(f"driver_locations_{city}", (lng, lat, driver_id))
                redis_client.expire(f"driver_locations_{city}", 30)  # 30-second TTL
                log.debug(f"Updated driver {driver_id} location in city {city}")
                return True
            else:
                log.error(f"No Redis connection available for city {city}")
                return False
                
        except Exception as e:
            log.error(f"Error updating driver location by city: {e}")
            return False

    def get_pubsub_client(self) -> Optional[Redis]:
        """Get Redis Pub/Sub client for a specific city"""
        return self.service_integration.get_redis_pubsub_client()
    
# Global service integration instances
def create_service_integration(service_config: Dict[str, Any]) -> ServiceIntegration:
    """Create service integration instance"""
    return ServiceIntegration(
        service_name=service_config["service_name"],
        host=service_config["host"],
        port=service_config["port"],
        health_url=service_config["health_url"]
    )

def create_city_redis_manager(service_integration: ServiceIntegration) -> LocationBasedRedisManager:
    """Create city-based Redis manager"""
    return LocationBasedRedisManager(service_integration)