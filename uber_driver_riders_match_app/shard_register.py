import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import requests
import time
import redis
from typing import Dict, Any
from commons import logger
from config import REDIS_CITY_SHARDS, SERVICE_DISCOVERY_URL

log = logger.setup_logger(__name__)

class RedisShardRegistrar:
    def __init__(self):
        self.service_discovery_url = SERVICE_DISCOVERY_URL
        self.registered_shards = set()
        
    def test_redis_connection(self, host: str, port: int) -> bool:
        """Test if Redis shard is accessible"""
        try:
            r = redis.Redis(host=host, port=port, decode_responses=True)
            r.ping()
            log.info(f"Redis shard {host}:{port} is accessible")
            return True
        except Exception as e:
            log.warning(f"Redis shard {host}:{port} is not accessible: {e}")
            return False
    
    def register_shard(self, shard_id: str, host: str, port: int, weight: int = 100) -> bool:
        """Register a single Redis shard with service discovery"""
        try:
            # First test if Redis is accessible
            if not self.test_redis_connection(host, port):
                log.warning(f"Skipping registration of inaccessible shard: {shard_id}")
                return False
                
            shard_data = {
                "shard_id": shard_id,
                "host": host,
                "port": port,
                "weight": weight
            }
            
            response = requests.post(
                f"{self.service_discovery_url}/register/redis_shard",
                json=shard_data,
                timeout=5
            )
            
            if response.status_code == 200:
                self.registered_shards.add(shard_id)
                log.info(f"Successfully registered Redis shard: {shard_id} at {host}:{port}")
                return True
            else:
                log.error(f"Failed to register Redis shard {shard_id}: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            log.error(f"Network error registering Redis shard {shard_id}: {e}")
            return False
        except Exception as e:
            log.error(f"Error registering Redis shard {shard_id}: {e}")
            return False
    
    def register_all_shards(self) -> Dict[str, bool]:
        """Register all configured Redis shards"""
        results = {}
        
        log.info(f"Starting registration of {len(REDIS_CITY_SHARDS)} Redis shards...")
        
        for city, config in REDIS_CITY_SHARDS.items():
            shard_id = f"{city}"
            success = self.register_shard(
                shard_id=shard_id,
                host=config['host'],
                port=config['port'],
                weight=config.get('weight', 100)
            )
            results[city] = success
            
            if success:
                log.info(f"✓ Registered shard for city: {city}")
            else:
                log.error(f"✗ Failed to register shard for city: {city}")
        
        successful_registrations = sum(1 for success in results.values() if success)
        log.info(f"Registration complete: {successful_registrations}/{len(REDIS_CITY_SHARDS)} shards registered successfully")
        
        return results
    


def register_redis_shards():
    """Main function to register all Redis shards"""
    registrar = RedisShardRegistrar()
    
    log.info("=" * 60)
    log.info("REDIS SHARD REGISTRATION STARTING")
    log.info("=" * 60)
    
    # Register all shards
    registration_results = registrar.register_all_shards()
    
    # Wait a moment for registration to propagate
    time.sleep(2)

    # Summary
    successful_registrations = sum(1 for success in registration_results.values() if success)

    log.info("\n" + "=" * 60)
    log.info("REGISTRATION SUMMARY")
    log.info("=" * 60)
    log.info(f"Shards registered: {successful_registrations}/{len(REDIS_CITY_SHARDS)}")

    
    if successful_registrations == len(REDIS_CITY_SHARDS):
        log.info("✓ ALL REDIS SHARDS REGISTERED AND DISCOVERABLE!")
    else:
        log.warning("⚠ Some Redis shards failed to register or are not discoverable")
    
    return {
        "registration_results": registration_results,
    }

if __name__ == "__main__":
    try:
        results = register_redis_shards()
        
        # Print final status
        if all(results["registration_results"].values()):
            print("\n Redis shard registration completed successfully!")
        else:
            print("\n Some Redis shards failed to register.")
            failed_shards = [city for city, success in results["registration_results"].items() if not success]
            print(f"Failed shards: {', '.join(failed_shards)}")
            
    except KeyboardInterrupt:
        log.info("Registration interrupted by user")
    except Exception as e:
        log.error(f"Registration failed: {e}")
        raise