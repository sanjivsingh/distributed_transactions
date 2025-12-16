import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


from  redis import Redis
import json
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from typing import Optional
from config import DRIVER_LOCATIONS_KEY, RIDE_REQUESTS_KEY, PUBSUB_CHANNEL, MATCH_SERVICE_NAME , MATCH_SERVICE_PORT, DRIVER_METADATA_KEY
from commons import logger
from service_integration import LocationBasedRedisManager, create_service_integration

app = FastAPI()
log = logger.setup_logger(__name__)

# Initialize service discovery integration
service_integration = create_service_integration(
    {
        "service_name": MATCH_SERVICE_NAME,
        "host": "localhost", # replace with actual host if needed
        "port": MATCH_SERVICE_PORT,
        "health_url": "/health",
    }
)

# City-based Redis manager for sharding
redis_manager = LocationBasedRedisManager(service_integration)


# Register match service
service_integration.register_service(metadata={
    "version": "1.0",
    "type": "core_service",
    "capabilities": ["driver_matching", "ride_assignment", "lua_scripts", "city_sharding"]
})

class MatchRequest(BaseModel):
    driver_id: str
    ride_id: str
    action: str  # 'accept' or 'reject'

class MatchResponse(BaseModel):
    success: bool
    message: str
    ride_id: str
    driver_id: Optional[str] = None

# Service URL helpers using service discovery
def get_ride_estimate_service_url() -> Optional[str]:
    """Get ride estimate service URL from service discovery"""
    try:
        service_url = service_integration.get_service_url("ride_estimate_service")
        print(f"using ride_estimate_service_url: {service_url}")
        return service_url
    except Exception as e:
        log.warning(f"Service discovery failed for ride_estimate_service, using fallback: {e}")
        return "http://localhost:8003"  # Fallback

def get_redis_client_for_city(city: str)-> Optional[Redis]:
    """Get Redis client for specific city"""
    try:
        return redis_manager.get_redis_for_city(city)
    except Exception as e:
        log.error(f"Error getting Redis client for city {city}: {e}")
        return None

# Updated Lua script for city-based sharding with proper argument handling
script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "find_matching_drivers_script.lua")
with open(script_path, "r") as lua_file:
    FIND_MATCHING_DRIVERS_SCRIPT = lua_file.read()

print(FIND_MATCHING_DRIVERS_SCRIPT)

script_hashes = {}  # Dict[str, str] - redis_client_id -> script_sha

def get_or_load_script_hash(redis_client: Redis) -> str:
    """Get cached script hash or load it if not available"""
    client_id = f"{redis_client.connection_pool.connection_kwargs['host']}:{redis_client.connection_pool.connection_kwargs['port']}"
    
    if client_id not in script_hashes:
        try:
            # Load the script and store the hash
            script_hash = redis_client.script_load(FIND_MATCHING_DRIVERS_SCRIPT)
            script_hashes[client_id] = script_hash
            log.info(f"Loaded Lua script for Redis {client_id}, hash: {script_hash}")
        except Exception as e:
            log.error(f"Failed to load Lua script for Redis {client_id}: {e}")
            raise
    
    return script_hashes[client_id]

def find_nearby_drivers_with_criteria(lat, lng, car_type, payment_preference='both', radius=5, count=5):
    """Find nearby drivers that match ride criteria using city-based sharding and Lua script"""
    try:
        # Determine city based on coordinates
        city = service_integration.get_city_from_coordinates(lat, lng)
        log.debug(f"Searching for drivers in city {city} at ({lat}, {lng})")
        
        # Get Redis client for the specific city
        city_redis = get_redis_client_for_city(city)
        
        # Use city-specific keys
        city_locations_key = f"{DRIVER_LOCATIONS_KEY}_{city}"
        city_metadata_key = f"{DRIVER_METADATA_KEY}_{city}"
        
        # Get or load script hash for this Redis client
        script_hash = get_or_load_script_hash(city_redis)
        
        # Convert all arguments to strings as required by Redis Lua
        args = [
            str(lng),           # longitude as string
            str(lat),           # latitude as string  
            str(radius),        # radius as string
            str(count),         # count as string
            str(car_type),      # car_type as string
            str(payment_preference)  # payment_preference as string
        ]
        
        keys = [city_locations_key, city_metadata_key]
        
        print("keys", city_locations_key, city_metadata_key)
        print("args", args)
        
        # Execute Lua script using EVALSHA with fallback
        try:
            # Execute using EVALSHA (faster - script already loaded)
            result = city_redis.evalsha(script_hash, len(keys), *keys, *args)
            print("lua result:", result)
            
        except redis.exceptions.NoScriptError:
            # Fallback: Script not in Redis cache, reload and retry
            log.warning(f"Script not found in Redis cache, reloading for client {city_redis}")
            
            # Remove cached hash and reload
            client_id = f"{city_redis.connection_pool.connection_kwargs['host']}:{city_redis.connection_pool.connection_kwargs['port']}"
            if client_id in script_hashes:
                del script_hashes[client_id]
            
            # Reload and retry
            new_script_hash = get_or_load_script_hash(city_redis)
            result = city_redis.evalsha(new_script_hash, len(keys), *keys, *args)
            print("lua result (after reload):", result)
        
        drivers = []
        # Process result: [driver_id, distance, lng, lat, metadata_json, ...]
        for i in range(0, len(result), 5):
            if i + 4 < len(result):
                driver_info = {
                    'driver_id': result[i].decode('utf-8') if isinstance(result[i], bytes) else str(result[i]),
                    'distance_miles': round(float(result[i + 1]), 2),
                    'coordinates': {
                        'lng': float(result[i + 2]),
                        'lat': float(result[i + 3])
                    },
                    'metadata': json.loads(result[i + 4].decode('utf-8') if isinstance(result[i + 4], bytes) else result[i + 4]),
                    'city': city,
                    'shard': 'city_specific'
                }
                drivers.append(driver_info)
        
        log.info(f"Found {len(drivers)} matching drivers in city {city} and nearby areas")
        return drivers
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        log.error(f"Error finding nearby drivers with criteria: {e}")
        return []

def publish_ride_offer(driver_id, ride_details):
    """Publish ride offer to a specific driver via Redis PubSub"""
    try:
        message = {
            'driver_id': driver_id,
            'type': 'ride_offer',
            'payload': ride_details
        }
        
        # Publish to default Redis for PubSub (could be enhanced to use city-specific channels)
        redis_pubsub = service_integration.get_redis_pubsub_client()
        redis_pubsub.publish(PUBSUB_CHANNEL, json.dumps(message))
        log.info(f"Ride offer published to driver {driver_id}")
    except Exception as e:
        log.error(f"Error publishing ride offer: {e}")

def get_ride_details(ride_id: str):
    """Get ride details from the ride estimate service"""
    try:
        service_url = get_ride_estimate_service_url()
        if not service_url:
            log.error("Unable to discover ride estimate service")
            return None
        
        print(f"using ride_estimate_service_url: {service_url}")
        response = requests.get(f"{service_url}/ride/{ride_id}", timeout=5)
        if response.status_code == 200:
            return response.json()
        else:
            log.error(f"Failed to get ride details: {response.status_code}")
            return None
    except Exception as e:
        log.error(f"Error getting ride details: {e}")
        return None

def update_ride_status(ride_id: str, status: str, driver_id: Optional[str] = None):
    """Update ride status in the ride estimate service"""
    try:
        service_url = get_ride_estimate_service_url()
        if not service_url:
            log.error("Unable to discover ride estimate service")
            return False
        
        print(f"using ride_estimate_service_url: {service_url}")
        params = {'status': status}
        if driver_id:
            params['driver_id'] = driver_id
        
        response = requests.put(
            f"{service_url}/ride/{ride_id}/status", 
            params=params,
            timeout=5
        )
        return response.status_code == 200
    except Exception as e:
        log.error(f"Error updating ride status: {e}")
        return False

@app.post("/find_match/{ride_id}")
async def find_match(ride_id: str):
    """Find drivers for a specific ride and send offers using city-based sharding and Lua script"""
    try:
        log.info(f"Finding match for ride {ride_id}")
        
        ride_estimate_service_url = get_ride_estimate_service_url()
        if not ride_estimate_service_url:
            log.error("Unable to discover ride estimate service")
            return None

        # Get ride details from ride estimate service
        ride_details = get_ride_details(ride_id)
        if not ride_details:
            raise HTTPException(status_code=404, detail="Ride not found")
        
        # Check if ride is still available
        if ride_details.get('status') != 'pending':
            raise HTTPException(
                status_code=400, 
                detail=f"Ride is not available for matching. Current status: {ride_details.get('status')}"
            )
        
        # Extract ride criteria and location
        car_type = ride_details.get('car_type', 'economy')
        payment_preference = 'both'
        source_lat = ride_details['source_lat']
        source_lng = ride_details['source_lng']
        
        # Determine city for the ride
        city = service_integration.get_city_from_coordinates(source_lat, source_lng)
        log.info(f"Ride {ride_id} is in city {city}")
        
        # Find nearby drivers that match criteria using city-based sharding
        matching_drivers = find_nearby_drivers_with_criteria(
            source_lat, 
            source_lng,
            car_type,
            payment_preference,
            radius=100,
            count=5
        )
        
        # Mark as matching criteria
        for driver in matching_drivers:
            driver['match_criteria'] = True
        
        # Update ride status to 'matching'
        update_ride_status(ride_id, 'matching')
        
        # Send ride offers to matching drivers
        offers_sent = 0
        exact_matches = 0
        cities_involved = set()
        
        for driver in matching_drivers:
            driver_id = driver['driver_id']
            driver_city = driver.get('city', 'unknown')
            cities_involved.add(driver_city)
            
            # Count exact matches
            if driver.get('match_criteria', False):
                exact_matches += 1
            
            # Prepare ride offer payload
            ride_offer = {
                'ride_id': ride_id,
                'rider_id': ride_details['rider_id'],
                'source': {
                    'lat': ride_details['source_lat'],
                    'lng': ride_details['source_lng']
                },
                'destination': {
                    'lat': ride_details['dest_lat'],
                    'lng': ride_details['dest_lng']
                },
                'car_type': ride_details['car_type'],
                'estimated_cost': ride_details['estimated_cost'],
                'estimated_distance_km': ride_details['estimated_distance_km'],
                'estimated_duration_minutes': ride_details['estimated_duration_minutes'],
                'driver_distance_miles': driver['distance_miles'],
                'match_criteria': driver.get('match_criteria', False),
                'driver_metadata': driver.get('metadata', {}),
                'driver_city': driver_city,
                'shard_used': driver.get('shard', 'unknown')
            }
            
            # Publish offer to driver
            publish_ride_offer(driver_id, ride_offer)
            offers_sent += 1
        
        log.info(f"Ride matching initiated for {ride_id}, sent {offers_sent} offers ({exact_matches} exact matches) across cities: {cities_involved}")
        
        return {
            "message": "Ride offers sent to nearby drivers",
            "ride_id": ride_id,
            "search_city": city,
            "cities_involved": list(cities_involved),
            "drivers_found": len(matching_drivers),
            "exact_matches": exact_matches,
            "offers_sent": offers_sent,
            "criteria": {
                "car_type": car_type,
                "payment_preference": payment_preference
            },
            "services_used": {
                "ride_estimate_service": ride_estimate_service_url
            },
            "sharding_info": {
                "primary_city": city,
                "sharding_enabled": True
            },
            "drivers": matching_drivers
        }
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error in find_match for ride {ride_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/accept_ride")
async def accept_ride(match_request: MatchRequest):
    """Handle driver's response to ride offer"""
    try:
        driver_id = match_request.driver_id
        ride_id = match_request.ride_id
        action = match_request.action.lower()
        
        log.info(f"Driver {driver_id} {action} ride {ride_id}")
        
        if action not in ['accept', 'reject']:
            raise HTTPException(status_code=400, detail="Action must be 'accept' or 'reject'")
        
        if action == 'reject':
            return MatchResponse(
                success=True,
                message="Ride offer rejected",
                ride_id=ride_id
            )
        
        # Handle ride acceptance with Redis lock for race condition (use default Redis for global locks)
        lock_key = f"ride_lock_{ride_id}"
        default_redis = service_integration.get_redis_pubsub_client()
        lock = default_redis.lock(lock_key, timeout=10)
        
        try:
            # Try to acquire lock
            if lock.acquire(blocking=True, blocking_timeout=5):
                # Check if ride is still available (use default Redis for global state)
                ride_status = default_redis.hget(RIDE_REQUESTS_KEY, ride_id)
                redis_status_str = ride_status.decode('utf-8') if ride_status else None
                print("redis_status_str", redis_status_str)
                if not ride_status :
                    # Mark ride as taken
                    default_redis.hset(RIDE_REQUESTS_KEY, ride_id, f'taken_{driver_id}')
                    
                    # Update ride status in database
                    if update_ride_status(ride_id, 'matched', driver_id):
                        log.info(f"Ride {ride_id} successfully assigned to driver {driver_id}")
                        
                        # Publish confirmation to other drivers (rejection)
                        reject_message = {
                            'type': 'ride_unavailable',
                            'payload': {
                                'ride_id': ride_id,
                                'reason': 'Ride has been accepted by another driver'
                            }
                        }
                        default_redis.publish(PUBSUB_CHANNEL, json.dumps(reject_message))
                        
                        return MatchResponse(
                            success=True,
                            message="Ride successfully accepted",
                            ride_id=ride_id,
                            driver_id=driver_id
                        )
                    else:
                        # Delete Redis state if database update failed
                        default_redis.hdel(RIDE_REQUESTS_KEY, ride_id)
                        raise HTTPException(status_code=500, detail="Failed to update ride status")
                
                else:
                    driver_id = str(redis_status_str).replace("taken_", "")
                    # Ride already taken
                    return MatchResponse(
                        success=False,
                        message=f"Ride {ride_id} has already been accepted by another driver {driver_id}, can't allocate to drive {driver_id} ",
                        ride_id=ride_id
                    )
            else:
                raise HTTPException(status_code=409, detail="Could not acquire lock, try again")
                
        finally:
            # Always release the lock
            try:
                lock.release()
            except:
                pass  # Lock might have already expired
        
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        log.error(f"Error in accept_ride: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/ride/{ride_id}/status")
async def get_ride_status(ride_id: str):
    """Get current status of a ride"""
    try:
        # Check Redis status (use default Redis for global state)
        default_redis = service_integration.get_redis_pubsub_client()
        redis_status = default_redis.hget(RIDE_REQUESTS_KEY, ride_id)
        redis_status_str = redis_status.decode('utf-8') if redis_status else None
        
        # Get database status using service discovery
        ride_details = get_ride_details(ride_id)
        if not ride_details:
            raise HTTPException(status_code=404, detail="Ride not found")
        
        return {
            "ride_id": ride_id,
            "database_status": ride_details.get('status'),
            "redis_status": redis_status_str,
            "driver_id": ride_details.get('driver_id'),
            "service_urls": {
                "ride_estimate_service": get_ride_estimate_service_url()
            },
            "sharding_enabled": True
        }
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error getting ride status: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Check default Redis connectivity
        pubsub_client = redis_manager.get_pubsub_client()   
        pubsub_client.ping()

        # Check city-specific Redis connections
        city_health = {}
        for city in ["ashburn", "herndon", "reston"]:
            try:
                city_redis = get_redis_client_for_city(city)
                city_redis.ping()
                city_health[city] = "healthy"
            except Exception as e:
                city_health[city] = f"unhealthy: {str(e)}"
        
        # Check service discovery connectivity
        estimate_url = get_ride_estimate_service_url()
        return {
            "status": "healthy",
            "service": "match_service",
            "redis_default": "connected",
            "redis_city_shards": city_health,
            "discovered_services": {
                "ride_estimate_service": estimate_url
            },
            "capabilities": ["driver_matching", "ride_assignment", "lua_scripts", "city_sharding"],
            "sharding_enabled": True
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e)
        }

@app.get("/services/refresh")
async def refresh_service_cache():
    """Refresh service discovery cache"""
    try:
        service_integration.clear_cache()
        
        # Test new service URLs
        estimate_url = get_ride_estimate_service_url()
        
        return {
            "message": "Service cache refreshed",
            "discovered_services": {
                "ride_estimate_service": estimate_url
            },
            "sharding_enabled": True
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error refreshing cache: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=MATCH_SERVICE_PORT)