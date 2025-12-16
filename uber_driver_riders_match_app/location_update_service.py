import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from typing import Optional
from config import REDIS_SERVER, REDIS_PORT, DRIVER_LOCATIONS_KEY, LOCATION_TTL, LOCATION_SERVICE_NAME , LOCATION_SERVICE_PORT, DRIVER_METADATA_KEY
from commons import logger
import json
import time
from service_integration import LocationBasedRedisManager, create_service_integration


log = logger.setup_logger(__name__)

app = FastAPI()

# Service integration
service_integration = create_service_integration(
    {
        "service_name": LOCATION_SERVICE_NAME,
        "host": "localhost", # replace with actual host if needed
        "port": LOCATION_SERVICE_PORT,
        "health_url": "/health",
    }
)

# City-based Redis manager for sharding
redis_manager = LocationBasedRedisManager(service_integration)

# Redis keys


class LocationUpdate(BaseModel):
    driver_id: str
    lat: float
    lng: float
    car_type: str
    payment_preference: str
    timestamp: Optional[str] = None

class LocationUpdateResponse(BaseModel):
    success: bool
    message: str
    driver_id: str
    shard_info: Optional[dict] = None

class DriverStatus(BaseModel):
    driver_id: str
    location: dict
    metadata: dict
    last_updated: str

def get_redis_client_for_location(lat: float, lng: float):
    """Get Redis client for specific geographic location"""
    try:
        # Get city-based Redis connection
        redis_client = redis_manager.get_redis_for_location(lat, lng)
        if redis_client:
            city = service_integration.get_city_from_coordinates(lat, lng)
            return redis_client, city
        else:
            # Fall back to default Redis
            log.warning("Using default Redis connection as fallback")
            raise Exception("City-based Redis connection not found")
    except Exception as e:
        log.error(f"Error getting Redis client for location ({lat}, {lng}): {e}")
        raise e

def update_driver_location(driver_id: str, lat: float, lng: float):
    """Update driver location in appropriate Redis shard based on city"""
    try:
        # Get Redis client for this location
        redis_client, city = get_redis_client_for_location(lat, lng)
        print("city:", city , " lat:", lat, " lng:", lng, " driver_id:", driver_id)
        # Use city-specific key for better sharding
        city_specific_key = f"{DRIVER_LOCATIONS_KEY}_{city}"
        
        # Add to geospatial index in the appropriate shard
        redis_client.geoadd(city_specific_key, (lng, lat, driver_id))
        # Set TTL for the city-specific geo index
        redis_client.expire(city_specific_key, LOCATION_TTL)
        
        log.debug(f" city_specific_key : {city_specific_key} Updated driver {driver_id} location in city {city} shard")
                
        return True, city
    except Exception as e:
        log.error(f"Error updating driver location: {e}")
        return False, None

def update_driver_metadata(driver_id: str, car_type: str, payment_preference: str, timestamp: str, city: str):
    """Update driver metadata in appropriate Redis shard"""
    try:
        metadata = {
            'car_type': car_type,
            'payment_preference': payment_preference,
            'last_updated': timestamp,
            'status': 'available',
            'city': city
        }
        print("city:", city , " driver_id:", driver_id)
        # Use city-specific Redis if available
        if city and city != "default":
            redis_client = redis_manager.get_redis_for_city(city)
            if redis_client:
                city_specific_key = f"{DRIVER_METADATA_KEY}_{city}"
                redis_client.hset(city_specific_key, driver_id, json.dumps(metadata))
                redis_client.expire(city_specific_key, LOCATION_TTL * 2)
                log.debug(f"city_specific_key : {city_specific_key} Updated driver {driver_id} metadata in city {city} shard")

        return True
    except Exception as e:
        log.error(f"Error updating driver metadata: {e}")
        return False

@app.post("/update_location", response_model=LocationUpdateResponse)
async def update_location(location_data: LocationUpdate):
    """Update driver location and metadata using city-based sharding"""
    try:
        driver_id = location_data.driver_id
        lat = location_data.lat
        lng = location_data.lng
        car_type = location_data.car_type.lower()
        payment_preference = location_data.payment_preference.lower()
        timestamp = location_data.timestamp or str(int(time.time()))
        
        # Validate car_type
        valid_car_types = ['economy', 'premium', 'luxury']
        if car_type not in valid_car_types:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid car_type. Must be one of: {', '.join(valid_car_types)}"
            )
        
        # Validate payment_preference
        valid_payment_prefs = ['cash', 'card', 'both']
        if payment_preference not in valid_payment_prefs:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid payment_preference. Must be one of: {', '.join(valid_payment_prefs)}"
            )
        
        # Validate coordinates
        if not (-90 <= lat <= 90):
            raise HTTPException(status_code=400, detail="Latitude must be between -90 and 90")
        if not (-180 <= lng <= 180):
            raise HTTPException(status_code=400, detail="Longitude must be between -180 and 180")
        
        # Get city and Redis shard information
        city = service_integration.get_city_from_coordinates(lat, lng)
        shard_info = service_integration.get_redis_shard_for_driver_location(driver_id, lat, lng)
        
        # Update location in appropriate shard
        location_success, assigned_city = update_driver_location(driver_id, lat, lng)
        if not location_success:
            raise HTTPException(status_code=500, detail="Failed to update location")
        
        # Update metadata
        metadata_success = update_driver_metadata(driver_id, car_type, payment_preference, timestamp, assigned_city)
        if not metadata_success:
            raise HTTPException(status_code=500, detail="Failed to update metadata")
        
        log.info(f"Location updated for driver {driver_id} at (lat {lat}, lng {lng}) in city {assigned_city} with {car_type} car")
        
        # Prepare shard info for response
        shard_response = None
        if shard_info:
            shard_response = {
                'shard_id': shard_info['shard']['shard_id'],
                'city': city,
                'connection_string': shard_info.get('connection_string')
            }
        
        return LocationUpdateResponse(
            success=True,
            message=f"Location and metadata updated successfully for driver {driver_id} in city {assigned_city}",
            driver_id=driver_id,
            shard_info=shard_response
        )
        
    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Error in update_location: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test default Redis connection
        pubsub_client = redis_manager.get_pubsub_client()
        pubsub_client.ping()
        
        # Test a few city-specific connections
        city_health = {}
        for city in ["ashburn", "herndon"]:
            try:
                redis_client = redis_manager.get_redis_for_city(city)
                if redis_client:
                    redis_client.ping()
                    city_health[city] = "healthy"
                else:
                    city_health[city] = "unavailable"
            except Exception as e:
                city_health[city] = f"unhealthy: {str(e)}"
        
        return {
            "status": "healthy",
            "service": "location_service", 
            "instance_id": service_integration.instance_id,
            "redis_default": "connected",
            "city_shards": city_health,
            "sharding_enabled": True
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "service": "location_service",
            "instance_id": service_integration.instance_id,
            "redis_default": "disconnected",
            "error": str(e)
        }

@app.on_event("startup")
async def startup_event():
    """Register service on startup"""
    metadata = {
        "version": "1.0",
        "capabilities": ["location_update", "driver_metadata", "geospatial_search", "city_sharding"],
        "redis_config": {"host": REDIS_SERVER, "port": REDIS_PORT},
        "sharding_strategy": "city_based"
    }
    service_integration.register_service(metadata)

@app.on_event("shutdown")
async def shutdown_event():
    """Deregister service on shutdown"""
    service_integration.deregister_service()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=LOCATION_SERVICE_PORT)