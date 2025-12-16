import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


import asyncio
import json
import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn
import threading
import requests
from config import (
    REDIS_SERVER,
    REDIS_PORT,
    PUBSUB_CHANNEL,
    WEBSOCKET_GATEWAY_SERVICE_NAME,
    WEBSOCKET_GATEWAY_SERVICE_PORT,
)
from service_integration import create_service_integration

app = FastAPI()

# Service integration
service_integration = create_service_integration(
    {
        "service_name": WEBSOCKET_GATEWAY_SERVICE_NAME,
        "host": "localhost", # replace with actual host if needed
        "port": WEBSOCKET_GATEWAY_SERVICE_PORT,
        "health_url": "/health",
    }
)


# In-memory map of driver_id to websocket
driver_connections = {}

# Redis pubsub for subscribing to ride offers
redis_client = redis.Redis(host=REDIS_SERVER, port=REDIS_PORT, db=0)


def pubsub_listener():
    pubsub = redis_client.pubsub()
    pubsub.subscribe(PUBSUB_CHANNEL)
    for message in pubsub.listen():
        if message["type"] == "message":
            data = json.loads(message["data"].decode("utf-8"))
            driver_id = data["driver_id"]
            if driver_id in driver_connections:
                asyncio.run(send_to_driver(driver_id, data))


async def send_to_driver(driver_id, data):
    websocket = driver_connections[driver_id]
    await websocket.send_text(json.dumps(data))


@app.websocket("/ws/{driver_id}")
async def websocket_endpoint(websocket: WebSocket, driver_id: str):
    await websocket.accept()
    driver_connections[driver_id] = websocket
    try:
        while True:
            data = await websocket.receive_text()
            # Handle location updates or accept messages
            message = json.loads(data)
            if message["type"] == "location":
                # Forward to location update service
                await forward_to_location_service(driver_id, message)
            elif message["type"] == "accept":
                # Forward to match service
                await forward_to_match_service(driver_id, message)
    except WebSocketDisconnect:
        del driver_connections[driver_id]


async def forward_to_location_service(driver_id, message):
    """Forward location update to Location Update Service using service discovery"""
    try:
        # Discover location service
        location_service_url = service_integration.get_service_url("location_service")
        if not location_service_url:
            print(f"Location service not available for driver {driver_id}")
            return

        payload = {
            "driver_id": driver_id,
            "lat": message.get("lat"),
            "lng": message.get("lng"),
            "car_type": message.get("car_type", "economy"),
            "payment_preference": message.get("payment_preference", "both"),
            "timestamp": message.get("timestamp"),
        }

        # Use asyncio.to_thread to run requests.post asynchronously
        response = await asyncio.to_thread(
            requests.post,
            f"{location_service_url}/update_location",
            json=payload,
            timeout=5,
        )

        if response.status_code == 200:
            print(f"Location updated for driver {driver_id}")
            # Send confirmation back to driver
            if driver_id in driver_connections:
                await driver_connections[driver_id].send_text(
                    json.dumps(
                        {
                            "type": "location_updated",
                            "status": "success",
                            "message": "Location updated successfully",
                        }
                    )
                )
        else:
            print(
                f"Failed to update location for driver {driver_id}: {response.status_code}"
            )
            # Send error back to driver
            if driver_id in driver_connections:
                await driver_connections[driver_id].send_text(
                    json.dumps(
                        {
                            "type": "location_update_error",
                            "status": "error",
                            "message": "Failed to update location",
                        }
                    )
                )
    except Exception as e:
        print(f"Error forwarding location for driver {driver_id}: {e}")
        # Send error back to driver
        if driver_id in driver_connections:
            await driver_connections[driver_id].send_text(
                json.dumps(
                    {
                        "type": "location_update_error",
                        "status": "error",
                        "message": "Error updating location",
                    }
                )
            )


async def forward_to_match_service(driver_id, message):
    """Forward ride acceptance to Match Service using service discovery"""
    try:
        # Discover match service
        match_service_url = service_integration.get_service_url("match_service")
        if not match_service_url:
            print(f"Match service not available for driver {driver_id}")
            return

        payload = {
            "driver_id": driver_id,
            "ride_id": message.get("ride_id"),
            "action": "accept",
        }

        # Use asyncio.to_thread to run requests.post asynchronously
        response = await asyncio.to_thread(
            requests.post, f"{match_service_url}/accept_ride", json=payload, timeout=5
        )

        # Send response back to driver
        if driver_id in driver_connections:
            if response.status_code == 200:
                await driver_connections[driver_id].send_text(
                    json.dumps(
                        {
                            "type": "ride_accepted",
                            "ride_id": message.get("ride_id"),
                            "status": "confirmed",
                        }
                    )
                )
            else:
                response_data = response.json() if response.content else {}
                await driver_connections[driver_id].send_text(
                    json.dumps(
                        {
                            "type": "ride_rejected",
                            "ride_id": message.get("ride_id"),
                            "reason": response_data.get(
                                "error", "Ride no longer available"
                            ),
                        }
                    )
                )

    except Exception as e:
        print(f"Error forwarding ride acceptance for driver {driver_id}: {e}")
        # Send error response to driver
        if driver_id in driver_connections:
            await driver_connections[driver_id].send_text(
                json.dumps(
                    {"type": "error", "message": "Failed to process ride acceptance"}
                )
            )


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": WEBSOCKET_GATEWAY_SERVICE_NAME,
        "instance_id": service_integration.instance_id,
        "connections": len(driver_connections),
    }


@app.on_event("startup")
async def startup_event():
    """Register service on startup"""
    metadata = {"version": "1.0", "protocol": "websocket", "max_connections": 10000}
    service_integration.register_service(metadata)

    # Start pubsub listener in a thread
    threading.Thread(target=pubsub_listener, daemon=True).start()


@app.on_event("shutdown")
async def shutdown_event():
    """Deregister service on shutdown"""
    service_integration.deregister_service()


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=WEBSOCKET_GATEWAY_SERVICE_PORT)
