import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import redis
import uvicorn
from commons import logger
import os
import json
import asyncio

log = logger.setup_logger(__name__)

app = FastAPI()

# Redis connection
from setup.redis_setup import config as redis_config, constants as redis_constants
import os

redis_host = os.environ.get('REDIS_HOST', redis_config.configurations[redis_constants.REDIS_SERVER])
redis_port = int(os.environ.get('REDIS_PORT', redis_config.configurations[redis_constants.REDIS_PORT]))

redis_client = redis.Redis(
    host=redis_host,
    port=redis_port,
    db=0,
)

# Create directories if they don't exist
os.makedirs("online_status_application_websocket/templates", exist_ok=True)

# Templates and static files
templates = Jinja2Templates(directory="online_status_application_websocket/templates")

# Connected WebSocket clients: {user_id: [websocket1, websocket2, ...]}
connected_clients = {}

# User-specific locks: {user_id: asyncio.Lock}
user_locks = {}


@app.get("/", response_class=HTMLResponse)
async def get_home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


def get_user_key(user_id: str) -> str:
    return f"online_user:{user_id}"

async def broadcast_online_users():
    """Broadcast the current online users to all connected clients."""
    try:
        keys = redis_client.keys("online_user:*")
        online_users = [key.decode("utf-8").split(":")[1] for key in keys if redis_client.get(key)]
        message = json.dumps({"type": "online_users", "users": online_users})
        for user_id in connected_clients.keys():
            user_id_connections = connected_clients[user_id]
            disconnected_connections = []
            for client in user_id_connections:
                try:
                    await client.send_text(message)
                except Exception as e:
                    # Client may have disconnected
                    log.warning(f"error in sending msg : {e}")
                    # remove connection
                    disconnected_connections.append(client)
            for client in disconnected_connections:
                connected_clients[user_id].remove(client)
                if len(connected_clients[user_id]) == 0:
                    del connected_clients[user_id]
                    log.info(f"Delete webSocket connection : {user_id} - {client} ")

    except Exception as e:
        log.error(f"Error broadcasting: {e}")


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    await websocket.accept()
    # Get or create a lock for this user_id
    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Lock()
    async with user_locks[user_id]:
        if user_id not in connected_clients:
            connected_clients[user_id] = []
        connected_clients[user_id].append(websocket)
    log.info(
        f"Client {user_id} connected (total connections: {len(connected_clients[user_id])})"
    )

    try:
        # Set initial online status
        ttl_time_in_sec = 10
        redis_client.setex(get_user_key(user_id), ttl_time_in_sec, "online")
        await broadcast_online_users()

        while True:
            # Wait for heartbeat or any message
            data = await websocket.receive_text()
            if data == "heartbeat":
                # Refresh TTL
                redis_client.setex(get_user_key(user_id), ttl_time_in_sec, "online")
                log.info(f"Heartbeat from {user_id}")
                # Optionally send confirmation
                await websocket.send_text(json.dumps({"type": "heartbeat_ack"}))
            else:
                await websocket.send_text(
                    json.dumps({"type": "error", "message": "Unknown message"})
                )
    except WebSocketDisconnect:
        log.info(f"Client {user_id} disconnected")
    finally:
        if user_id in connected_clients:
            connected_clients[user_id].remove(websocket)
            if not connected_clients[user_id]:
                del connected_clients[user_id]
                # Clean up lock if no connections
                if user_id in user_locks:
                    del user_locks[user_id]
        # Note: User will go offline after TTL expires


# Background task to clean up expired users and broadcast
async def cleanup_expired_users():
    log.info("cleanup_expired_users.....")
    while True:
        await asyncio.sleep(5)  # Check every 5 seconds
        await broadcast_online_users()


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(cleanup_expired_users())


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
