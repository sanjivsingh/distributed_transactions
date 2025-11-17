from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import redis
import uvicorn
from commons import logger
import os

log = logger.setup_logger(__name__)

app = FastAPI()

# Redis connection
from setup.redis_setup import config as redis_config, constants as redis_constants
redis_client = redis.Redis(
    host=redis_config.configurations[redis_constants.REDIS_SERVER],
    port=redis_config.configurations[redis_constants.REDIS_PORT],
    db=0,
)

# Create directories if they don't exist
os.makedirs("online_status_application/templates", exist_ok=True)

# Templates and static files
templates = Jinja2Templates(directory="online_status_application/templates")

@app.get("/", response_class=HTMLResponse)
async def get_home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/heartbeat")
async def heartbeat(user_id: str = Form(...)):
    """Receive heartbeat from user and set in Redis with TTL 10 seconds."""
    try:
        redis_client.setex(user_id, 10, "online")
        log.info(f"Heartbeat received from user: {user_id}")
        return {"status": "success"}
    except Exception as e:
        log.error(f"Error setting heartbeat for {user_id}: {e}")
        return {"status": "error", "message": str(e)}

@app.get("/online_users")
async def get_online_users():
    """Get list of online users from Redis."""
    try:
        keys = redis_client.keys("*")
        online_users = [key.decode('utf-8') for key in keys if redis_client.get(key)]
        log.info(f"Online users: {online_users}")
        return {"online_users": online_users}
    except Exception as e:
        log.error(f"Error fetching online users: {e}")
        return {"online_users": [], "error": str(e)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)