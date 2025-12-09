import json
from datetime import datetime,timezone
from confluent_kafka import Producer
from fastapi.responses import  JSONResponse
from fastapi import FastAPI, HTTPException 
from pydantic import BaseModel

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import config
from datetime import datetime, timedelta
from realtime_event_streaming_distinct_users_range_search.search import SearchService

app = FastAPI()

# ---------- Fast Path Producer (ingest raw events) ----------
class UserActivity(BaseModel):
    user_id: str
    post_id: str
    activity : str

# ---------- Publish Service ----------
producer = Producer({"bootstrap.servers": config.KAFKA_BROKER})

def publish_user_activity(event: UserActivity):
    producer.produce(config.USER_ACTIVITY_TOPIC, json.dumps(event.dict()).encode())
    producer.flush(0)

@app.post("/user_activity")
def user_activity(event: UserActivity) -> JSONResponse:
    try:
        publish_user_activity(event)
        return JSONResponse(content={"status": "success", "status_code": 200})
    except Exception as e:
        raise HTTPException(500, f"Internal server error: {e}")

# ---------- Distinct Service ----------
class DistinctUserCountRequest(BaseModel):
    start_ts: str
    end_ts: str

from search import SearchService
searchService = SearchService()

@app.post("/distinct_users")
def distinct_users(req: DistinctUserCountRequest) -> JSONResponse:
    if req.start_ts >= req.end_ts:
        raise HTTPException(400, "start_ts must be < end_ts")

    try:
        start_date = datetime.strptime(req.start_ts, '%Y/%m/%d %H:%M')
    except Exception:
        raise HTTPException(400, "Invalid start_ts timestamp format")

    try:
        end_date = datetime.strptime(req.end_ts, '%Y/%m/%d %H:%M')
    except Exception:
        raise HTTPException(400, "Invalid end_ts timestamp format")

    try:
        distinct_users = searchService.search_distinct_users(req.start_ts, req.end_ts)
        return JSONResponse(content={"status": "success", "status_code": 200, "distinct_users": distinct_users})
    except Exception as e:
        raise HTTPException(500, f"Internal server error: {e}")


# Add imports for simulation
import random
import time
import threading

def simulate_user_activity():
    print("simulate_user_activity started")
    """
    Simulates publishing 10 user activity events per minute with random user IDs,
    post IDs, and activities.
    """
    activities = ["view", "like", "comment", "share"]
    while True:
        for _ in range(10):
            user_id = f"user_{random.randint(1, 100000):06d}"
            post_id = f"post_{random.randint(1, 100000):06d}"
            activity = random.choice(activities)
            event = UserActivity(user_id=user_id, post_id=post_id, activity=activity)
            try:
                publish_user_activity(event)
                print(f"Published event: {event.dict()}")
            except Exception as e:
                print(f"Error publishing event: {e}")
        time.sleep(60)  # Sleep for 1 minute (60 seconds) after 10 events

if __name__ == "__main__":
    print("Starting simulation thread")
    # Start the simulation in a separate daemon thread for background processing
    threading.Thread(target=simulate_user_activity, daemon=True).start()  

    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)



    