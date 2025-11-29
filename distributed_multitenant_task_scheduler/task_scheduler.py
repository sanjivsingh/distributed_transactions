import asyncio
import json
import threading
import time
from dataclasses import dataclass
from typing import Dict
import pymysql
from pymysql.cursors import DictCursor

import kazoo.client
import uvicorn
from fastapi import FastAPI, HTTPException
from confluent_kafka import Producer as KafkaProducer, Consumer as KafkaConsumer
import config
from scheduler_orchestrator import ShardManager
app = FastAPI()


@dataclass
class Task:
    tenant_id: str
    payload: Dict
    priority: str
    schedule_ts: float

shard_namager = ShardManager()

async def insert_task(task: Task):
    try:
        shard_uri = shard_namager.get_shard_uri(task.tenant_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    host, port, user, password, db = shard_namager.get_shard_uri(task.tenant_id).split(",")
    async def _insert():
        conn = pymysql.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=db,
            autocommit=True,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO tasks (tenant_id, payload, priority, schedule_time, status)
                    VALUES (%s, %s, %s, FROM_UNIXTIME(%s), 'pending')
                    """,
                    (
                        task.tenant_id,
                        json.dumps(task.payload),
                        task.priority,
                        task.schedule_ts,
                    ),
                )
        finally:
            conn.close()

    await asyncio.to_thread(_insert)


@app.post("/tasks")
async def create_task(body: Dict):
    task = Task(
        tenant_id=body["tenant_id"],
        payload=body["payload"],
        priority=body.get("priority", "medium"),
        schedule_ts=body.get("schedule_ts", time.time()),
    )
    await insert_task(task)
    return {"status": "queued", "code": 202}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
