import asyncio
import json
import time
from dataclasses import dataclass
from typing import Dict
import pymysql
from pymysql.cursors import DictCursor

import uvicorn
from fastapi import FastAPI, HTTPException
from distributed_multitenant_task_scheduler.orchestrator import ShardManager
app = FastAPI()
from commons.uniqueid import SnowflakeIDGenerator

@dataclass
class Task:
    tenant_id: str
    payload: Dict
    priority: str
    schedule_ts: str

@dataclass
class ScheduledTask:
    tenant_id: str
    payload: Dict
    priority: str
    cron_schedule: str

shard_namager = ShardManager()
id_generator = SnowflakeIDGenerator(1)

def get_connection(tenant_id: str):
    shard_uri = shard_namager.get_shard_uri(tenant_id)
    host, port, user, password, db = shard_uri.split(":")
    print(""f"Getting connection for tenant {tenant_id} on shard {db}")
    conn = pymysql.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        database=db,
        cursorclass=DictCursor,
    )
    print(f"Established connection for tenant {tenant_id} on shard {db}")
    return conn

def insert_task(task: Task):
    try:
        shard_uri = shard_namager.get_shard_uri(task.tenant_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    conn = get_connection(task.tenant_id)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO executable_tasks (task_id, tenant_id, payload, priority, schedule_time, status)
                VALUES (%s, %s, %s, %s, %s, 'pending')
                """,
                (   id_generator.generate_id(),
                    task.tenant_id,
                    json.dumps(task.payload),
                    task.priority,
                    task.schedule_ts,
                ),
            )
            conn.commit()
            print(f"Inserted task : {task}")
    except Exception as exc:
        print(f"Error inserting task : {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()

def insert_scheduled_task(task: ScheduledTask):
    try:
        shard_uri = shard_namager.get_shard_uri(task.tenant_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    conn = get_connection(task.tenant_id)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO scheduled_tasks (task_id, tenant_id, payload, priority, cron_schedule, status)
                VALUES (%s, %s, %s, %s, %s, 'pending')
                """,
                (
                    id_generator.generate_id(),
                    task.tenant_id,
                    json.dumps(task.payload),
                    task.priority,
                    task.cron_schedule,
                ),
            )
            conn.commit()
            print(f"Inserted scheduled task : {task}")
    except Exception as exc:
        print(f"Error inserting scheduled task : {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()

@app.post("/schedule_tasks")
async def create_schedule_task(body: Dict):
    task = ScheduledTask(
        tenant_id=body["tenant_id"],
        payload=body["payload"],
        priority=body.get("priority", "medium"),
        cron_schedule=body["cron_schedule"],
    )
    try:
        insert_scheduled_task(task)
        return {"status": "queued", "code": 202}
    except Exception as exc:
        return {"status": "error", "detail": str(exc), "code": exc.status_code}
    
@app.post("/tasks")
async def create_task(body: Dict):
    task = Task(
        tenant_id=body["tenant_id"],
        payload=body["payload"],
        priority=body.get("priority", "medium"),
        schedule_ts=body["schedule_ts"] if "schedule_ts" in body else time.time(),
    )
    try:
        insert_task(task)
        return {"status": "queued", "code": 202}
    except Exception as exc:
        return {"status": "error", "detail": str(exc), "code": exc.status_code}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
