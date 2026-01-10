from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import redis
import pymysql
import json
import asyncio
from datetime import datetime
from commons import logger
import os
import pymongo

log = logger.setup_logger(__name__)

app = FastAPI()

# Redis for pubsub
from setup.redis_setup import config as redis_config, constants as redis_constants

redis_client = redis.Redis(
    host=redis_config.configurations[redis_constants.REDIS_SERVER],
    port=redis_config.configurations[redis_constants.REDIS_PORT],
    db=0,
)

from setup.mysql_setup import config as mysql_config, constants as mysql_constants


# MySQL connection
def get_db_connection():
    return pymysql.connect(
        host=mysql_config.configurations[mysql_constants.HOST],
        user=mysql_config.configurations[mysql_constants.USER],
        password=mysql_config.configurations[mysql_constants.PASSWORD],
        port=mysql_config.configurations[mysql_constants.PORT],
        database="slack_db",
        charset="utf8mb4",
    )

def setup_db():
    connection =  pymysql.connect(
        host=mysql_config.configurations[mysql_constants.HOST],
        user=mysql_config.configurations[mysql_constants.USER],
        password=mysql_config.configurations[mysql_constants.PASSWORD],
        port=mysql_config.configurations[mysql_constants.PORT],
        charset="utf8mb4",
    )
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE DATABASE IF NOT EXISTS slack_db
        """
        )
    connection.commit()
    connection.close()

# MongoDB connection
from setup.mongodb_setup import config as mongodb_config, constants as mongodb_constants

mongo_client = pymongo.MongoClient(
    mongodb_config.configurations[mongodb_constants.SERVER],
    mongodb_config.configurations[mongodb_constants.PORT],
)
mongo_db = mongo_client["slack_db"]
offline_collection = mongo_db["offline_messages"]

# Create directories
os.makedirs("slack_application/templates", exist_ok=True)
os.makedirs("slack_application/static", exist_ok=True)

templates = Jinja2Templates(directory="slack_application/templates")
app.mount("/static", StaticFiles(directory="slack_application/static"), name="static")

# Connected WebSocket clients: {user_id: [websockets]}
connected_clients = {}
user_locks = {}


# Initialize DB
def init_db():
    setup_db()
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute(
            """
            DROP TABLE IF EXISTS messages CASCADE
        """
        )
        cursor.execute(
            """
            DROP TABLE IF EXISTS group_members CASCADE
        """
        )
        cursor.execute(
            """
            DROP TABLE IF EXISTS slack_groups CASCADE
        """
        )
        cursor.execute(
            """
            DROP TABLE IF EXISTS slack_users CASCADE
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS slack_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS slack_groups (
                id INT AUTO_INCREMENT PRIMARY KEY,
                groupname VARCHAR(255) UNIQUE NOT NULL,
                created_by INT,
                type INT,
                FOREIGN KEY (created_by) REFERENCES slack_users(id)
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS group_members (
                group_id INT,
                user_id INT,
                PRIMARY KEY (group_id, user_id),
                FOREIGN KEY (group_id) REFERENCES slack_groups(id),
                FOREIGN KEY (user_id) REFERENCES slack_users(id)
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INT AUTO_INCREMENT PRIMARY KEY,
                group_id INT NULL,     -- For group messages
                content TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (group_id) REFERENCES slack_groups(id)
            )
            """
        )

        # Check and create index on slack_groups (groupname)
        cursor.execute(
            """
            SELECT COUNT(*) FROM information_schema.STATISTICS 
            WHERE TABLE_SCHEMA = 'slack_db' AND TABLE_NAME = 'slack_groups' AND INDEX_NAME = 'idx_groupname'
            """
        )
        if cursor.fetchone()[0] == 0:
            cursor.execute(
                """
                CREATE INDEX idx_groupname ON slack_groups (groupname)
                """
            )

        # Check and create index on group_members (user_id)
        cursor.execute(
            """
            SELECT COUNT(*) FROM information_schema.STATISTICS 
            WHERE TABLE_SCHEMA = 'slack_db' AND TABLE_NAME = 'group_members' AND INDEX_NAME = 'idx_group_members_user_id'
            """
        )
        if cursor.fetchone()[0] == 0:
            cursor.execute(
                """
                CREATE INDEX idx_group_members_user_id ON group_members (user_id)
                """
            )
    conn.commit()
    conn.close()


init_db()


def get_offline_members(cursor, group_id: int) -> list[int]:
    cursor.execute(
        """
        SELECT user_id FROM group_members WHERE group_id = %s
    """,
        (group_id,),
    )
    members = [row[0] for row in cursor.fetchall()]
    offline_members = []
    for user_id in members:
        if not redis_client.get(str(user_id)):
            offline_members.append(user_id)
    log.info(f"Offline members for group {group_id}: {offline_members}")
    return offline_members


async def broadcast_online_users():
    """Broadcast the current online users to all connected clients."""
    try:
        keys = redis_client.keys("online_users:*")
        online_users = [key.decode("utf-8") for key in keys if redis_client.get(key)]
        message = json.dumps({"type": "online_users", "users": online_users})
        for user_id in connected_clients.keys():
            # if user_id has multiple connections, send to all
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
        log.error("Error broadcasting",e)


@app.get("/", response_class=HTMLResponse)
async def get_home():
    return templates.TemplateResponse("index.html", {"request": {}})


@app.post("/register")
async def register_user(username: str = Form(...)):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM slack_users WHERE username = %s", (username,)
            )
            result = cursor.fetchone()
            if result:
                user_id = result[0]
            else:
                cursor.execute(
                    "INSERT INTO slack_users (username) VALUES (%s)", (username,)
                )
                user_id = cursor.lastrowid
        conn.commit()
        return {"user_id": user_id, "username": username}
    except pymysql.IntegrityError:
        raise HTTPException(status_code=400, detail="Username already exists")
    finally:
        conn.close()


@app.post("/create_group")
async def create_group(
    groupName: str = Form(...),
    created_by: int = Form(...),
    user_ids: str = Form(...),
    type: int = 1,
):
    user_ids_list = [int(uid.strip()) for uid in user_ids.split(",") if uid.strip()]
    if len(user_ids_list) < 1:
        raise HTTPException(status_code=400, detail="Group must have at least 1 user")
    user_ids_list.append(created_by)  # Include creator
    user_ids_list = list(set(user_ids_list))  # Remove duplicates
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id FROM slack_groups WHERE groupName  = %s ", (groupName)
            )
            result = cursor.fetchone()

            if not result:
                cursor.execute(
                    "INSERT INTO slack_groups (groupName, created_by, type) VALUES (%s, %s , %s )",
                    (groupName, created_by, type),
                )
                group_id = cursor.lastrowid
            else:
                group_id = result[0]

            for uid in user_ids_list:
                cursor.execute(
                    "INSERT INTO group_members (group_id, user_id) VALUES (%s, %s)",
                    (group_id, uid),
                )
        conn.commit()
        # Publish refresh to all group members
        for uid in user_ids_list:
            redis_client.publish(f"user_{uid}", json.dumps({"type": "refresh"}))
            print(f"""published refresh to redis pubsub : user_{uid} """)

        group_data = {"group_id": group_id, "groupName": groupName}
        log.info(f"create group  : {group_data}")
        return group_data
    except pymysql.IntegrityError:
        raise HTTPException(
            status_code=400, detail="Group name already exists or invalid users"
        )
    finally:
        conn.close()


@app.post("/send_message")
async def send_message(
    sender_id: int = Form(...),
    # The above code is a Python code snippet with the comment "content" and "
    content: str = Form(...),
    receiver_id: int = Form(None),
    group_id: int = Form(None),
):
    log.info(f" send_message : {sender_id} {content} {receiver_id} {group_id}")
    if not receiver_id and not group_id:
        raise HTTPException(
            status_code=400, detail="Must specify receiver_id or group_id"
        )
    if receiver_id and group_id:
        raise HTTPException(
            status_code=400, detail="Cannot specify both receiver_id and group_id"
        )
    # can use connection-pool
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:

            if receiver_id:
                log.info(
                    f"Get or create one2one group for user1 : {sender_id} and user2 : {receiver_id}"
                )
                cursor.execute(
                    "SELECT id FROM slack_groups WHERE groupName in (%s, %s ) ",
                    (
                        f"ONE_TO_ONE_{sender_id}_{receiver_id}",
                        f"ONE_TO_ONE_{receiver_id}_{sender_id}",
                    ),
                )
                result = cursor.fetchone()

                if not result:
                    log.info(
                        f"Creating one2one group for user1 :  {sender_id} and user2 : {receiver_id}"
                    )
                    group_data = await create_group(
                        groupName=f"ONE_TO_ONE_{sender_id}_{receiver_id}",
                        created_by=sender_id,
                        user_ids=f"{sender_id},{receiver_id}",
                        type=2,
                    )
                    group_id = group_data["group_id"]
                else:
                    log.info(
                        f"use exiting one2one group  for user1 : {sender_id} and user2 : {receiver_id}"
                    )
                    group_id = result[0]

            log.info(f"Sending msg to group : {group_id} content : {content}")

            cursor.execute(
                "SELECT COUNT(*) FROM group_members WHERE group_id = %s AND user_id = %s",
                (group_id, sender_id),
            )

            if cursor.fetchone()[0] == 0:
                raise HTTPException(status_code=403, detail="Not a member of the group")
            # persisting all messages to mysql, can be removed if not needed
            cursor.execute(
                "INSERT INTO messages (group_id, content) VALUES ( %s, %s)",
                (group_id, content),
            )
            message_id = cursor.lastrowid
            log.info(f"created : message_id : {message_id} : content : {content}")

            offline_members = get_offline_members(cursor, group_id)
            for member_id in offline_members:
                # Save message to MongoDB for offline user
                offline_collection.insert_one(
                    {
                        "user_id": member_id,
                        "message_data": {
                            "type": "message",
                            "message_id": message_id,
                            "group_id": group_id,
                            "sender_id": sender_id,
                            "content": content,
                            "timestamp": str(datetime.now()),
                        },
                    }
                )
                log.info(f"Saved offline message for user {member_id}")

        conn.commit()
    finally:
        conn.close()

    # Publish to Redis pubsub
    channel = f"group_{group_id}"
    message_data = {
        "type": "message",
        "message_id": message_id,
        "group_id": group_id,
        "sender_id": sender_id,
        "content": content,
        "timestamp": str(datetime.now()),
    }
    redis_client.publish(channel, json.dumps(message_data))
    print(f"published to to redis pubsub : {channel}")
    return {"message_id": message_id}


@app.websocket("/ws/{user_id}")
async def websocket_endpoint(websocket: WebSocket, user_id: int):
    await websocket.accept()
    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Lock()
    async with user_locks[user_id]:
        if user_id not in connected_clients:
            connected_clients[user_id] = []
        connected_clients[user_id].append(websocket)

    log.info(f"User {user_id} connected")

    # Send offline messages
    offline_messages = list(offline_collection.find({"user_id": user_id}))
    for msg_doc in offline_messages:
        try:
            msg_data = json.dumps(msg_doc["message_data"])
            await websocket.send_text(msg_data)
            log.info(f"Sent offline message to user {user_id}: {msg_doc['message_data']}")
        except Exception as e:
            log.error(f"Error sending offline message to user {user_id}: {e}")
    # Delete offline messages after sending
    offline_collection.delete_many({"user_id": user_id})
    log.info(f"Deleted offline messages for user {user_id}")

    # Subscribe to group channels the user is in
    pubsub = redis_client.pubsub()
    subscribed_groups = set()

    pubsub.subscribe(f"user_{user_id}")
    log.info(f"User {user_id} subscribe user_{user_id}")

    def refresh_subscriptions():
        log.info(f"User {user_id} refresh_subscriptions......")
        nonlocal subscribed_groups
        conn = get_db_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT group_id FROM group_members WHERE user_id = %s", (user_id,)
                )
                current_groups = {group[0] for group in cursor.fetchall()}
                log.info(f"current_groups : {current_groups}")
                # Subscribe to new groups
                for group_id in current_groups - subscribed_groups:
                    pubsub.subscribe(f"group_{group_id}")
                    log.info(f"User {user_id} subscribe group_{group_id}")
                    subscribed_groups.add(group_id)
                # Unsubscribe from old groups (optional, but for completeness)
                for group_id in subscribed_groups - current_groups:
                    pubsub.unsubscribe(f"group_{group_id}")
                    log.info(f"User {user_id} unsubscribe group_{group_id}")
                    subscribed_groups.remove(group_id)
        finally:
            conn.close()
        log.info(f"User {user_id} refresh_subscriptions completed")

    refresh_subscriptions()  # Initial subscription

    # Set initial online status
    ttl_time_in_sec = 10
    redis_client.setex(str(user_id), ttl_time_in_sec, "online")
    await broadcast_online_users()

    try:
        while True:
            # Listen for pubsub messages asynchronously
            message = await asyncio.to_thread(pubsub.get_message)
            if message and message["type"] == "message":
                log.info(f"User {user_id} received msg {message}")
                data = json.loads(message["data"])
                if data.get("type") == "refresh":
                    refresh_subscriptions()
                else:
                    await websocket.send_text(json.dumps(data))

            # Handle heartbeat with timeout
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=2.0)
                if data == "heartbeat":
                    redis_client.setex(str(user_id), ttl_time_in_sec, "online")
                    await websocket.send_text(json.dumps({"type": "heartbeat_ack"}))
            except asyncio.TimeoutError:
                # No heartbeat received, continue loop
                log.info(f"User {user_id} no msg received ")

    except WebSocketDisconnect:
        log.info(f"User {user_id} disconnected")
    except Exception as e:
        log.error(f"User {user_id} Exception: {e}")
    finally:
        if user_id in connected_clients:
            connected_clients[user_id].remove(websocket)
            if not connected_clients[user_id]:
                del connected_clients[user_id]
        pubsub.close()


async def cleanup_expired_users():
    log.info("cleanup_expired_users.....")
    while True:
        await asyncio.sleep(5)  # Check every 5 seconds
        await broadcast_online_users()


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(cleanup_expired_users())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
