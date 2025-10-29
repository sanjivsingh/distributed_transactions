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

log = logger.setup_logger(__name__)

app = FastAPI()

# Redis for pubsub
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# MySQL connection
def get_db_connection():
    return pymysql.connect(host='localhost', user='root', password='rootadmin', database='slack_db', charset='utf8mb4')

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
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            DROP TABLE IF EXISTS messages CASCADE
        """)
        cursor.execute("""
            DROP TABLE IF EXISTS group_members CASCADE
        """)
        cursor.execute("""
            DROP TABLE IF EXISTS slack_groups CASCADE
        """)
        cursor.execute("""
            DROP TABLE IF EXISTS slack_users CASCADE
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS slack_users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS slack_groups (
                id INT AUTO_INCREMENT PRIMARY KEY,
                groupname VARCHAR(255) UNIQUE NOT NULL,
                created_by INT,
                type INT,
                FOREIGN KEY (created_by) REFERENCES slack_users(id)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS group_members (
                group_id INT,
                user_id INT,
                PRIMARY KEY (group_id, user_id),
                FOREIGN KEY (group_id) REFERENCES slack_groups(id),
                FOREIGN KEY (user_id) REFERENCES slack_users(id)
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INT AUTO_INCREMENT PRIMARY KEY,
                group_id INT NULL,     -- For group messages
                content TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (group_id) REFERENCES slack_groups(id)
            )
        """)
    conn.commit()
    conn.close()

init_db()

@app.get("/", response_class=HTMLResponse)
async def get_home():
    return templates.TemplateResponse("index.html", {"request": {}})

@app.post("/register")
async def register_user(username: str = Form(...)):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM slack_users WHERE username = %s", (username,))
            result = cursor.fetchone()
            if result:
                user_id = result[0]
            else:
                cursor.execute("INSERT INTO slack_users (username) VALUES (%s)", (username,))
                user_id = cursor.lastrowid
        conn.commit()
        return {"user_id": user_id, "username": username}
    except pymysql.IntegrityError:
        raise HTTPException(status_code=400, detail="Username already exists")
    finally:
        conn.close()

@app.post("/create_group")
async def create_group(groupName: str = Form(...), created_by: int = Form(...), user_ids: str = Form(...), type : int = 1):
    user_ids_list = [int(uid.strip()) for uid in user_ids.split(',') if uid.strip()]
    if len(user_ids_list) < 1 :
        raise HTTPException(status_code=400, detail="Group must have at least 1 user")
    user_ids_list.append(created_by)  # Include creator
    user_ids_list = list(set(user_ids_list))  # Remove duplicates
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO slack_groups (groupName, created_by, type) VALUES (%s, %s , %s )", (groupName, created_by, type))
            group_id = cursor.lastrowid
            for uid in user_ids_list:
                cursor.execute("INSERT INTO group_members (group_id, user_id) VALUES (%s, %s)", (group_id, uid))
        conn.commit()
        # Publish refresh to all group members
        for uid in user_ids_list:
            redis_client.publish(f"user_{uid}", json.dumps({"type": "refresh"}))
            print(f"""published refresh to redis pubsub : user_{uid} """)

        group_data  = {"group_id": group_id, "groupName": groupName}
        log.info(f"create group  : {group_data}")
        return group_data
    except pymysql.IntegrityError:
        raise HTTPException(status_code=400, detail="Group name already exists or invalid users")
    finally:
        conn.close()

@app.post("/send_message")
async def send_message(sender_id: int = Form(...), content: str = Form(...), receiver_id: int = Form(None), group_id: int = Form(None)):
    log.info(f" send_message : {sender_id} {content} {receiver_id} {group_id}")
    if not receiver_id and not group_id:
        raise HTTPException(status_code=400, detail="Must specify receiver_id or group_id")
    if receiver_id and group_id:
        raise HTTPException(status_code=400, detail="Cannot specify both receiver_id and group_id")

    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:

            if receiver_id:
                log.info(f"Get or create one2one group for user1 : {sender_id} and user2 : {receiver_id}")
                cursor.execute("SELECT id FROM slack_groups WHERE groupName in (%s, %s ) ", (f"ONE_TO_ONE_{sender_id}_{receiver_id}", f"ONE_TO_ONE_{receiver_id}_{sender_id}"))
                result = cursor.fetchone()
                
                if not result :
                    log.info(f"Creating one2one group for user1 :  {sender_id} and user2 : {receiver_id}")
                    group_data = await create_group(groupName =  f"ONE_TO_ONE_{sender_id}_{receiver_id}", created_by = sender_id, user_ids = f"{sender_id},{receiver_id}", type  =2)
                    group_id = group_data["group_id"]
                else:
                    log.info(f"use exiting one2one group  for user1 : {sender_id} and user2 : {receiver_id}")
                    group_id  = result[0]

            log.info(f"Sending msg to group : {group_id} content : {content}")

            # Group message
            # The `cursor` in the code is a cursor object that allows Python code to execute SQL
            # commands and retrieve results from the database. In this context, the `cursor` is
            # used within a database connection to execute SQL queries and interact with the
            # database tables. It is created using the `conn.cursor()` method, where `conn` is the
            # database connection object obtained from `get_db_connection()` function.
            # The `cursor` in the code is being used to interact with the database in the context
            # of the MySQL connection. Here are some key points about what `cursor` is doing in
            # different parts of the code:
            cursor.execute("SELECT COUNT(*) FROM group_members WHERE group_id = %s AND user_id = %s", (group_id, sender_id))
            if cursor.fetchone()[0] == 0:
                raise HTTPException(status_code=403, detail="Not a member of the group")
            cursor.execute("INSERT INTO messages (group_id, content) VALUES ( %s, %s)",
                (group_id, content))
            message_id = cursor.lastrowid
            log.info(f"created : message_id : {message_id} : content : {content}")
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
        "timestamp": str(datetime.now())
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
                cursor.execute("SELECT group_id FROM group_members WHERE user_id = %s", (user_id,))
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

    try:
        while True:
            # Listen for pubsub messages asynchronously
            message = await asyncio.to_thread(pubsub.get_message)
            if message and message['type'] == 'message':
                log.info(f"User {user_id} received msg {message}")
                data = json.loads(message['data'])
                if data.get("type") == "refresh":
                    refresh_subscriptions()
                else:
                    await websocket.send_text(json.dumps(data))

            # Handle heartbeat with timeout
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=2.0)
                if data == "heartbeat":
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)