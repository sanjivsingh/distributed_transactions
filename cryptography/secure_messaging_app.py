from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pymysql

app = FastAPI()

from mysql_setup import config as mysql_config, constants as mysql_constants

# MySQL connection
def get_db_connection():
    return pymysql.connect(
            host=mysql_config.configurations[mysql_constants.HOST],
            user=mysql_config.configurations[mysql_constants.USER],
            password=mysql_config.configurations[mysql_constants.PASSWORD],
            port=mysql_config.configurations[mysql_constants.PORT],
            database='secure_messaging_db', 
            charset='utf8mb4'
)

# Initialize DB
def init_db():
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INT AUTO_INCREMENT PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                public_key TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sender VARCHAR(255) NOT NULL,
                recipient VARCHAR(255) NOT NULL,
                encrypted_message TEXT NOT NULL,
                signature TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    conn.commit()
    conn.close()

init_db()

class RegisterRequest(BaseModel):
    username: str
    public_key: str  # Client generates and sends public key

class SendMessageRequest(BaseModel):
    sender: str
    recipient: str
    encrypted_message: str  # Encrypted client-side
    signature: str  # Signed client-side

class ReceiveMessagesRequest(BaseModel):
    username: str

@app.post("/register")
def register_user(request: RegisterRequest):
    # Store public key in DB
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO users (username, public_key) VALUES (%s, %s)", (request.username, request.public_key))
        conn.commit()
        return {"message": "User registered successfully"}
    except pymysql.IntegrityError:
        raise HTTPException(status_code=400, detail="Username already exists")
    finally:
        conn.close()

@app.post("/send_message")
def send_message(request: SendMessageRequest):
    # Store the encrypted message and signature
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # Check if recipient exists
            cursor.execute("SELECT id FROM users WHERE username = %s", (request.recipient,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Recipient not found")
            cursor.execute("INSERT INTO messages (sender, recipient, encrypted_message, signature) VALUES (%s, %s, %s, %s)",
                           (request.sender, request.recipient, request.encrypted_message, request.signature))
        conn.commit()
        return {"message": "Message sent successfully"}
    finally:
        conn.close()

@app.post("/receive_messages")
def receive_messages(request: ReceiveMessagesRequest):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, sender, encrypted_message, signature FROM messages WHERE recipient = %s", (request.username,))
            messages = cursor.fetchall()
        return {"messages": [{"id": msg[0], "sender": msg[1], "encrypted_message": msg[2], "signature": msg[3]} for msg in messages]}
    finally:
        conn.close()

@app.get("/get_public_key/{username}")
def get_public_key(username: str):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT public_key FROM users WHERE username = %s", (username,))
            result = cursor.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="User not found")
            return {"public_key": result[0]}
    finally:
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)