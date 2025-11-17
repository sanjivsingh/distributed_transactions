from fastapi import FastAPI, HTTPException, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import pymysql
from pymysql.cursors import DictCursor
import pymongo
from confluent_kafka import Producer
import json
import os
from commons.uniqueid import SnowflakeIDGenerator
import cryptography

app = FastAPI()

from commons import logger
log = logger.setup_logger(__name__)
# Create directories
os.makedirs("instagram_hashtags/templates", exist_ok=True)
os.makedirs("instagram_hashtags/static", exist_ok=True)

templates = Jinja2Templates(directory="instagram_hashtags/templates")
app.mount("/static", StaticFiles(directory="instagram_hashtags/static"), name="static")

# Snowflake ID Generator
id_generator = SnowflakeIDGenerator(worker_id=1)

# MongoDB connection
from setup.mongodb_setup import config as mongo_config, constants as mongo_constants
mongo_client = pymongo.MongoClient(
    mongo_config.configurations[mongo_constants.SERVER],
    mongo_config.configurations[mongo_constants.PORT],
)
mongo_db = mongo_client["instagram_db"]
hashtag_collection = mongo_db["hashtags"]

from setup.mysql_setup import config as mysql_config, constants as mysql_constants


# MySQL connection
def create_mysql_db():
    connection = pymysql.connect(
        host=mysql_config.configurations[mysql_constants.HOST],
        user=mysql_config.configurations[mysql_constants.USER],
        password=mysql_config.configurations[mysql_constants.PASSWORD],
        port=mysql_config.configurations[mysql_constants.PORT],
        cursorclass=DictCursor,
    )
    with connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE DATABASE IF NOT EXISTS instagram_db
        """
        )

def get_mysql_connection():
    return pymysql.connect(
        host=mysql_config.configurations[mysql_constants.HOST],
        user=mysql_config.configurations[mysql_constants.USER],
        password=mysql_config.configurations[mysql_constants.PASSWORD],
        port=mysql_config.configurations[mysql_constants.PORT],
        database="instagram_db",
        cursorclass=DictCursor,
    )



# Kafka producer
from setup.kafka_setup import config as kafka_config, constants as kafka_constants
producer_conf = {
    "bootstrap.servers": kafka_config.configurations[kafka_constants.KAFKA_BROKER]
}
producer = Producer(producer_conf)


# Initialize DBs
def init_mysql():
    conn = get_mysql_connection()
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS posts (
                post_id BIGINT PRIMARY KEY,
                creater_id BIGINT NOT NULL,
                caption TEXT,
                creation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (creater_id) REFERENCES users(user_id)
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS images (
                image_id BIGINT PRIMARY KEY,
                post_id BIGINT NOT NULL,
                FOREIGN KEY (post_id) REFERENCES posts(post_id)
            )
        """
        )
    conn.commit()
    conn.close()


def init_mongo():
    # Ensure collection exists
    pass

create_mysql_db()
init_mysql()
init_mongo()


@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.post("/register")
def register_user(username: str = Form(...)):
    user_id = id_generator.generate_id()
    conn = get_mysql_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO users (user_id, username) VALUES (%s, %s)",
                (user_id, username),
            )
        conn.commit()
        return {"user_id": user_id, "username": username}
    except pymysql.IntegrityError:
        raise HTTPException(status_code=400, detail="Username already exists")
    finally:
        conn.close()


@app.post("/create_post")
def create_post(
    user_id: int = Form(...), caption: str = Form(...), image_count: int = Form(0)
):
    post_id = id_generator.generate_id()
    conn = get_mysql_connection()
    try:
        with conn.cursor() as cursor:
            # Insert post
            cursor.execute(
                "INSERT INTO posts (post_id, creater_id, caption) VALUES (%s, %s, %s)",
                (post_id, user_id, caption),
            )
            # Insert images
            image_ids = []
            for _ in range(image_count):
                image_id = id_generator.generate_id()
                cursor.execute(
                    "INSERT INTO images (image_id, post_id) VALUES (%s, %s)",
                    (image_id, post_id),
                )
                image_ids.append(image_id)
        conn.commit()

        # Send to Kafka
        message = {"post_id": post_id, "caption": caption, "image_ids": image_ids}
        producer.produce("instagram_posts", json.dumps(message).encode("utf-8"))
        producer.flush()
        log.info(f"Produced message to Kafka: {message}")

        return {"post_id": post_id, "message": "Post created successfully"}
    except Exception as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@app.post("/hashtag_summary")
def get_hashtag_summary(hashtag: str = Form(...)):
    hashtag = hashtag.lower()
    doc = hashtag_collection.find_one({"_id": hashtag})
    if not doc:
        return {"error": "Hashtag not found"}
    return {
        "hashtag": doc["_id"],
        "number_of_posts": doc.get("number_of_posts", 0),
        "top_hundred_posts": doc.get("top_hundred_posts", []),
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
