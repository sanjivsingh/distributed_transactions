# app.py
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import pymysql  # Changed from mysql.connector to pymysql
import redis
from elasticsearch import Elasticsearch
from confluent_kafka import Producer  # Changed from kafka import KafkaProducer
import json
import uuid
import os 

os.makedirs("user_recent_search_app/static", exist_ok=True)

app = FastAPI()
app.mount("/static", StaticFiles(directory="user_recent_search_app/static"), name="static")


# Connections
from setup.mysql_setup import config as mysql_config, constants as mysql_constants
mysql_conn = pymysql.connect( 
    host=mysql_config.configurations[mysql_constants.HOST],
    port=mysql_config.configurations[mysql_constants.PORT],
    user=mysql_config.configurations[mysql_constants.USER],
    password=mysql_config.configurations[mysql_constants.PASSWORD],
    database="toy_ecommerce",
)

# Redis connection
from setup.redis_setup import config as redis_config, constants as redis_constants
redis_client = redis.Redis(
    host=redis_config.configurations[redis_constants.REDIS_SERVER],
    port=redis_config.configurations[redis_constants.REDIS_PORT],
    decode_responses=True
)

from setup.elasticsearch_setup import constants as elasticsearch_constants, config as elasticsearch_config
es = Elasticsearch([{'host': elasticsearch_config.configurations[elasticsearch_constants.ES_HOST], 'port': elasticsearch_config.configurations[elasticsearch_constants.ES_PORT], 'scheme': 'http'}])

# Updated to use confluent_kafka Producer
from setup.kafka_setup import config as kafka_config, constants as kafka_constants
producer_conf = {'bootstrap.servers': kafka_config.configurations[kafka_constants.KAFKA_BROKER]}
producer = Producer(producer_conf)

@app.get("/", response_class=HTMLResponse)
async def home():
    with open("user_recent_search_app/templates/index.html", "r") as f:
        return f.read()

@app.post("/register")
async def register(username: str = Form(...)):
    cursor = mysql_conn.cursor()
    cursor.execute("INSERT INTO users (username) VALUES (%s)", (username,))
    mysql_conn.commit()
    return {"message": "User registered"}

@app.get("/recent_searches")
async def recent_searches(username: str):
    # Get user recent searches from Redis
    recent = redis_client.lrange(f"user:{username}:searches", 0, 9)
    if not recent:
        # Fallback to global popular searches
        recent = redis_client.zrevrange("global:popular_searches", 0, 9)
    return {"searches": recent}

@app.post("/search")
async def search(query: str = Form(...), username: str = Form(...)):
    # Save to Redis (user recent) - use list with set-like behavior: remove if exists, then add to front
    key = f"user:{username}:searches"
    redis_client.lrem(key, 0, query)  # Remove the query if it exists (0 removes all occurrences)
    redis_client.lpush(key, query)    # Add to front
    redis_client.ltrim(key, 0, 9)     # Keep only 10
    
    # Update global popular
    redis_client.zincrby("global:popular_searches", 1, query)
    
    # Search Elasticsearch - Updated for full text and fuzzy search
    response = es.search(index="products", body={
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["name", "description", "category", "sub_category"],
                "fuzziness": "AUTO"  # Enables fuzzy matching for typos
            }
        }
    })
    products = [hit["_source"] for hit in response["hits"]["hits"]]
    
    # Send to Kafka using confluent_kafka
    producer.produce('user_searches', value=json.dumps({"username": username, "query": query, "timestamp": str(uuid.uuid4())}).encode('utf-8'))
    producer.flush()  # Ensure message is sent
    
    return {"products": products}