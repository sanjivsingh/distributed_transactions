# app.py
from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import pymysql  # Changed from mysql.connector to pymysql
import redis
from elasticsearch import Elasticsearch
from confluent_kafka import Producer, KafkaError  # Changed from kafka import KafkaProducer
import json
import uuid
import os 
from datetime import datetime
import requests  # For IP geolocation (optional, install if needed)

os.makedirs("user_recent_search_app/static", exist_ok=True)

app = FastAPI()
app.mount("/static", StaticFiles(directory="user_recent_search_app/static"), name="static")

def get_location_from_ip(ip):
    try:
        response = requests.get(f"http://ipinfo.io/{ip}/json")
        data = response.json()
        return {
            "country": data.get("country"),
            "city": data.get("city"),
            "region": data.get("region"),
            "loc": data.get("loc")  # lat,long
        }
    except:
        return {"country": "Unknown", "city": "Unknown", "region": "Unknown", "loc": "Unknown"}


# Connections
from setup.mysql_setup import config as mysql_config, constants as mysql_constants
try:
    mysql_conn = pymysql.connect( 
        host=mysql_config.configurations[mysql_constants.HOST],
        port=mysql_config.configurations[mysql_constants.PORT],
        user=mysql_config.configurations[mysql_constants.USER],
        password=mysql_config.configurations[mysql_constants.PASSWORD],
        database="toy_ecommerce",
    )
except Exception as e:
    raise HTTPException(status_code=500, detail=f"MySQL connection failed: {str(e)}")

# Redis connection
from setup.redis_setup import config as redis_config, constants as redis_constants
try:
    redis_client = redis.Redis(
        host=redis_config.configurations[redis_constants.REDIS_SERVER],
        port=redis_config.configurations[redis_constants.REDIS_PORT],
        decode_responses=True
    )
except Exception as e:
    raise HTTPException(status_code=500, detail=f"Redis connection failed: {str(e)}")

from setup.elasticsearch_setup import constants as elasticsearch_constants, config as elasticsearch_config
try:
    es = Elasticsearch([{'host': elasticsearch_config.configurations[elasticsearch_constants.ES_HOST], 'port': elasticsearch_config.configurations[elasticsearch_constants.ES_PORT], 'scheme': 'http'}])
except Exception as e:
    raise HTTPException(status_code=500, detail=f"Elasticsearch connection failed: {str(e)}")

# Updated to use confluent_kafka Producer
from setup.kafka_setup import config as kafka_config, constants as kafka_constants
try:
    producer_conf = {'bootstrap.servers': kafka_config.configurations[kafka_constants.KAFKA_BROKER]}
    producer = Producer(producer_conf)
except Exception as e:
    raise HTTPException(status_code=500, detail=f"Kafka producer failed: {str(e)}")



@app.get("/", response_class=HTMLResponse)
async def home():
    try:
        with open("user_recent_search_app/templates/index.html", "r") as f:
            return f.read()
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Template file not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading home page: {str(e)}")

@app.post("/register")
async def register(username: str = Form(...)):
    try:
        cursor = mysql_conn.cursor()
        cursor.execute("INSERT INTO users (username) VALUES (%s)", (username,))
        mysql_conn.commit()
        return {"message": "User registered"}
    except pymysql.IntegrityError:
        raise HTTPException(status_code=400, detail="Username already exists")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Registration failed: {str(e)}")

@app.get("/recent_searches")
async def recent_searches(username: str):
    try:
        # Get user recent searches from Redis
        recent = redis_client.lrange(f"user:{username}:searches", 0, 9)
        if not recent:
            # Fallback to global popular searches
            recent = redis_client.zrevrange("global:popular_searches", 0, 9)
        return {"searches": recent}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching recent searches: {str(e)}")

@app.post("/search")
async def search(query: str = Form(...), username: str = Form(...), request: Request = None):
    try:
        # Collect user data
        ip = request.client.host if request else "Unknown"
        timestamp = datetime.utcnow().isoformat()
        location = get_location_from_ip(ip)
        
        # Save to Redis (user recent) - use Lua script for atomic operation
        key = f"user:{username}:searches"
        # Lua script for updating recent searches
        update_recent_searches_script = """
        local key = KEYS[1]
        local query = ARGV[1]
        redis.call('LREM', key, 0, query)
        redis.call('LPUSH', key, query)
        redis.call('LTRIM', key, 0, 9)
        """
        redis_client.eval(update_recent_searches_script, 1, key, query)
        
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
        
        # Send to Kafka using confluent_kafka with additional data
        event_data = {
            "username": username,
            "query": query,
            "timestamp": timestamp,
            "ip": ip,
            "location": location
        }
        producer.produce('user_searches', value=json.dumps(event_data).encode('utf-8'))
        producer.flush()  # Ensure message is sent
        
        return {"products": products}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")