# User Recent Search Application

## Overview
This is a web-based application for user registration and product search with recent search suggestions. It uses a distributed architecture with RestAPI, Elasticsearch, Kafka, Redis, and MySQL for scalable and efficient operations. The app allows users to register, search for products, view recent searches in a dropdown, and logs search events for analysis.

## Features
- **User Registration**: Register users and store in MySQL.
- **Product Search**: Search products indexed in Elasticsearch, with results displayed.
- **Recent Searches**: Dropdown shows user's recent searches (from Redis), falling back to global popular searches. Uses list with set-like behavior (unique, ordered by recency).
- **Event Logging**: Search events sent to Kafka for durability and analysis.
- **Analysis and Archival**: Workers process events into analysis Elasticsearch, with periodic cleanup to local files.
- **Realistic Products**: 100 dummy products with attributes like price, category, etc.

### Architecture Diagram

![Block Diagram](recent_search_block_diagram.png)


## Design Decisions
- **FastAPI for Backend**: Lightweight, async support for high-performance web API.
- **Elasticsearch for Search**: Full-text search on products; separate index for analysis to avoid impacting main search.
- **Redis for Recent Searches**: In-memory storage for user-specific and global search history; uses list with lrem/lpush for uniqueness and order.
- **Kafka for Event Streaming**: Ensures durability and decoupling; workers can scale independently.
- **MySQL for Relational Data**: Structured storage for users and products.
- **Workers Split**: Insert worker for real-time indexing; cleanup worker for periodic archival to reduce load.
- **Local Archival**: Simple file-based storage for old records instead of cloud for ease of setup.
- **No Authentication**: Simplified for demo; add JWT/OAuth for production.


## Prerequisites
- Python 3.8+
- MySQL Server (with SSL enabled for auth)
- Redis Server
- Elasticsearch Cluster
- Kafka Cluster
- Libraries: `pip install fastapi uvicorn pymysql redis elasticsearch confluent-kafka`

## Installation
1. Clone/download the project to your workspace.
3. Install dependencies: `pip install -r requirements.txt` (create if needed).
4. Start services: MySQL, Redis, Elasticsearch, Kafka.

## Setup
1. **Database**: Run `.venv/bin/python  -m user_recent_search_app.add_and_index_products` to create DB/tables and insert 100 products.
2. **Elasticsearch**: Ensure indices "products" and "search_analysis" exist (created automatically on first use).
3. **Kafka**: Create topic "user_searches" if needed.
4. **SSL for MySQL**: Ensure MySQL has SSL enabled (see troubleshooting).

## How to Run
Set `PYTHONPATH`:
```
export PYTHONPATH=/Users/sanjivsingh/Projects/VS_workspace/distributed_transactions:$PYTHONPATH
```

1. **Start Backend**: `.venv/bin/python -m uvicorn user_recent_search_app.search_app:app --reload --port 8000`
2. **Start Insert Worker**: `.venv/bin/python -m user_recent_search_app.worker_analysis_elastic_insert`
3. **Start Cleanup Worker**: `.venv/bin/python -m user_recent_search_app.worker_analysis_elastic_cleanup`
4. **Access App**: Open browser to http://localhost:8000. Register, search products.

### Running Components Separately
- For testing, run workers in separate terminals.
- Use `uvicorn` for development.

## API Endpoints
- `GET /`: Home page with registration/search form.
- `POST /register`: Register user (username).
- `GET /recent_searches?username=<id>`: Get recent searches.
- `POST /search`: Search products (query, username).

## Technologies Used
- **Backend**: FastAPI, Python
- **Database**: MySQL (pymysql)
- **Search**: Elasticsearch
- **Cache**: Redis
- **Messaging**: Kafka (confluent-kafka)
- **Archival**: Local files
- **Frontend**: HTML/JS

## Troubleshooting
- **Connection Errors**: Check service ports and configs.
- **No Search Results**: Ensure products are indexed in ES.
- **Worker Issues**: Verify Kafka topics and ES indices.

For enhancements (e.g., authentication, cloud archival), let me know!