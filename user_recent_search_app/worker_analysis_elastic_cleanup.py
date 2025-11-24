# archival_worker_cleanup.py - Archives old records from analysis Elasticsearch to local files
from elasticsearch import Elasticsearch
import json
import os
from datetime import datetime, timedelta
import time  # For periodic execution

from setup.elasticsearch_setup import (
    config as elasticsearch_config,
    constants as elasticsearch_constants,
)

es = Elasticsearch(
    [
        {
            "host": elasticsearch_config.configurations[
                elasticsearch_constants.ES_HOST
            ],
            "port": elasticsearch_config.configurations[
                elasticsearch_constants.ES_PORT
            ],
            "scheme": "http",
        }
    ]
)

# Local archival directory
ARCHIVAL_DIR = "archived_searches"
os.makedirs(ARCHIVAL_DIR, exist_ok=True)

def cleanup_old_records():
    # Cleanup: Move old records (>30 days) to local file and delete from analysis Elasticsearch
    old_date = datetime.now() - timedelta(days=30)
    query = {"query": {"range": {"timestamp": {"lt": old_date.isoformat()}}}}
    response = es.search(index="search_analysis", body=query)
    for hit in response["hits"]["hits"]:
        # Archive to local file
        file_path = os.path.join(ARCHIVAL_DIR, f"{hit['_id']}.json")
        with open(file_path, "w") as f:
            json.dump(hit["_source"], f)
        # Delete from analysis Elasticsearch
        es.delete(index="search_analysis", id=hit["_id"])
        print(f"Archived to local file and deleted old search record: {hit['_id']}")

# Run cleanup periodically (e.g., every 60 seconds)
while True:
    cleanup_old_records()
    time.sleep(60)  # Adjust interval as needed