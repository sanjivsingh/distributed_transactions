import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pymongo import MongoClient
from commons import logger
from shard_manager import ShardManager

log = logger.setup_logger(__name__)

class MongoShardClient:
    """Client for connecting to MongoDB shards"""
    
    def __init__(self, node_url: str, data_base: str, collection_name: str):
        self.node_url = node_url
        self.data_base = data_base
        self.collection_name = collection_name
        self.client = None
        self.connect()
    
    def connect(self):
        """Connect to MongoDB shard"""
        try:
            log.info(f"Connecting to MongoDB shard: {self.node_url}")
            self.client = MongoClient(
                self.node_url.split(":")[0],
                int(self.node_url.split(":")[1])
            )
            self.db = self.client[self.data_base]
            self.collection = self.db[self.collection_name]
            log.info(f"Connected to MongoDB shard: {self.node_url}")
        except Exception as e:
            log.error(f"Failed to connect to MongoDB shard {self.node_url}: {e}")
            raise
    
    def get_suggestions(self, prefix: str) -> list[str]:
        """Get auto-complete suggestions using exact key lookup"""
        try:
            # Convert prefix to lowercase for consistent lookup
            prefix_key = prefix.lower()
            
            # Direct lookup by prefix key
            document = self.collection.find_one(
                {"prefix": prefix_key},
                {"suggestions": 1, "_id": 0}
            )
            
            if document and "suggestions" in document:
                # Get suggestions sorted by score (they're pre-sorted)
                suggestions = document["suggestions"]
                suggestion_texts = [item["text"] for item in suggestions]
                
                log.debug(f"Found {len(suggestion_texts)} suggestions for prefix '{prefix}'")
                return suggestion_texts
            else:
                log.debug(f"No suggestions found for prefix '{prefix}'")
                return []
            
        except Exception as e:
            log.error(f"Error querying MongoDB shard {self.node_url}: {e}")
            return []
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            log.info(f"Closed connection to MongoDB shard: {self.node_url}") 

    def __del__(self):
        self.close()
    
def insert_sample_data():
    """Insert pre-calculated auto-complete data for exact key lookups"""
    try:
        # Pre-calculated suggestions data with exact prefix keys
        # Each document contains a prefix and all its suggestions sorted by score
        sample_data = [
            {
                "prefix": "a",
                "suggestions": [
                    {"text": "apple", "score": 100},
                    {"text": "amazon", "score": 98},
                    {"text": "application", "score": 95},
                    {"text": "android", "score": 90},
                    {"text": "artificial", "score": 88},
                    {"text": "algorithm", "score": 85},
                    {"text": "analytics", "score": 82},
                    {"text": "apply", "score": 80},
                    {"text": "apartment", "score": 70},
                    {"text": "amazing", "score": 60}
                ]
            },
            {
                "prefix": "ap",
                "suggestions": [
                    {"text": "apple", "score": 100},
                    {"text": "application", "score": 95},
                    {"text": "apply", "score": 80},
                    {"text": "apartment", "score": 70}
                ]
            },
            {
                "prefix": "app",
                "suggestions": [
                    {"text": "apple", "score": 100},
                    {"text": "application", "score": 95}
                ]
            },
            {
                "prefix": "appl",
                "suggestions": [
                    {"text": "apple", "score": 100},
                    {"text": "application", "score": 95},
                    {"text": "apply", "score": 80}
                ]
            },
            {
                "prefix": "apple",
                "suggestions": [
                    {"text": "apple", "score": 100},
                    {"text": "apple watch", "score": 85},
                    {"text": "apple iphone", "score": 80},
                    {"text": "apple macbook", "score": 75}
                ]
            },
            {
                "prefix": "f",
                "suggestions": [
                    {"text": "facebook", "score": 100},
                    {"text": "function", "score": 90},
                    {"text": "framework", "score": 88},
                    {"text": "frontend", "score": 85},
                    {"text": "fast", "score": 85},
                    {"text": "firebase", "score": 82},
                    {"text": "filter", "score": 80},
                    {"text": "fetch", "score": 80},
                    {"text": "fantastic", "score": 80},
                    {"text": "filesystem", "score": 75}
                ]
            },
            {
                "prefix": "fa",
                "suggestions": [
                    {"text": "facebook", "score": 100},
                    {"text": "fast", "score": 85},
                    {"text": "fantastic", "score": 80}
                ]
            },
            {
                "prefix": "fac",
                "suggestions": [
                    {"text": "facebook", "score": 100}
                ]
            },
            {
                "prefix": "face",
                "suggestions": [
                    {"text": "facebook", "score": 100}
                ]
            },
            {
                "prefix": "facebook",
                "suggestions": [
                    {"text": "facebook", "score": 100},
                    {"text": "facebook login", "score": 90},
                    {"text": "facebook marketplace", "score": 85},
                    {"text": "facebook messenger", "score": 80}
                ]
            },
            {
                "prefix": "j",
                "suggestions": [
                    {"text": "java", "score": 100},
                    {"text": "javascript", "score": 98},
                    {"text": "json", "score": 95},
                    {"text": "jquery", "score": 85},
                    {"text": "jira", "score": 82},
                    {"text": "jupyter", "score": 80},
                    {"text": "jwt", "score": 78},
                    {"text": "jenkins", "score": 75},
                    {"text": "join", "score": 70},
                    {"text": "journey", "score": 65}
                ]
            },
            {
                "prefix": "ja",
                "suggestions": [
                    {"text": "java", "score": 100},
                    {"text": "javascript", "score": 98}
                ]
            },
            {
                "prefix": "jav",
                "suggestions": [
                    {"text": "java", "score": 100},
                    {"text": "javascript", "score": 98}
                ]
            },
            {
                "prefix": "java",
                "suggestions": [
                    {"text": "java", "score": 100},
                    {"text": "javascript", "score": 98},
                    {"text": "java tutorial", "score": 90},
                    {"text": "java spring", "score": 85},
                    {"text": "java 8", "score": 80}
                ]
            },
            {
                "prefix": "javas",
                "suggestions": [
                    {"text": "javascript", "score": 98}
                ]
            },
            {
                "prefix": "javascript",
                "suggestions": [
                    {"text": "javascript", "score": 98},
                    {"text": "javascript tutorial", "score": 90},
                    {"text": "javascript framework", "score": 85},
                    {"text": "javascript react", "score": 80}
                ]
            },
            {
                "prefix": "s",
                "suggestions": [
                    {"text": "search", "score": 100},
                    {"text": "service", "score": 95},
                    {"text": "system", "score": 92},
                    {"text": "software", "score": 90},
                    {"text": "server", "score": 88},
                    {"text": "sql", "score": 87},
                    {"text": "security", "score": 85},
                    {"text": "spring", "score": 82},
                    {"text": "storage", "score": 80},
                    {"text": "scalability", "score": 78}
                ]
            },
            {
                "prefix": "se",
                "suggestions": [
                    {"text": "search", "score": 100},
                    {"text": "service", "score": 95},
                    {"text": "server", "score": 88},
                    {"text": "security", "score": 85}
                ]
            },
            {
                "prefix": "sea",
                "suggestions": [
                    {"text": "search", "score": 100}
                ]
            },
            {
                "prefix": "sear",
                "suggestions": [
                    {"text": "search", "score": 100}
                ]
            },
            {
                "prefix": "search",
                "suggestions": [
                    {"text": "search", "score": 100},
                    {"text": "search engine", "score": 90},
                    {"text": "search algorithm", "score": 85},
                    {"text": "search optimization", "score": 80}
                ]
            },
            {
                "prefix": "sa",
                "suggestions": [
                    {"text": "samsung", "score": 100},
                    {"text": "safari", "score": 85},
                    {"text": "sales", "score": 82},
                    {"text": "salary", "score": 80},
                    {"text": "save", "score": 78},
                    {"text": "sample", "score": 75},
                    {"text": "sandbox", "score": 70}
                ]
            },
            {
                "prefix": "sam",
                "suggestions": [
                    {"text": "samsung", "score": 100},
                    {"text": "sample", "score": 75}
                ]
            },
            {
                "prefix": "sams",
                "suggestions": [
                    {"text": "samsung", "score": 100}
                ]
            },
            {
                "prefix": "samsung",
                "suggestions": [
                    {"text": "samsung", "score": 100},
                    {"text": "samsung galaxy", "score": 95},
                    {"text": "samsung phone", "score": 90},
                    {"text": "samsung tablet", "score": 85}
                ]
            },
            {
                "prefix": "t",
                "suggestions": [
                    {"text": "technology", "score": 100},
                    {"text": "twitter", "score": 95},
                    {"text": "testing", "score": 90},
                    {"text": "typescript", "score": 88},
                    {"text": "tutorial", "score": 85},
                    {"text": "transaction", "score": 82},
                    {"text": "template", "score": 80},
                    {"text": "threading", "score": 78},
                    {"text": "tracking", "score": 77},
                    {"text": "terraform", "score": 75}
                ]
            },
            {
                "prefix": "te",
                "suggestions": [
                    {"text": "technology", "score": 100},
                    {"text": "testing", "score": 90},
                    {"text": "template", "score": 80},
                    {"text": "terraform", "score": 75}
                ]
            },
            {
                "prefix": "tec",
                "suggestions": [
                    {"text": "technology", "score": 100}
                ]
            },
            {
                "prefix": "tech",
                "suggestions": [
                    {"text": "technology", "score": 100}
                ]
            },
            {
                "prefix": "technology",
                "suggestions": [
                    {"text": "technology", "score": 100},
                    {"text": "technology news", "score": 90},
                    {"text": "technology trends", "score": 85},
                    {"text": "technology stack", "score": 80}
                ]
            },
            {
                "prefix": "tw",
                "suggestions": [
                    {"text": "twitter", "score": 95}
                ]
            },
            {
                "prefix": "twi",
                "suggestions": [
                    {"text": "twitter", "score": 95}
                ]
            },
            {
                "prefix": "twitter",
                "suggestions": [
                    {"text": "twitter", "score": 95},
                    {"text": "twitter api", "score": 85},
                    {"text": "twitter bot", "score": 80},
                    {"text": "twitter analytics", "score": 75}
                ]
            }
        ]
        shard_manager = ShardManager()
        
        for doc in sample_data:
            shard_info = shard_manager.find_shard_for_prefix(str(doc["prefix"]))
            
            node_url = shard_info["node"]

            client = MongoClient(
                node_url.split(":")[0],
                int(node_url.split(":")[1])
            )
            db = client[shard_info["data_base"]]
            collection = db[shard_info["collection"]]
            collection.insert_one(doc)
            client.close()
            log.info(f"Inserted sample data for prefix '{doc['prefix']}' into shard {node_url} {shard_info['data_base']}.{shard_info['collection']}")
        
    except Exception as e:
        log.error(f"Error inserting sample data: {e}")

if __name__ == "__main__":
    insert_sample_data()