from pymongo import MongoClient
from commons import logger
import datetime

log = logger.setup_logger(__name__)

def connect_mongo():
    try:
        client = MongoClient('localhost', 27017)
        db = client['distributed_db']
        log.info("Connected to MongoDB")
        return db
    except Exception as e:
        log.error(f"Error connecting to MongoDB: {e}")
        raise

def ingest_data(db):
    collection = db['orders']
    # Sample data to ingest
    orders = [
        {
            "order_id": 1,
            "customer_id": "C001",
            "items": [
                {"product": "Laptop", "category": "Electronics", "price": 1000, "quantity": 1},
                {"product": "Mouse", "category": "Electronics", "price": 50, "quantity": 2}
            ],
            "order_date": datetime.datetime(2023, 10, 1)
        },
        {
            "order_id": 2,
            "customer_id": "C002",
            "items": [
                {"product": "Book", "category": "Books", "price": 20, "quantity": 3},
                {"product": "Pen", "category": "Stationery", "price": 5, "quantity": 10}
            ],
            "order_date": datetime.datetime(2023, 10, 2)
        },
        {
            "order_id": 3,
            "customer_id": "C001",
            "items": [
                {"product": "Headphones", "category": "Electronics", "price": 200, "quantity": 1}
            ],
            "order_date": datetime.datetime(2023, 10, 3)
        }
    ]
    try:
        result = collection.insert_many(orders)
        log.info(f"Inserted {len(result.inserted_ids)} documents")
    except Exception as e:
        log.error(f"Error ingesting data: {e}")

def aggregation_pipeline(db):
    collection = db['orders']
    # Aggregation pipeline: Unwind items, group by category, sum total sales
    pipeline = [
        {"$unwind": "$items"},
        {"$group": {
            "_id": "$items.category",
            "total_sales": {"$sum": {"$multiply": ["$items.price", "$items.quantity"]}},
            "total_quantity": {"$sum": "$items.quantity"}
        }},
        {"$sort": {"total_sales": -1}}
    ]
    try:
        results = list(collection.aggregate(pipeline))
        log.info("Aggregation results:")
        for result in results:
            log.info(f"Category: {result['_id']}, Total Sales: {result['total_sales']}, Total Quantity: {result['total_quantity']}")
        return results
    except Exception as e:
        log.error(f"Error running aggregation: {e}")
        return []

if __name__ == "__main__":
    db = connect_mongo()
    ingest_data(db)
    aggregation_pipeline(db)