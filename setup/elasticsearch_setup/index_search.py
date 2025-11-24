from elasticsearch import Elasticsearch, helpers
import json


from setup.elasticsearch_setup import constants, config

# Elasticsearch configuration
ES_HOST = config.configurations[constants.ES_HOST]
ES_PORT = config.configurations[constants.ES_PORT]
INDEX_NAME = "test_documents"

# Sample documents to index
sample_documents = [
    {
        "_index": INDEX_NAME,
        "_id": "1",
        "_source": {
            "title": "Introduction to Elasticsearch",
            "content": "Elasticsearch is a distributed, RESTful search and analytics engine capable of addressing a growing number of use cases.",
            "author": "John Doe",
            "tags": ["search", "analytics", "distributed"]
        }
    },
    {
        "_index": INDEX_NAME,
        "_id": "2",
        "_source": {
            "title": "Python Programming Basics",
            "content": "Python is a high-level programming language known for its simplicity and readability.",
            "author": "Jane Smith",
            "tags": ["programming", "python", "basics"]
        }
    },
    {
        "_index": INDEX_NAME,
        "_id": "3",
        "_source": {
            "title": "Machine Learning with Python",
            "content": "Machine learning involves algorithms that can learn from data to make predictions.",
            "author": "Alice Johnson",
            "tags": ["machine learning", "python", "AI"]
        }
    },
    {
        "_index": INDEX_NAME,
        "_id": "4",
        "_source": {
            "title": "Distributed Systems Overview",
            "content": "Distributed systems are systems whose components are located on different networked computers.",
            "author": "Bob Brown",
            "tags": ["distributed", "systems", "networking"]
        }
    }
]

def connect_to_elasticsearch():
    """Connect to Elasticsearch."""
    try:
        # Updated connection to include 'scheme' for newer elasticsearch library versions
        es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT, 'scheme': 'http'}])
        if es.ping():
            print("Connected to Elasticsearch")
            return es
        else:
            print("Could not connect to Elasticsearch")
            return None
    except Exception as e:
        print(f"Error connecting to Elasticsearch: {e}")
        return None

def create_index(es, index_name):
    """Create an index if it doesn't exist."""
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
        print(f"Index '{index_name}' created")
    else:
        print(f"Index '{index_name}' already exists")

def index_documents(es, documents):
    """Index documents using bulk API."""
    try:
        helpers.bulk(es, documents)
        print(f"Indexed {len(documents)} documents")
    except Exception as e:
        print(f"Error indexing documents: {e}")

def perform_full_text_search(es, index_name, query_term):
    """Perform a full text search."""
    query = {
        "query": {
            "match": {
                "content": query_term
            }
        }
    }
    try:
        response = es.search(index=index_name, body=query)
        print(f"Search results for '{query_term}':")
        for hit in response['hits']['hits']:
            print(f"- ID: {hit['_id']}, Title: {hit['_source']['title']}, Score: {hit['_score']}")
        return response
    except Exception as e:
        print(f"Error performing search: {e}")
        return None

def perform_term_search(es, index_name, field, term):
    """Perform a term search (exact match)."""
    query = {
        "query": {
            "term": {
                field: term
            }
        }
    }
    try:
        response = es.search(index=index_name, body=query)
        print(f"Term search results for '{term}' in field '{field}':")
        for hit in response['hits']['hits']:
            print(f"- ID: {hit['_id']}, Title: {hit['_source']['title']}")
        return response
    except Exception as e:
        print(f"Error performing term search: {e}")
        return None

def perform_fuzzy_search(es, index_name, field, term):
    """Perform a fuzzy search."""
    query = {
        "query": {
            "fuzzy": {
                field: {
                    "value": term,
                    "fuzziness": "AUTO"
                }
            }
        }
    }
    try:
        response = es.search(index=index_name, body=query)
        print(f"Fuzzy search results for '{term}' in field '{field}':")
        for hit in response['hits']['hits']:
            print(f"- ID: {hit['_id']}, Title: {hit['_source']['title']}")
        return response
    except Exception as e:
        print(f"Error performing fuzzy search: {e}")
        return None

def perform_aggregation_search(es, index_name):
    """Perform an aggregation search (e.g., count by author)."""
    query = {
        "size": 0,  # No hits, only aggregations
        "aggs": {
            "authors": {
                "terms": {
                    "field": "author.keyword"
                }
            }
        }
    }
    try:
        response = es.search(index=index_name, body=query)
        print("Aggregation results (documents by author):")
        for bucket in response['aggregations']['authors']['buckets']:
            print(f"- {bucket['key']}: {bucket['doc_count']} documents")
        return response
    except Exception as e:
        print(f"Error performing aggregation: {e}")
        return None

def main():
    es = connect_to_elasticsearch()
    if not es:
        return

    create_index(es, INDEX_NAME)
    index_documents(es, sample_documents)

    # Full text search tests
    perform_full_text_search(es, INDEX_NAME, "Elasticsearch")
    perform_full_text_search(es, INDEX_NAME, "Python")
    perform_full_text_search(es, INDEX_NAME, "machine learning")

    # Term search tests
    perform_term_search(es, INDEX_NAME, "author", "John Doe")
    perform_term_search(es, INDEX_NAME, "tags", "python")

    # Fuzzy search tests
    perform_fuzzy_search(es, INDEX_NAME, "title", "Elastcsearch")  # Typo
    perform_fuzzy_search(es, INDEX_NAME, "content", "distributd")  # Typo

    # Aggregation test
    perform_aggregation_search(es, INDEX_NAME)

if __name__ == "__main__":
    main()