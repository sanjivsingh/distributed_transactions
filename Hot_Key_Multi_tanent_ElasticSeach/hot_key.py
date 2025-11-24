from elasticsearch import Elasticsearch
import random

from setup.elasticsearch_setup import (
    config as elasticsearch_config,
    constants as elasticsearch_constants
)

index_name = "multi_tenant_index"
hot_docs_per_tenant = 200
non_hot_docs_per_tenant = 50

# Define 50 tenants
tenants = [f"tenant{i}" for i in range(1, 20)]

# Define hot tenants (first 3, with random shard counts)
# mapping of hot tenant to shard count 
hot_tanent = {f"tenant{i}": random.randint(2, 5) for i in range(1, 4)}

# Elasticsearch connection
es = Elasticsearch([
    {
        "host": elasticsearch_config.configurations[elasticsearch_constants.ES_HOST],
        "port": elasticsearch_config.configurations[elasticsearch_constants.ES_PORT],
        "scheme": "http",
    }
])

def create_index():

    # delete index if already exists
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)

    # Define index with number_of_shards and number_of_replicas
    index_body = {
        "settings": {
            "number_of_shards": 10,  # Total shards for the index
            "number_of_replicas": 1
        },
        "mappings": {
            "properties": {
                "tenant_id": {"type": "keyword"},
                "name": {"type": "text"},
                "value": {"type": "integer"},
                "description": {"type": "text"}
            }
        }
    }
    es.indices.create(index=index_name, body=index_body)


def populate_index():


    def generate_routing(tenant, doc_id):
        if tenant not in hot_tanent:
            return tenant
        else:
            return f"{tenant}_{doc_id % hot_tanent[tenant]}"  # Shard by doc_id mod shard_count

    # Sample data generator
    def generate_document(tenant, doc_id):
        return {
            "tenant_id": tenant,
            "name": f"Document {doc_id} for {tenant}",
            "value": random.randint(1, 100),
            "description": f"A sample document for tenant {tenant}."
        }

    # Total documents: 100,000
    # Hot tenants: 10 (80% of docs = 80,000), so 8,000 per hot tenant
    # Non-hot tenants: 40 (20% of docs = 20,000), so 500 per non-hot tenant



    # Insert documents for each tenant
    for tenant in tenants:
        print(f"Inserting tenant : {tenant} ...")
        # Determine number of documents for this tenant
        num_docs = hot_docs_per_tenant if tenant in hot_tanent else non_hot_docs_per_tenant 
        
        for i in range(1, num_docs + 1):
            doc = generate_document(tenant, i)
            response = es.index(
                index=index_name,  # Single index for all tenants
                routing=generate_routing(tenant, i),
                id=f"{tenant}_{i}",
                body=doc
            )
            # Print every 1000 inserts to avoid spam
            if i % 1000 == 0:
                print(f"Inserted {i} documents for {tenant}: last ID {response['_id']}")

    print("All 100,000 documents inserted for 50 tenants (80% for hot tenants).")

def get_stats():
    # After insertion, check shard information and document distribution

    # 1. Get number of shards on the Elastic node/cluster
    print("\n--- Shard Information ---")
    try:
        shards_info = es.cat.shards(index="multi_tenant_index", format="json")
        total_shards = len(shards_info)
        print(f"Total shards for index 'multi_tenant_index': {total_shards}")
        
        # Group by node
        node_shards = {}
        for shard in shards_info:
            node = shard['node']
            if node not in node_shards:
                node_shards[node] = 0
            node_shards[node] += 1
        
        print("Shards per node:")
        for node, count in node_shards.items():
            print(f"  {node}: {count} shards")
    except Exception as e:
        print(f"Error fetching shard info: {e}")

    # 2. Document distribution for hot tenants
    print("\n--- Document Distribution for Hot Tenants ---")
    for tenant in hot_tanent.keys():
        try:
            # Use search with routing to count docs for the tenant
            response = es.count(index=index_name, routing=tenant, body={
                "query": {"term": {"tenant_id": tenant}}
            })
            doc_count = response['count']
            print(f"{tenant}: {doc_count} documents")
        except Exception as e:
            print(f"Error counting docs for {tenant}: {e}")

    print("\nNote: For hot tenants, documents are routed to specific shards based on routing logic.")

    # 3. Per shard document count for each tenant
    print("\n--- Per Shard Document Count for Each Tenant ---")
    data = []
    try:
        # Get list of unique shard IDs
        shards_info = es.cat.shards(index=index_name, format="json")
        shard_ids = list(set(shard['shard'] for shard in shards_info))
        
        for tenant in tenants:
            print(f"\nTenant: {tenant}")
            for shard_id in shard_ids:
                try:
                    # Use count with preference to target specific shard
                    response = es.count(
                        index="multi_tenant_index",
                        preference=f"_shards:{shard_id}",
                        body={"query": {"term": {"tenant_id": tenant}}}
                    )
                    doc_count = response['count']
                    if doc_count > 0:  # Only print if there are docs
                        print(f"  tenant {tenant} Shard {shard_id}: {doc_count} documents")
                        data.append(f"Shard{shard_id},tenant{tenant},{doc_count}")
                except Exception as e:
                    print(f"  Error for shard {shard_id}: {e}")
    except Exception as e:
        print(f"Error fetching per shard counts: {e}")

    with open("Hot_Key_Multi_tanent_ElasticSeach/shard_tenant_distribution.csv", "w") as f:
        f.write("shard_id,tenant_id,document_count\n")
        for line in data:
            f.write(line + "\n")    

if __name__ == "__main__":
    create_index()
    populate_index()
    get_stats() 

