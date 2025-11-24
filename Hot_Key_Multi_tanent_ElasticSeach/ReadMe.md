# Hot Key Multi-Tenant Elasticsearch

## Overview
This project demonstrates a multi-tenant Elasticsearch setup with special handling for "hot tenants" to prevent shard hotspots. It generates and indexes 100,000 documents across 50 tenants, with 80% of documents allocated to hot tenants for load distribution. The system uses custom routing to ensure hot tenants' documents are evenly distributed across multiple shards, improving performance and avoiding bottlenecks.

## Features
- **Multi-Tenant Indexing**: Supports 50 tenants in a single Elasticsearch index.
- **Hot Tenant Handling**: Identifies and optimizes for hot tenants (high-traffic) by distributing their documents across multiple shards using custom routing.
- **Document Generation**: Creates realistic sample documents with tenant-specific data.
- **Shard Monitoring**: Provides insights into shard distribution and document counts per shard per tenant.
- **Load Balancing**: Uses routing to balance load for hot tenants, reducing the risk of hot shards.

## Architecture
### Design Decisions
- **Single Index for Multi-Tenancy**: All tenants share one index (`multi_tenant_index`) to simplify management, with routing ensuring isolation.
- **Routing Strategy**:
  - **Hot Tenants**: Documents routed to sub-shards based on `doc_id % shard_count` to distribute load.
  - **Non-Hot Tenants**: All documents routed to a single shard per tenant for simplicity.
- **Document Distribution**: 80% of documents (80,000) go to 10 hot tenants (8,000 each), simulating real-world traffic patterns.
- **Monitoring**: Post-insertion analysis shows shard counts and per-shard document distributions.

### Hot Tenant Handling
Hot tenants are tenants with high document volume or query frequency. To handle them:
- **Identification**: Defined as the first 10 tenants (`tenant1` to `tenant10`), each with a random shard count (2-5).
- **Benefits**: Prevents a single shard from becoming overloaded, improves query performance, and allows for better resource utilization.
- **Comparison to Non-Hot**: Non-hot tenants use simple routing (`tenant`), placing all docs on one shard, which is efficient for low-traffic tenants.

- **Routing Logic**: For hot tenants, routing key is `f"{tenant}_{doc_id % hot_tanent[tenant]}"`, distributing documents across multiple shards.
  - Example: If `hot_tanent["tenant1"] = 3`, documents for tenant1 are routed to `tenant1_0`, `tenant1_1`, `tenant1_2`.

```
    def generate_routing(tenant, doc_id):
        if tenant not in hot_tanent:
            return tenant
        else:
            return f"{tenant}_{doc_id % hot_tanent[tenant]}"  # Shard by doc_id mod shard_count
```

This approach mimics production scenarios where popular tenants need distributed indexing to avoid Elasticsearch hotspots.


## How to Run
1. Run the script: `python hot_key.py`
2. The script will:
   - Generate and index 100,000 documents.
   - Print insertion progress.
   - Display shard information (total shards, per node).
   - Show document counts for hot tenants.
   - Provide per-shard document counts for each tenant.

### Sample Output
```
Inserted 1000 documents for tenant1: last ID tenant1_1000
...
Total shards for index 'multi_tenant_index': 5
Shards per node:
  node1: 5 shards
tenant1: 8000 documents
...
Tenant: tenant1
  Shard 0: 2667 documents
  Shard 1: 2667 documents
  Shard 2: 2666 documents
```

# Stats:

## 
![Shard_owning_tenants](Shard_owning_tenants.png)

![Tenant_distribution_on_shards](Tenant_distribution_on_shards.png)


## Usage
- **Customization**: Adjust `tenants`, `hot_tanent`, or document counts in the script.
- **Monitoring**: Use the output to verify load distribution. For hot tenants, docs should be evenly spread across their shards.
- **Scaling**: In production, monitor shard usage with Kibana or ES APIs and adjust `hot_tanent` shard counts based on traffic.

## Troubleshooting
- **Connection Errors**: Ensure Elasticsearch is running and accessible at the configured host/port.
- **Shard Imbalance**: If hot tenants show uneven distribution, check the routing logic or increase shard counts.
- **Performance**: For large datasets, increase ES heap size or use more nodes.
- **Index Not Found**: The script assumes the index exists; create it manually if needed.

For enhancements or production deployment, consider using Elasticsearch's index templates for tenant isolation. Let me know!