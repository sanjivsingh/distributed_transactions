# Distributed Multitenant Task Scheduler
- A robust and scalable task scheduling system designed for multi-tenant environments.
- Built to efficiently manage and execute tasks across distributed systems while ensuring tenant isolation and resource optimization.
- Supports prioritization, retries, and monitoring of scheduled tasks.
- Supports tenants of various workloads and SLA tiers.
- UTC timestamps are used throughout for consistency across distributed components.

## Architecture Overview
```
           +-------------+        +-----------------------------+
           |  Tenants    |        |  ZooKeeper                   |
           +------+------+        +-----------------------------+
                  |                | /scheduler/shards           |
                  v                |   tenant -> mysql shard URI |
          +---------------+        | /scheduler/pullers          |
          | Create Task   |        |   tenant desired/active     |
          | Submission API|        | /scheduler/executors        |
          +-------+-------+        |   priority desired/active   |
                  |                +--------------+--------------+
                  v                               |   ^
      +-------------------------+                 |   |
      | Sharded MySQL           |<----------------+   | 
      | scheduled_tasks         |                     |
      | executable_tasks        |                     |
      +-----------+-------------+                     |
                  |                                   |
                  v                                   |
       +----------------------+          +----------------------+               +-------------+
       | Cron Materialiser    |      ----| Scheduler Orchestrator| <------------| monitioring |
       | (per minute)         |      |   | (dynamic provisioning)|              |-------------+
       +----------+-----------+      |    +-----------+----------+
                  |                  |                 |
                  v                  |                 v
      +---------------------+        |   +-----------------------------+
      | Task Pullers (per   |<-------    | Task Executors (per priority)|
      | tenant)             |--Kafka-->  | high/medium/low queues       |
      +---------------------+            +-----------------------------+
```

### Critical Aspects
- **Multi-tenant Isolation**: Each tenant’s tasks live on a dedicated MySQL shard defined in ZooKeeper. The API resolves a tenant→shard mapping on every submission, ensuring data locality and isolation.
- **Task Materializer**: A standalone cron-driven service scans `scheduled_tasks`, resolves cron expressions, and materializes the next minute’s work into `executable_tasks` per shard without blocking API or orchestrator flows.
- **ZooKeeper Orchestration**:
  - `/scheduler/shards/<tenant>` → shard URI.
  - `/scheduler/pullers/<tenant>/desired_pullers` vs `/active/...` → orchestrator compares desired vs actual pullers and provisions new worker processes when needed.
  - `/scheduler/executors/<priority>/desired` vs `/active/...` → per-priority executor pools scale independently.
- **Dynamic Provisioning**: The scheduler orchestrator watches ZooKeeper znodes and automatically spins up/down task pullers (1:M tenant mapping) and executors (per priority) to meet configured capacity targets.
- **Priority Queues**: Pullers push executable tasks into Kafka topics (`tasks_high`, `tasks_medium`, `tasks_low`) based on priority. Executors subscribe only to their priority topic to maintain SLA separation.

## Start application
### Scheduler Orchestrator
```bash
.venv/bin/python -m distributed_multitenant_task_scheduler.scheduler_orchestrator
```
### Application Server
```bash
.venv/bin/python -m uvicorn distributed_multitenant_task_scheduler.app:app --reload --port 8000
```
### Cron Materialiser
```bash
.venv/bin/python -m distributed_multitenant_task_scheduler.schedule_materialiser
```

