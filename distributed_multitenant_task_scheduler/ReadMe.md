# Distributed Multitenant Task Scheduler
- A robust and scalable task scheduling system designed for multi-tenant environments.
- Built to efficiently manage and execute tasks across distributed systems while ensuring tenant isolation and resource optimization.
- supports prioritization, retries, and monitoring of scheduled tasks.
- supports tenant of various Size(# of tasks for minutes)  types with different sla and priority levels.


# Start application


## **Run the scheduler_orchestrator** (for asynchronous conversions):
   ```bash
   .venv/bin/python -m distributed_multitenant_task_scheduler.scheduler_orchestrator
   ```

## **Start app server**:
   use uvicorn:
   ```
   .venv/bin/python -m uvicorn distributed_multitenant_task_scheduler.app:app --reload --port 8000
   ```

## **Run the scheduler_orchestrator** (for asynchronous conversions):
   ```bash
   .venv/bin/python -m distributed_multitenant_task_scheduler.orchestrator
   ```

