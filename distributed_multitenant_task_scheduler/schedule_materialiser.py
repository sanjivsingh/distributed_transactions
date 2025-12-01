import json
import time
from datetime import datetime, timedelta, timezone

import pymysql
from croniter import croniter

from distributed_multitenant_task_scheduler.shard_manager import ShardManager
from distributed_multitenant_task_scheduler import config

class ScheduledTaskMaterializer:
    def __init__(self):
        self.shard_manager = ShardManager()

    def _get_conn(self, shard_uri: str):
        print(shard_uri)
        host, port, user, password, db = shard_uri.split(":")
        return pymysql.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=db,
            autocommit=False,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def _materialize_tasks(self, shard: str):

        data, _ = self.shard_manager.zk.get(f"{config.SHARD_BASE_PATH}/{shard}")
        shard_uri =  data.decode()

        now = datetime.now(timezone.utc)
        window_end = now + timedelta(minutes=1)

        conn = self._get_conn(shard_uri)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM scheduled_tasks")
                scheduled = cur.fetchall()

            executable_rows = []
            for row in scheduled:
                itr = croniter(row["cron_schedule"], now)
                next_time = itr.get_next(datetime)
                while next_time <= window_end:
                    executable_rows.append(
                        (
                            row["task_id"],
                            row["tenant_id"],
                            row["payload"],
                            row["priority"],
                            next_time.strftime("%Y-%m-%d %H:%M:%S"),
                        )
                    )
                    next_time = itr.get_next(datetime)

            if executable_rows:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO executable_tasks
                            (task_id, tenant_id, payload, priority, schedule_time, status)
                        VALUES (%s, %s, %s, %s, %s, 'pending')
                        ON DUPLICATE KEY UPDATE
                            schedule_time=VALUES(schedule_time),
                            status='pending',
                            updated_at=CURRENT_TIMESTAMP
                        """,
                        executable_rows,
                    )
                conn.commit()
        finally:
            conn.close()

    def run_forever(self):
        while True:
            shard_paths = self.shard_manager.zk.get_children(config.SHARD_BASE_PATH)
            for shard in shard_paths:   
                try:
                    self._materialize_tasks(shard)
                except KeyError:
                    continue
            time.sleep(60)

if __name__ == "__main__":
    scheduler = ScheduledTaskMaterializer()
    scheduler.run_forever()
