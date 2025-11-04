import pymysql
import time
from faker import Faker
from mysql_setup import config as mysql_config, constants as mysql_constants

fake = Faker()

def get_connection():
    return pymysql.connect(
        host=mysql_config.configurations[mysql_constants.HOST],
        user=mysql_config.configurations[mysql_constants.USER],
        password=mysql_config.configurations[mysql_constants.PASSWORD],
        port=mysql_config.configurations[mysql_constants.PORT],
        database="benchmark_db",  # Assume this database exists
        charset="utf8mb4",
        autocommit=False
    )

def setup_table():
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS benchmark_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                age INT NOT NULL,
                email VARCHAR(255) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    conn.commit()
    conn.close()
    print("Table setup complete.")

def insert_records(num_records=10000000):
    conn = get_connection()
    batch_size = 1000
    total_batches = num_records // batch_size
    start_time = time.time()

    with conn.cursor() as cursor:
        for batch in range(total_batches):
            data = [
                (fake.name(), fake.random_int(min=18, max=80), fake.email())
                for _ in range(batch_size)
            ]
            cursor.executemany(
                "INSERT INTO benchmark_table (name, age, email) VALUES (%s, %s, %s)",
                data
            )
            conn.commit()
            if batch % 100 == 0:
                print(f"Inserted {batch * batch_size} records...")

    conn.close()
    end_time = time.time()
    print(f"Inserted {num_records} records in {end_time - start_time:.2f} seconds.")

def page_based_paginate_records(page=1, limit=10):
    offset = (page - 1) * limit
    conn = get_connection()
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(
            "SELECT id, name, age, email, created_at FROM benchmark_table LIMIT %s OFFSET %s",
            (limit, offset)
        )
        records = cursor.fetchall()
    conn.close()
    return records

def offset_based_paginate_records(offset=1, limit=10):
    conn = get_connection()
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(
            "SELECT id, name, age, email, created_at FROM benchmark_table LIMIT %s OFFSET %s",
            (limit, offset)
        )
        records = cursor.fetchall()
    conn.close()
    return records

def Keyset_paginate_records(key=1, limit=10):
    conn = get_connection()
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(
            "SELECT id, name, age, email, created_at FROM benchmark_table where id = %s LIMIT %s ",
            (key, limit)
        )
        records = cursor.fetchall()
    conn.close()
    return records



if __name__ == "__main__":
    # Setup table
    setup_table()

    # Insert 10M records (comment out if already done)
    #insert_records(10000000)

    # Example pagination
    page = 1
    limit = 1000

    csv_content = ["level,page_time,offset_time,keyset_time"]
    print("Limit and offset based Pagination Benchmarking")
    while True:
        id = (page-1)*limit + 1
        start_time = time.time()
        records = page_based_paginate_records(page, limit)
        page_time = (time.time() - start_time)

        start_time = time.time()
        records = offset_based_paginate_records(id, limit)
        offset_time = (time.time() - start_time)

        start_time = time.time()
        records = Keyset_paginate_records( id, limit)
        keyset_time = (time.time() - start_time)

        msg = f"{page},{page_time:0.4f},{offset_time:0.4f},{keyset_time:0.4f}"
        csv_content.append(msg)
        print(f"{msg}")
        page += 100
        if not records:
            break
    with open("mysql_benchmark/pagination_benchmark_results.csv", "w") as f:
        f.write("\n".join(csv_content))
    print("Benchmarking complete. Results saved to pagination_benchmark_results.csv.")

