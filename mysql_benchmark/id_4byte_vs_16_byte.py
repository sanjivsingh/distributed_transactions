import pymysql
import time
import random
import uuid

# MySQL connection
def get_db_connection():
    return pymysql.connect(host='localhost', user='root', password='rootadmin', database='benchmark_db', charset='utf8mb4')

# Create database if not exists
def create_database():
    conn = pymysql.connect(host='localhost', user='root', password='rootadmin', charset='utf8mb4')
    with conn.cursor() as cursor:
        cursor.execute("CREATE DATABASE IF NOT EXISTS benchmark_db")
    conn.commit()
    conn.close()

# Create tables
def create_tables():
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            DROP TABLE IF EXISTS TableId4Byte
        """)
        cursor.execute("""
            DROP TABLE IF EXISTS TableId16Byte
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TableId4Byte (
                ID INT AUTO_INCREMENT PRIMARY KEY,
                Age INT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS TableId16Byte (
                ID VARCHAR(16) PRIMARY KEY,
                Age INT NOT NULL
            )
        """)
    conn.commit()
    conn.close()

# Insert data
def insert_data(table_name, num_rows=10000000):
    conn = get_db_connection()
    start_time = time.time()
    with conn.cursor() as cursor:
        batch_size = 1000
        for i in range(0, num_rows, batch_size):
            if table_name == 'TableId4Byte':
                values = [(random.randint(1, 100),) for _ in range(min(batch_size, num_rows - i))]
                cursor.executemany("INSERT INTO TableId4Byte (Age) VALUES (%s)", values)
            else:
                values = [(str(uuid.uuid4().hex)[:16], random.randint(1, 100)) for _ in range(min(batch_size, num_rows - i))]
                cursor.executemany("INSERT INTO TableId16Byte (ID, Age) VALUES (%s, %s)", values)
    conn.commit()
    conn.close()
    end_time = time.time()
    return end_time - start_time

# Create index and measure time
def create_index(table_name):
    conn = get_db_connection()
    start_time = time.time()
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE INDEX idx_age ON {table_name} (Age)")
    conn.commit()
    conn.close()
    end_time = time.time()
    return end_time - start_time

# Get index size
def get_index_size(table_name):
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT INDEX_LENGTH
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = 'benchmark_db' AND TABLE_NAME = %s
        """, (table_name,))
        result = cursor.fetchone()
    conn.close()
    return result[0] if result else 0

if __name__ == "__main__":
    create_database()
    create_tables()
    print("Creating index on TableId4Byte...")
    index_time_4byte = create_index('TableId4Byte')

    print("Creating index on TableId16Byte...")
    index_time_16byte = create_index('TableId16Byte')

    print("Inserting 1M rows into TableId4Byte...")
    time_4byte = insert_data('TableId4Byte')
    print(f"Time taken: {time_4byte:.2f} seconds")

    print("Inserting 1M rows into TableId16Byte...")
    time_16byte = insert_data('TableId16Byte')
    print(f"Time taken: {time_16byte:.2f} seconds")


    size_4byte = get_index_size('TableId4Byte')
    print(f"Index size: {size_4byte} bytes")

    size_16byte = get_index_size('TableId16Byte')
    print(f"Index size: {size_16byte} bytes")

    print("\nSummary:")
    print(f"Insert 4-byte ID: {time_4byte:.2f}s, Index size: {size_4byte} bytes")
    print(f"Insert 16-byte ID: {time_16byte:.2f}s, Index size: {size_16byte} bytes")