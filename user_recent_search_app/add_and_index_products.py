import pymysql
import ssl
from elasticsearch import Elasticsearch
import random


from setup.mysql_setup import constants as mysql_constants, config as mysql_config



try:
    # First, create database if not exists
    # Added ssl=False to avoid SSL-related auth issues; if using SHA256 auth, ensure MySQL user is set to mysql_native_password or install cryptography properly
    mysql_temp_conn = pymysql.connect(
        host=mysql_config.configurations[mysql_constants.HOST],
        user=mysql_config.configurations[mysql_constants.USER],
        password=mysql_config.configurations[mysql_constants.PASSWORD],
        port=mysql_config.configurations[mysql_constants.PORT]
    )   
    mysql_temp_conn.cursor().execute("CREATE DATABASE IF NOT EXISTS toy_ecommerce")
    mysql_temp_conn.commit()
    mysql_temp_conn.close()
except Exception as e:
    print(f"Error creating database: {e}")
    raise e

# MySQL connection using pymysql
# Added ssl=False and autocommit=True to match and avoid auth errors
mysql_conn = pymysql.connect(
    host=mysql_config.configurations[mysql_constants.HOST],
    user=mysql_config.configurations[mysql_constants.USER],
    password=mysql_config.configurations[mysql_constants.PASSWORD],
    port=mysql_config.configurations[mysql_constants.PORT],
    database="toy_ecommerce"
)

# Create tables if not exist
cursor = mysql_conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        username VARCHAR(255) UNIQUE
    )
""")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS products (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255),
        price DECIMAL(10,2),
        description TEXT,
        category VARCHAR(255),
        sub_category VARCHAR(255)
    )
""")
mysql_conn.commit()

from setup.elasticsearch_setup import (
    constants as elasticsearch_constants,
    config as elasticsearch_config,
)

es = Elasticsearch(
    [
        {
            "host": elasticsearch_config.configurations[
                elasticsearch_constants.ES_HOST
            ],
            "port": elasticsearch_config.configurations[
                elasticsearch_constants.ES_PORT
            ],
            "scheme": "http",
        }
    ]
)

# Realistic product data
categories = {
    "Electronics": ["Smartphones", "Laptops", "Headphones", "Tablets"],
    "Clothing": ["Shirts", "Pants", "Dresses", "Jackets"],
    "Books": ["Fiction", "Non-Fiction", "Textbooks", "Biographies"],
    "Home & Garden": ["Furniture", "Decor", "Appliances", "Tools"],
    "Sports": ["Equipment", "Clothing", "Footwear", "Accessories"],
}

brands = [
    "Apple",
    "Samsung",
    "Nike",
    "Adidas",
    "Sony",
    "Dell",
    "Penguin",
    "IKEA",
    "Bosch",
]


def generate_product(i):
    category = random.choice(list(categories.keys()))
    sub_category = random.choice(categories[category])
    name = f"{random.choice(brands)} {sub_category} {i}"
    price = round(random.uniform(10, 1000), 2)
    description = f"A high-quality {sub_category.lower()} from {name.split()[0]}. Perfect for {random.choice(['daily use', 'professional work', 'leisure activities', 'home improvement'])}."
    return name, price, description, category, sub_category


cursor = mysql_conn.cursor()
for i in range(1, 101):  # 100 products
    name, price, description, category, sub_category = generate_product(i)
    cursor.execute(
        "INSERT INTO products (name, price, description, category, sub_category) VALUES (%s, %s, %s, %s, %s)",
        (name, price, description, category, sub_category),
    )
    # Index in Elasticsearch
    es.index(
        index="products",
        #routing=i,
        id=i,
        body={
            "name": name,
            "price": price,
            "description": description,
            "category": category,
            "sub_category": sub_category,
        },
    )

mysql_conn.commit()
cursor.close()
mysql_conn.close()
print("Inserted 100 realistic products with attributes")
