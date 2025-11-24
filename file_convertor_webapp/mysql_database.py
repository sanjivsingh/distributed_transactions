import pymysql
from pymysql.cursors import DictCursor
from typing import List, Optional, Tuple, Any
from file_convertor_webapp.models import ConversionRequest
from commons import logger

from setup.mysql_setup import config as mysql_config, constants as mysql_constants

class MysqlDatabaseConnection:


    def __init__(self, reset: bool = False):
        self.host = mysql_config.configurations[mysql_constants.HOST]
        self.user = mysql_config.configurations[mysql_constants.USER]
        self.password = mysql_config.configurations[mysql_constants.PASSWORD]
        self.database = 'conversion_db'
        self.table = 'conversion_request'
        self.port = mysql_config.configurations[mysql_constants.PORT]
        self.connection : pymysql.Connection[DictCursor]

        # Setup logger
        self.log = logger.setup_logger(__name__)

    def connect(self):
        try:
            mysql_temp_conn = pymysql.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    cursorclass=DictCursor
                )
            mysql_temp_conn.cursor().execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            mysql_temp_conn.commit()
            mysql_temp_conn.close()
        except Exception as e:
            self.log.error(f"Error creating database: {e}")

        if self.connection is None:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
                cursorclass=DictCursor
            )
            self.log.info("connected.....")

    def __del__(self):
        self.disconnect()

    def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None

    def init_db(self, reset: bool = False):
        self.connect()
        self.log.info(f"reset : {reset}")
        with self.connection.cursor() as cursor:
            try:
                if reset == True:
                    cursor.execute(f"DROP TABLE IF EXISTS {self.table}")
                    self.log.info("deleted table conversion_request")
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {self.table} (
                        id BIGINT PRIMARY KEY,
                        filename VARCHAR(300),
                        input_format VARCHAR(50),
                        output_format VARCHAR(50),
                        upload_path VARCHAR(300),
                        converted_path VARCHAR(300),
                        status VARCHAR(50),
                        details TEXT,
                        file_hash VARCHAR(64)
                    )
                """)
                self.log.info("created table conversion_request")
                self.connection.commit()
            except Exception as e:
                self.log.info("error in db setup : {e}")
                self.connection.rollback()
            finally:
                self.log.info("db init completed")
            

    def print_records(self):
        self.connect()
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(f"SELECT * FROM {self.table}")
                records = cursor.fetchall()
                for record in records:
                    print(f"ID: {record['id']}, Filename: {record['filename']}, Status: {record['status']}")
            except Exception as e:
                print(f"Error in print_records: {e}")

    def get_pending_requests(self, limit: int = 10) -> Tuple[dict[str,Any]] | None:
        self.connect()
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(f"SELECT * FROM {self.table} WHERE status = 'pending' LIMIT %s ", (limit,))
                return cursor.fetchall()
            except Exception as e:
                print(f"Error in get_pending_requests: {e}")

    def get_record(self, request_number: int) -> dict | None:
        self.connect()
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(f"SELECT * FROM {self.table} WHERE id = %s", (request_number))
                return cursor.fetchone()
            except Exception as e:
                print(f"Error in get_record: {e}")

    def list_records(self) -> Tuple[dict[str,Any]] | None:
        self.connect()
        with self.connection.cursor() as cursor:
            print("list_records...")
            try:
                cursor.execute(f"SELECT * FROM {self.table}")
                return cursor.fetchall()
            except Exception as e:
                print(f"Error in list_records: {e}")

    def get_connection(self):
        return self.connection

    def add_records(self, reqs: List[ConversionRequest]) -> bool:
        self.connect()
        try:
            with self.connection.cursor() as cursor:
                for req in reqs:
                    cursor.execute(f"""
                        INSERT INTO {self.table} (
                        id, filename, input_format, output_format, upload_path, converted_path, status, details, file_hash)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (req.id, req.filename, req.input_format, req.output_format, req.upload_path, req.converted_path, req.status, req.details, req.file_hash))
                self.connection.commit()
            return True
        except Exception as e:
            print(f"Error adding records: {e}")
            self.connection.rollback()
            return False

    def add_record(self, req: ConversionRequest) -> bool:
        self.connect()
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(f"""
                    INSERT INTO {self.table} (id, filename, input_format, output_format, upload_path, converted_path, status, details, file_hash)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (req.id, req.filename, req.input_format, req.output_format, req.upload_path, req.converted_path, req.status, req.details, req.file_hash))
                self.connection.commit()
            return True
        except Exception as e:
            print(f"Error adding record: {e}")
            self.connection.rollback()
            return False

    def update_status(self, request_id: int, status: str, details: str ):
        self.connect()
        try:
            with self.connection.cursor() as cursor:
                if details:
                    cursor.execute(f"UPDATE {self.table} SET status = %s, details = %s WHERE id = %s", (status, details, request_id))
                else:
                    cursor.execute(f"UPDATE {self.table} SET status = %s WHERE id = %s", (status, request_id))
                self.connection.commit()
        except Exception as e:
            print(f"Error updating status: {e}")
            self.connection.rollback()