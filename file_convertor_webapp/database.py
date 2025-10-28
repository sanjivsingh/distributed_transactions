from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query
from file_convertor_webapp.models import Base, ConversionRequest

from typing import List
from commons import logger

from abc import ABC


class SqliteDatabaseConnection(ABC):

    def __init__(self, reset: bool = False) -> None:
        super().__init__()
        self.DATABASE_URL = "sqlite:///./conversion.db"
        self.engine = create_engine(
            self.DATABASE_URL, connect_args={"check_same_thread": False}
        )
        self.SessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        self.db = self.SessionLocal()
        # Setup logger
        self.logger = logger.setup_logger(__name__)
        self.init_db(reset)

    def __del__(self):
        if self.db:
            self.db.close()

    def init_db(self, reset: bool = False):
        if reset:
            Base.metadata.drop_all(bind=self.engine)
        Base.metadata.create_all(bind=self.engine)

    def print_records(self):
        records = self.db.query(ConversionRequest).all()
        for record in records:
            self.logger.info(
                f"ID: {record.id}, Filename: {record.filename}, Status: {record.status}"
            )

    def get_pending_requests(self, limit: int) -> list[ConversionRequest]:
        records = (
            self.db.query(ConversionRequest)
            .filter(ConversionRequest.status == "pending")
            .limit(limit)
            .all()
        )
        return records

    def get_record(self, request_number: int) -> ConversionRequest:
        record: ConversionRequest | None = (
            self.db.query(ConversionRequest)
            .filter(ConversionRequest.id == request_number)
            .first()
        )
        return record

    def list_records(self) -> list[ConversionRequest]:
        try:
            records = [item for item in self.db.query(ConversionRequest).all()]
            return records
        except Exception as e:
            self.logger.error(f"error in list_records: {e}")
            raise RuntimeError(" error in list_records : {e}")

    def add_records(self, reqs: list[ConversionRequest]) -> bool:
        try:
            for req in reqs:
                self.db.add(req)
                self.db.refresh(req)
            self.db.commit()
            return True
        except Exception as e:
            self.logger.error(f"error in add_records: {e}")
            raise RuntimeError(" error in add_records : {e}")

    def add_record(self, req: ConversionRequest) -> bool:
        try:

            self.db.add(req)
            self.db.commit()
            self.db.refresh(req)
            return True
        except Exception as e:
            self.logger.error(f"error in add_record: {e}")
            raise RuntimeError(" error in add_record : {e}")
