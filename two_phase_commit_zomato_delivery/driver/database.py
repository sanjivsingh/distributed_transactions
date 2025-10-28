from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query
from .models import Base, DriverPayload

from typing import List

DATABASE_URL = "sqlite:///./driver.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db(reset: bool = False):
    if reset:
        Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def print_records():
    db = SessionLocal()
    records = db.query(DriverPayload).all()
    for record in records:
        print(record)
    db.close()


def get_free_drivers() -> list[DriverPayload]:
    db = SessionLocal()
    records: list[DriverPayload] = [
        item
        for item in db.query(DriverPayload).filter(DriverPayload.reserve == False).all()
    ]
    db.close()
    return records


def get_reserved_drivers() -> list[DriverPayload]:
    db = SessionLocal()
    records: list[DriverPayload] = [
        item
        for item in db.query(DriverPayload)
        .filter((DriverPayload.reserve == True) & (DriverPayload.order_id == None))
        .all()
    ]
    db.close()
    return records


def get_record(driver_id: str) -> DriverPayload:
    db = SessionLocal()
    record: DriverPayload | None = (
        db.query(DriverPayload).filter(DriverPayload.driver_id == driver_id).first()
    )
    db.close()
    return record


def list_records() -> list[DriverPayload]:
    try:
        db = SessionLocal()
        records = [item for item in db.query(DriverPayload).all()]
        db.close()
        return records
    except Exception as e:
        print(f" error in list_records : {e}")
        raise RuntimeError(" error in list_records : {e}")


def add_records(reqs: list[DriverPayload]) -> bool:
    try:
        db = SessionLocal()
        for req in reqs:
            db.add(req)
            db.refresh(req)
        db.commit()
        db.close()
        return True
    except Exception as e:
        print(f" error in add_records : {e}")
        raise RuntimeError(f" error in add_records : {e}")


def delete_record(driver_id: str) -> bool:
    try:
        db = SessionLocal()
        record = (
            db.query(DriverPayload).filter(DriverPayload.driver_id == driver_id).first()
        )
        if record:
            db.delete(record)
            db.commit()
            db.close()
            return True
        db.close()
        return False
    except Exception as e:
        print(f" error in delete_record : {e}")
        raise RuntimeError(f" error in delete_record : {e}")


def add_record(req: DriverPayload) -> bool:
    try:
        db = SessionLocal()
        db.add(req)
        db.commit()
        db.refresh(req)
        db.close()
        return True
    except Exception as e:
        print(f" error in add_record : {e}")
        raise RuntimeError(" error in add_record : {e}")
