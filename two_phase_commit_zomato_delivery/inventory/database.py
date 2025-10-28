from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query
from .models import Base, ItemPayload

from typing import List

DATABASE_URL = "sqlite:///./inventory.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db(reset: bool = False):
    if reset:
        Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def print_records():
    db = SessionLocal()
    records = db.query(ItemPayload).all()
    for record in records:
        print(record)
    db.close()


def get_available_items(item_name: str) -> list[ItemPayload]:
    db = SessionLocal()
    records: list[ItemPayload] = [
        item
        for item in db.query(ItemPayload)
        .filter((ItemPayload.item_name == item_name) & (ItemPayload.reserve == False))
        .all()
    ]
    db.close()
    return records


def get_reserved_items(item_name: str) -> list[ItemPayload]:
    db = SessionLocal()
    records = (
        db.query(ItemPayload)
        .filter(
            (ItemPayload.item_name == item_name)
            & (ItemPayload.reserve == True)
            & (ItemPayload.order_id == None)
        )
        .all()
    )
    db.close()
    return records


def get_record(item_id: str) -> ItemPayload:
    db = SessionLocal()
    record: ItemPayload | None = (
        db.query(ItemPayload).filter(ItemPayload.item_id == item_id).first()
    )
    db.close()
    return record


def delete_record(item_id: str) -> bool:
    try:
        db = SessionLocal()
        record = db.query(ItemPayload).filter(ItemPayload.item_id == item_id).first()
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


def list_records() -> list[ItemPayload]:
    try:
        db = SessionLocal()
        records = [item for item in db.query(ItemPayload).all()]
        db.close()
        return records
    except Exception as e:
        print(f" error in list_records : {e}")
        raise RuntimeError(" error in list_records : {e}")


def add_records(reqs: list[ItemPayload]) -> bool:
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
        raise RuntimeError(" error in add_records : {e}")


def add_record(req: ItemPayload) -> bool:
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
