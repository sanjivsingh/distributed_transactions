

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query
from .models import Base , OrderPayload

from typing import List

DATABASE_URL = "sqlite:///./order.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def init_db(reset: bool = False):
    if reset:
        Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

def print_records():
    db = SessionLocal()
    records = db.query(OrderPayload).all()
    for record in records:
        print(record)
    db.close()

def get_record(order_id: str) -> OrderPayload:
    db = SessionLocal()
    record: OrderPayload | None = db.query(OrderPayload).filter(OrderPayload.order_id ==order_id).first()
    db.close()
    return record

def list_records() -> list[OrderPayload]:
    try :
        db = SessionLocal()
        records =  [item for item in db.query(OrderPayload).all()]
        db.close()
        return records
    except Exception as e :
        print(f" error in list_records : {e}")
        raise RuntimeError(" error in list_records : {e}")

def add_records(reqs: list[OrderPayload]) -> bool:
    try :
        db = SessionLocal()
        for req in reqs:
            db.add(req)
            db.refresh(req)
        db.commit()
        db.close()
        return True
    except Exception as e :
        print(f" error in add_records : {e}")
        raise RuntimeError(f" error in add_records : {e}")

def delete_record(order_id: str) -> bool:
    try:
        db = SessionLocal()
        record = db.query(OrderPayload).filter(OrderPayload.order_id == order_id).first()
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

def add_record(req: OrderPayload) -> bool:
    try :
        db = SessionLocal()
        db.add(req)
        db.commit()
        db.refresh(req)
        db.close()
        return True
    except Exception as e :
        print(f" error in add_record : {e}")
        raise RuntimeError(f" error in add_record : {e}")


