
from pydantic import BaseModel

from sqlalchemy import Column, String, Integer, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


class NewDriversPayload(BaseModel):
    names: list[str]


class DriverPayload(Base):
    __tablename__ = "driver"

    driver_id =  Column(String, primary_key=True, index=True)
    driver_name = Column(String)
    reserve =  Column(Boolean)
    reserve_timestamp = Column(String, nullable=True)
    order_id = Column(String, nullable=True)
    order_timestamp = Column(DateTime, nullable=True)


    def __str__(self):  # This method defines the string representation
        return f"driver: driver_id:{self.driver_id}, driver_name:{self.driver_name}, reserve:{self.reserve}, reserve_timestamp:{self.reserve_timestamp}, order_id:{self.order_id}, order_timestamp:{self.order_timestamp}"

