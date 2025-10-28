from typing import Optional
from pydantic import BaseModel
from decimal import Decimal


from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class QuantityItemPayload(BaseModel):
    quantity: int

class OrderQuantityPayload(BaseModel):
    order_id : str
    quantity: int

class ItemPayload(Base):
    __tablename__ = "inventory"

    item_id =  Column(String, primary_key=True, index=True)
    item_name = Column(String)
    reserve = Column(Boolean)
    reserve_timestamp = Column(DateTime, nullable=True)
    order_id = Column(String, nullable=True)
    order_timestamp = Column(DateTime, nullable=True)

    def __str__(self):  # This method defines the string representation
        return f"Request: item_id:{self.item_id}, item_name:{self.item_name}, reserve:{self.reserve}, reserve_timestamp:{self.reserve_timestamp}, order_id:{self.order_id}, order_timestamp:{self.order_timestamp}"
