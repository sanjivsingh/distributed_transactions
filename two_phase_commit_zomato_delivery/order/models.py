from pydantic import BaseModel

from sqlalchemy import Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

class PlaceOrderPayload(BaseModel):
    item_name : str
    quantity : int


class OrderPayload(Base):

    __tablename__ = "order"
    order_id =  Column(String, primary_key=True, index=True)
    item_name = Column(String)
    quantity = Column(Integer)
    driver_id = Column(String)
    order_timestamp = Column(DateTime)
    order_status = Column(String)
    delivery_timestamp = Column(DateTime)

    def __str__(self):  # This method defines the string representation
        return f"Request: order_id:{self.order_id}, item_name:{self.item_name}, quantity:{self.quantity}, driver_id:{self.driver_id} order_timestamp:{self.order_timestamp}, order_status:{self.order_status}, delivery_timestamp:{self.delivery_timestamp}"
