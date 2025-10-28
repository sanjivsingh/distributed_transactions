from typing import Optional
from pydantic import BaseModel


class FileConvertPayload(BaseModel):
    item_id: Optional[int]
    item_name: str
    quantity: int

from sqlalchemy import Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ConversionRequest(Base):

    __tablename__ = "conversion_requests"
    id = Column(String, primary_key=True, index=True)
    filename = Column(String)
    input_format = Column(String)
    output_format = Column(String)
    upload_path = Column(String)
    converted_path = Column(String)
    status = Column(String)
    details = Column(Text)
    file_hash = Column(String, index=True)  # Add this line

    def __str__(self):  # This method defines the string representation
        return f"Request: id:{self.id}, filename:{self.filename}, input_format:{self.input_format}, output_format:{self.output_format}, upload_path:{self.upload_path}, converted_path:{self.converted_path}, status:{self.status}, details:{self.details}, file_hash:{self.file_hash},"


