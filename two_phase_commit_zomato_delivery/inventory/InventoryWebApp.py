from typing import Optional
from pydantic import BaseModel
from decimal import Decimal
from datetime import datetime
import threading

from fastapi import FastAPI, HTTPException , Body 
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

app = FastAPI()
from commons import uniqueid

id_generator = uniqueid.SnowflakeIDGenerator(worker_id= 1)
inventory_lock = threading.Lock()

# 
from .models import OrderQuantityPayload, QuantityItemPayload, ItemPayload

# setup database
from .database import init_db, get_record, add_record, delete_record, list_records, get_available_items, get_reserved_items
init_db(reset = True)
#init_db()  # Ensure DB is initialized


# Route to add an item
@app.post("/items/{item_name}")
def add_items_to_inventory( item_name : str , addItems : QuantityItemPayload) -> JSONResponse:

    quantity = addItems.quantity
    with inventory_lock:
        if not quantity or quantity <= 0:
            raise HTTPException(status_code=400, detail="Quantity must be greater than 0.")

        for i in range(quantity):
            item_id = str(id_generator.generate_id())
            print(f"item_id = {item_id}")
            add_record(ItemPayload(
                item_id = item_id,
                item_name = item_name,
                reserve =  False,
                reserve_timestamp =  None,
                order_id  = None,
                order_timestamp =  None
            ))
    return JSONResponse({"status_code" : 200, "detail": f"Quantity {quantity} added"})


@app.post("/items/{item_name}/reserve")
def reserve_items_from_inventory(item_name: str, reverseItems: QuantityItemPayload) -> JSONResponse:
    quantity = reverseItems.quantity
    with inventory_lock:
        available_items = get_available_items(item_name)
        if len(available_items) < quantity:
            raise HTTPException(status_code=400, detail="quantity can't be reserved")
        # Reserve items
        for i in range(quantity):
            available_items[i].reserve = True
            available_items[i].reserve_timestamp = datetime.now()
            add_record(available_items[i])
    return JSONResponse({"status_code": 200, "detail": f"Quantity {quantity} reserved"})

@app.post("/items/{item_name}/order")
def order_inventory(item_name: str, orderItem: OrderQuantityPayload) -> JSONResponse:
    quantity = orderItem.quantity
    order_id = orderItem.order_id
    with inventory_lock:
        reserve_items = get_reserved_items(item_name)
        if len(reserve_items) < quantity:
            raise HTTPException(status_code=400, detail="quantity can't be ordered")
        # Reserve items
        for i in range(quantity):
            reserve_items[i].order_id = order_id
            reserve_items[i].order_timestamp = datetime.now()
            add_record(reserve_items[i])
    return JSONResponse({"status_code": 200, "detail": f"Quantity {quantity} ordered"})



# Route to list a specific item by ID
@app.get("/items/{item_id}")
def list_item(item_id: str):
    item = get_record(item_id= item_id)
    if item == None:
        raise HTTPException(status_code=404, detail="Item not found.")
    return item

# Route to list all items
@app.get("/items")
def list_items():
    return list_records()


# Route to delete a specific item by ID
@app.delete("/items/{item_id}")
def delete_item(item_id: str) ->  JSONResponse:
    item = get_record(item_id= item_id)
    if item == None:
        raise HTTPException(status_code=404, detail="Item not found.")

    status  = delete_record(item_id)
    if status:
        return JSONResponse({"status_code": 200, "detail": f"item deleted"})
    else:
        return JSONResponse({"status_code": 400, "detail": f"deletion failed"})












