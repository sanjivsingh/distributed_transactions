from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import requests
from datetime import datetime
import time
import random
from commons import uniqueid
import threading
from .models import PlaceOrderPayload, OrderPayload
from .database import init_db, get_record, add_record, list_records, delete_record

def retry_request(method, url, max_retries=5, base_delay=1, **kwargs):
    JITTER_FACTOR = 0.5 
    for attempt in range(max_retries):
        try:
            resp = requests.request(method, url, **kwargs)
            if resp.status_code < 500:  # Retry only on server errors
                return resp
        except requests.RequestException:
            pass
        if attempt < max_retries - 1:
            exponential_backoff_delay = base_delay * (2 ** attempt)
            jitter = random.uniform(0, exponential_backoff_delay * JITTER_FACTOR)
            delay = exponential_backoff_delay + jitter  # Exponential backoff + jitter
            time.sleep(delay)
    raise HTTPException(status_code=500, detail="Service unavailable after retries")

app = FastAPI()

# TODO : Implement Service discovery
INVENTORY_RESERVE_SERVICE_URL = "http://127.0.0.1:8001/items/{item_name}/reserve"
DRIVER_RESERVE_SERVICE_URL = "http://127.0.0.1:8002/drivers/reserve"
INVENTORY_ORDER_SERVICE_URL = "http://127.0.0.1:8001/items/{item_name}/order"
DRIVER_ORDER_SERVICE_URL = "http://127.0.0.1:8002/drivers/{order_id}/order"
DRIVER_FREE_SERVICE_URL = "http://127.0.0.1:8002/drivers/{driver_id}/free"

id_generator = uniqueid.SnowflakeIDGenerator(worker_id= 1)
order_lock = threading.Lock()

# setup database
init_db(reset = True)
#init_db()  # Ensure DB is initialized


@app.post("/orders/place")
def place_order(payload: PlaceOrderPayload):
    item_name = payload.item_name
    quantity = payload.quantity

    # Reserve inventory
    inv_resp = retry_request('POST',
        INVENTORY_RESERVE_SERVICE_URL.format(item_name=item_name),
        json={"quantity": quantity}
    )
    if inv_resp.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Inventory reservation failed: {inv_resp.text}")

    # Reserve order
    driver_resp = retry_request('POST',
        DRIVER_RESERVE_SERVICE_URL
    )
    if driver_resp.status_code != 200:
        # Optionally, rollback inventory reservation here
        raise HTTPException(status_code=400, detail=f"Driver reservation failed: {driver_resp.text}")

    order_id = str(id_generator.generate_id())

    # Order inventory
    inv_resp = retry_request('POST',
        INVENTORY_ORDER_SERVICE_URL.format(item_name=item_name),
        json={"quantity": quantity, "order_id"  : order_id}
    )
    if inv_resp.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Inventory commit failed: {inv_resp.text}")

    # Order driver
    driver_order_resp = retry_request('POST',
        DRIVER_ORDER_SERVICE_URL.format(order_id=order_id),
    )
    if driver_order_resp.status_code != 200:
        # Optionally, rollback inventory reservation here
        raise HTTPException(status_code=400, detail=f"Driver commit failed: {driver_order_resp.text}")
    driver_order_data = driver_order_resp.json()
    driver_id = driver_order_data.get("driver_id")

    print("order_finalised")
    add_record(OrderPayload(
                order_id = order_id,
                item_name = item_name,
                quantity =  quantity,
                driver_id =  driver_id,
                order_timestamp =  datetime.now(),
                order_status = "pending"
            ))
    print("order_saved")
    return JSONResponse({
        "status_code": 200,
        "order_id" : order_id,
        "item_name" : item_name,
        "quantity" : quantity,
        "driver_id" : driver_id,
        "detail": f"Order placed for {quantity} of {item_name} with driver {driver_id}."
    })

@app.post("/orders/{order_id}/deliver")
def deliver(order_id: str) -> JSONResponse:
    with order_lock:
        order = get_record(order_id)
        if order is not None:
            
            driver_free_resp = retry_request('POST',
                DRIVER_FREE_SERVICE_URL.format(driver_id=order.driver_id),
            )
            if driver_free_resp.status_code != 200:
                # Optionally, rollback inventory reservation here
                raise HTTPException(status_code=400, detail=f"Rider free failed: {driver_free_resp.text}")

            order.order_status = "delivered"
            order.delivery_timestamp = datetime.now()
            add_record(order)

            return JSONResponse({"status_code": 200, "detail": f"Order {order_id} delivered"})
    raise HTTPException(status_code=400, detail=f"Invalid order {order_id}")

# Route to list a specific item by ID
@app.get("/orders/{order_id}")
def get(order_id: str)  :
    order =  get_record(order_id)
    if order:
        return order
    else:
        raise HTTPException(status_code=400, detail=f"Invalid order {order_id}")

# Route to list all items
@app.get("/orders")
def list_items() :
    return list_records()

# Route to delete a specific item by ID
@app.delete("/orders/{order_id}")
def delete_item(order_id: str) -> JSONResponse:
    item = get_record(order_id= order_id)
    if item is None:
        raise HTTPException(status_code=404, detail="Item not found.")

    status  = delete_record(order_id)
    if status:
        return JSONResponse({"status_code": 200, "detail": "item deleted"})
    else:
        return JSONResponse({"status_code": 400, "detail": "deletion failed"})

