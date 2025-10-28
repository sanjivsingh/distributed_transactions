from datetime import datetime

from fastapi import FastAPI, HTTPException , Body 
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse

app = FastAPI()

from commons import uniqueid
id_generator = uniqueid.SnowflakeIDGenerator(worker_id= 1)

# 
from .models import NewDriversPayload, DriverPayload

# setup database
from .database import init_db, get_record, add_record, list_records, delete_record, get_free_drivers, get_reserved_drivers
init_db(reset = True)
#init_db()  # Ensure DB is initialized


import threading
driver_lock = threading.Lock()

# Route to add an item
@app.post("/drivers/add")
def add_items_to_inventory( newRiders : NewDriversPayload) -> JSONResponse:

    names = newRiders.names
    with driver_lock:

        for name in names:
            driver_id = id_generator.generate_id()
            print(f"added driver_id : {driver_id}")
            add_record(DriverPayload(
                driver_id = str(driver_id),
                driver_name = name,
                reserve =  False,
                reserve_timestamp =  None,
                order_id  = None,
                order_timestamp =  None
            ))
    return JSONResponse({"status_code" : 200, "detail": f"Drivers added"})


@app.post("/drivers/reserve")
def reserve_driver_from_inventory() -> JSONResponse:
    with driver_lock:
        available_drivers = get_free_drivers()
        if len(available_drivers) == 0:
            raise HTTPException(status_code=400, detail="No driver Available")
        
        # Reserve a driver
        available_drivers[0].reserve = True
        available_drivers[0].reserve_timestamp = datetime.now()
        add_record(available_drivers[0])
    return JSONResponse({"status_code": 200, "detail": f"Driver reserved"})


@app.post("/drivers/{order_id}/order")
def book_driver_from_inventory(order_id : str) -> JSONResponse:

    driver_id = None
    with driver_lock:
        reserve_drivers = get_reserved_drivers()
        if len(reserve_drivers) == 0:
            raise HTTPException(status_code=400, detail="No reserve driver Available")

        # Reserve a driver
        reserve_drivers[0].order_id = order_id
        reserve_drivers[0].order_timestamp = datetime.now()
        add_record(reserve_drivers[0])
        driver_id = reserve_drivers[0].driver_id
        
    return JSONResponse({"status_code": 200, "driver_id": driver_id,"detail": f"Driver booked"})


@app.post("/drivers/{driver_id}/free")
def free_driver(driver_id: str) -> JSONResponse:
    with driver_lock:
        driver = get_record(driver_id)
        if driver is not None:
            driver.reserve = False
            driver.reserve_timestamp = None
            driver.order_id = None
            driver.order_timestamp = None
            add_record(driver)
            return JSONResponse({"status_code": 200, "detail": f"Driver {driver_id} freed"})
    raise HTTPException(status_code=400, detail=f"Invalid driver {driver_id}")

# Route to list a specific item by ID
@app.get("/drivers/{driver_id}")
def get(driver_id: str)  :
    driver =  get_record(driver_id)
    if driver:
        return driver
    else:
        raise HTTPException(status_code=400, detail=f"Invalid driver {driver_id}")

# Route to list all items
@app.get("/drivers")
def list_items() :
    return list_records()

# Route to delete a specific item by ID
@app.delete("/drivers/{driver_id}")
def delete_item(driver_id: str) -> JSONResponse:
    item = get_record(driver_id= driver_id)
    if item == None:
        raise HTTPException(status_code=404, detail="Item not found.")

    status  = delete_record(driver_id)
    if status:
        return JSONResponse({"status_code": 200, "detail": f"item deleted"})
    else:
        return JSONResponse({"status_code": 400, "detail": f"deletion failed"})












