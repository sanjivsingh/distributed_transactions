from .database import SessionLocal
from .models import DriverPayload
import time
from datetime import datetime,timedelta


def main():

    while True:
        # print_records()
        ## time.sleep(60) # Pause for 60 seconds
        db = SessionLocal()
        reqs = (
            db.query(DriverPayload)
            .filter(
                (DriverPayload.reserve == True)
                & (DriverPayload.order_id == None)
                & (DriverPayload.reserve_timestamp + timedelta(minutes=1) < datetime.now())
            )
            .all()
        )

        if reqs:  
            
            for req in reqs:
                print(f"request found : {req}")
                req.reserve = False
                req.reserve_timestamp = None
                db.add(req)
                db.commit()
        else:
            print("no request found !!!")
        time.sleep(60)  # Pause for 60 seconds
        db.close()

if __name__ == "__main__":
    main()
