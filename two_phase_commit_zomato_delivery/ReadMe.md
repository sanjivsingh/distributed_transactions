.venv/bin/python -m uvicorn two_phase_commit_zomato_delivery.inventory.InventoryWebApp:app --reload --port 8001
.venv/bin/python -m uvicorn two_phase_commit_zomato_delivery.driver.DriverWebApp:app --reload --port 8002
.venv/bin/python -m uvicorn two_phase_commit_zomato_delivery.order.OrderWebApp:app --reload --port 8000

.venv/bin/python -m two_phase_commit_zomato_delivery.driver.worker
.venv/bin/python -m two_phase_commit_zomato_delivery.inventory.worker