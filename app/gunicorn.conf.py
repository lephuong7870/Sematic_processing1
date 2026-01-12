import multiprocessing
import os
port_running  = os.environ.get("PORT_RUNNING")
workers  = 3
bind = f"0.0.0.0:{port_running}"

worker_class = "uvicorn.workers.UvicornWorker"
timeout = 120