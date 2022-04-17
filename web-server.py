"""
JSON File Uploader

1. Receive JSON file from field device over HTTPS
2. Save raw JSON file to local file system, ./data/raw
"""
# import libraries
import os
from datetime import datetime
import logging as log
import json

# import web server libraries
from fastapi import FastAPI, Request, Response, BackgroundTasks
from starlette.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# import custom modules
from utils import process_json

base_dir = os.getcwd()

log.basicConfig(
    filename="web_server.log",
    level=log.DEBUG,
    format="%(asctime)s.%(msecs)d: %(levelname)s\t%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

origins = [
    "*",
]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

html = """
<!DOCTYPE html>
<html>
    <head>
        <title>JSON File Uploader</title>
    </head>
    <body>
        <h1>JSON File Uploader</h1>
    </body>
</html>
"""

# basic page used for testing
@app.get("/")
async def get():
    """Home"""
    return HTMLResponse(html)

# data upload post
@app.post("/upload/data")
async def upload_data_from_device(data_request: Request, background_tasks: BackgroundTasks):
    """Upload data from field devices"""
    # get device ip address
    eqmt_ip = data_request.client[0]
    log.info(f"--- New POST received from EqmtIP '{eqmt_ip}'.")

    # get json body from request
    try:
        json_body = await data_request.json()
    except:
        log.error(f"*** Error processing JSON file for '{eqmt_ip}'. Sent 500.")
        return Response(content=None, status_code=500)

    # pass data to background tasks for saving and etl
    background_tasks.add_task(process_json.process_data_task, eqmt_ip, json_body)
    log.info(f"--- Background task added 'process_data_task'.")

    # important to include "status_code=200" to avoid duplicate uploads from devices
    return Response(content=None, status_code=200)


if __name__ == "__main__":
    uvicorn.run(
        "fastapi-json:app",
        host="127.0.0.1",
        port=8443,
        log_level="info",
        ssl_keyfile="./cert/key.pem",
        ssl_certfile="./cert/certificate.pem",
        workers=4,
        debug=True,
    )

