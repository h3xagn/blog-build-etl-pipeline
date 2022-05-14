""" 
Function app to send data to Vendor
- App is triggered when new raw JSON file is upload to storage account
- File is read by the app
- POST request created with JSON file as the body
"""

# load libraries
import logging
import azure.functions as func
import requests
import json
import gzip
from io import StringIO


def main(blobin: func.InputStream) -> None:
    """"Send data to Vendor

    Args:
        blobin (func.InputStream): Compressed raw data file (JSON) from FastAPI web server upload.
    """

    logging.info(f"Python blob trigger function processed blob \n" f"Name: {blobin.name}\n" f"Blob Size: {blobin.length} bytes")

    # read file passed from input stream
    import_file_bytes = blobin.read()
    # OLD: reading JSON file
    #import_file = BytesIO(import_file_bytes)
    # NEW: reading compressed JSON file
    import_file = StringIO(gzip.decompress(import_file_bytes).decode("utf-8"))

    # read the file as JSON
    try:
        json_data = json.load(import_file)
 
    except:
        logging.error(f"*** Failed to read JSON file: {blobin.name}")

    # send POST request to vendor
    try:
        r = requests.post("https://<vendor_url>/", json=json_data)
        logging.info(f"JSON file sent to Vendor: {blobin.name}. Status code: {r.status_code}")
    except:
        logging.error(f"*** Failed to send JSON file to Vendor: {blobin.name}.")