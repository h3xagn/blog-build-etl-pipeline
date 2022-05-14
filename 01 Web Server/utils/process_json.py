"""
JSON File Uploader: Utils module for Background Tasks
- Saving raw JSON file to disk
- Transform JSON data in columnar format
- Save processed data as compressed CSV file
"""

# import libraries
import os
import json
import logging as log
from datetime import date, datetime
import gzip
from typing import Tuple
import pandas as pd

# import Azure Storage libraries
from azure.storage.blob import BlobServiceClient, ContentSettings

# get working directory
base_dir = os.getcwd()

# define tag name constants
site = "DemoSite"


def process_data_task(eqmt_ip: str, json_data: dict) -> None:
    """Process data tasks

    Args:
        eqmt_ip (str): IP address of the equipment from the POST request.
        json_data (dict): JSON data file from the POST request as a dictionary.

    Returns:
        None.
    """

    # get current time
    file_time = datetime.now()
    file_datetime = file_time.strftime("%Y-%m-%d_%H-%M-%S-%f")
    file_datetime_ts = file_time.strftime("%Y-%m-%d %H:%M:%S.%f")

    # generate json filename and path
    json_filename = f'data_{json_data["sn"]}_{file_datetime}.json.gz'
    json_file_path = f'{base_dir}\\data\\raw\\{json_data["sn"]}\\{file_time.year}\\{file_time.month}\\{file_time.day}'

    # create json_file_path if it does not exist
    if not os.path.exists(json_file_path):
        os.makedirs(json_file_path)

    # OLD: save json file to disk
    # with open(json_file_path, "w") as jsonfile:
    #     json.dump(json_data, jsonfile)
    # NEW: save compressed json file to disk
    with gzip.open(f"{json_file_path}/{json_filename}", "w") as file_zip:
        file_zip.write(json.dumps(json_data).encode("utf-8"))
    log.info(f"--- Raw JSON data file saved: '{json_filename}'.")

    # transform json data to CSV
    log.info(f"--- Starting data transformation for '{json_filename}'...")
    transformed_filename, transformed_data = transform_data(eqmt_ip, file_datetime, json_data, file_datetime_ts, file_time)
    log.info(f"--- Data transformation complete for '{json_filename}'.")

    # upload data to azure
    has_json_uploaded, has_csvgz_uploaded = upload_data_to_azure(json_filename, json_data, transformed_filename, transformed_data, file_time)

    if not has_json_uploaded:
        # generate json path for failed uploads
        json_file_path_failed = f"{base_dir}\\data\\failed_upload\\raw\\{json_filename}"
        with open(json_file_path_failed, "w") as jsonfile:
            json.dump(json_data, jsonfile)

    if not has_csvgz_uploaded:
        # generate csv path for failed uploads
        csv_file_path_failed = f"{base_dir}\\data\\failed_upload\\processed\\{transformed_filename}"
        transform_data.to_csv(
            csv_file_path_failed,
            compression="gzip",
            index=False,
        )

    # log background tasks completed
    log.info(f"--- Background task completed 'process_data_task'.")


def transform_data(eqmt_ip: str, file_datetime: str, json_body: dict, file_datetime_ts: str, file_time: datetime) -> Tuple[str, pd.DataFrame]:
    """Transform JSON data to CSV

    Args:
        eqmt_ip (str): IP address of the equipment from the POST request.
        file_datetime (str): Datetime string for the filename.
        json_body (dict): JSON data file from the POST request as a dictionary.
        file_datetime_ts (str): Datetime string to use for the timestamp in the dataframe.
        file_time (datetime): The datatime the file was received.

    Returns:
        Tuple[str, pd.DataFrame]: Returns transformed filename and the dataframe.
    """
    # create the first row with the current timestamp and device serial number
    row = [
        {
            "Timestamp": file_datetime_ts,
            "Tag": f"{site}.OilMon.{eqmt_ip.replace('.', '_')}.DvcSrlNmbr",
            "Value": json_body["sn"],
        }
    ]
    df = pd.DataFrame.from_dict(row)

    # iterate through each data object for device type 'aa' and type 'bb'
    for ds in json_body["data"]:

        if ds["type"] == "aa":
            # convert unix time to date time field when measurement was taken
            timestamp = datetime.fromtimestamp(int(ds["ts"])).strftime("%Y-%m-%d %H:%M:%S.%f")

            # create a dictionary with Timestamp, Tag and Value from the JSON object
            rows = [
                {
                    "Timestamp": timestamp,
                    "Tag": f"{site}.OilMon.{eqmt_ip.replace('.', '_')}.OlTmp",
                    "Value": ds["temperature"].replace("-", ""),
                },
                {
                    "Timestamp": timestamp,
                    "Tag": f"{site}.OilMon.{eqmt_ip.replace('.', '_')}.OlVscsty",
                    "Value": ds["visco"],
                },
                {
                    "Timestamp": timestamp,
                    "Tag": f"{site}.OilMon.{eqmt_ip.replace('.', '_')}.OlDnsty",
                    "Value": ds["density"],
                },
            ]
            # append rows to data frame
            df = df.append(rows)

        if ds["type"] == "bb":
            # convert unix time to date time field when measurement was taken
            timestamp = datetime.fromtimestamp(int(ds["ts"])).strftime("%Y-%m-%d %H:%M:%S.%f")

            # create a dictionary with Timestamp, Tag and Value from the JSON object
            rows = [
                {
                    "Timestamp": timestamp,
                    "Tag": f"{site}.OilMon.{eqmt_ip.replace('.', '_')}.DvcSrlNmbr",
                    "Value": ds["sn"],
                },
                {
                    "Timestamp": timestamp,
                    "Tag": f"{site}.OilMon.{eqmt_ip.replace('.', '_')}.TmSncLstRst",
                    "Value": ds["uptime"],
                },
                {
                    "Timestamp": timestamp,
                    "Tag": f"{site}.OilMon.{eqmt_ip.replace('.', '_')}.EISIntrrgtnsSncLstRst",
                    "Value": ds["sweepCount"],
                },
            ]
            # append rows to data frame
            df = df.append(rows)

    # create filename for processed data
    transformed_filename = f'data_{site}_{eqmt_ip.replace(".", "_")}._{json_body["sn"]}_{file_datetime}.csv.gz'
    transformed_path = f'{base_dir}\\data\\processed\\{json_body["sn"]}\\{file_time.year}\\{file_time.month}\\{file_time.day}'

    # create path if it does not exist
    if not os.path.exists(transformed_path):
        os.makedirs(transformed_path)

    # save to compressed CSV file
    df.to_csv(
        f"{transformed_path}\\{transformed_filename}",
        compression="gzip",
        index=False,
    )

    return transformed_filename, df


def upload_data_to_azure(json_filename: str, json_data: dict, transformed_filename: str, transformed_data: pd.DataFrame, file_time: datetime) -> Tuple[bool, bool]:
    """Upload to Azure Blob Storage

    Args:
        json_filename (str): JSON data assigned filename.
        json_data (dict): JSON data file from the POST request as a dictionary.
        transformed_filename (str): Transformed data assigend filename.
        transformed_data (pd.DataFrame): Pandas dataframe containing the transformed data.
        file_time (datetime): The datatime the file was received.

    Returns:
        Tuple[bool, bool]: Returns the two boolean values to confirm file uploads.
    """

    # set upload flags to False
    has_json_uploaded = False
    has_csvgz_uploaded = False

    # get connection string from .env file
    connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    # create a blob client using the local file name as the name for the blob
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # define blob container and paths
    blob_client_json = blob_service_client.get_blob_client(container="device-data", blob=f'raw/{json_data["sn"]}/{file_time.year}/{file_time.month}/{file_time.day}/{json_filename}')
    blob_client_csvgz = blob_service_client.get_blob_client(container="device-data", blob=f'processed/{json_data["sn"]}/{file_time.year}/{file_time.month}/{file_time.day}/{transformed_filename}')

    # upload raw compressed JSON file
    try:
        json_content_setting = ContentSettings(content_type="application/json")
        blob_client_json.upload_blob(gzip.compress(json.dumps(json_data).encode("utf-8")), overwrite=True, content_settings=json_content_setting)
        has_json_uploaded = True
        log.info(f"--- JSON file uploaded to Azure: '{json_filename}'.")
    except:
        log.error(f"*** JSON file NOT uploaded to Azure: '{json_filename}'.")

    # upload processed compressed CSV file
    try:
        csvgz_content_setting = ContentSettings(content_type="application/x-gzip")

        # OLD: read compressed CSV file from disk
        #  with open(f'{base_dir}\\data\\processed\\{json_data["sn"]}\\{file_time.year}\\{file_time.month}\\{file_time.day}\\{transformed_filename}', "rb") as data:
        #     blob_client_csvgz.upload_blob(data, overwrite=True, content_settings=csvgz_content_setting)

        # NEW: method to load the data frame in memory to blob
        # encode data frame and then compress it
        blob_client_csvgz.upload_blob(gzip.compress(transformed_data.encode("utf-8")), overwrite=True, content_settings=csvgz_content_setting)

        has_csvgz_uploaded = True
        log.info(f"--- Transformed file uploaded to Azure: '{transformed_filename}'.")
    except:
        log.error(f"*** Transformed file NOT uploaded to Azure: '{transformed_filename}'.")

    return has_json_uploaded, has_csvgz_uploaded
