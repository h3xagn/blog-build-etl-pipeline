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
from datetime import datetime
import pandas as pd

# get working directory
base_dir = os.getcwd()

# define tag name constants
site = "DemoSite"

def process_data_task(eqmt_ip: str, json_data: dict):
    """Process data tasks"""

    # get current time
    file_time = datetime.now()
    file_datetime = file_time.strftime("%Y-%m-%d_%H-%M-%S-%f")
    file_datetime_ts = file_time.strftime("%Y-%m-%d %H:%M:%S.%f")

    # generate json filename and path
    json_filename = f'data_{json_data["sn"]}_{file_datetime}.json'
    json_file_path = f"{base_dir}\\data\\raw\\{json_filename}"

    # save json file to disk
    with open(json_file_path, "w") as jsonfile:
        json.dump(json_data, jsonfile)
    log.info(f"--- Raw JSON data file saved: '{json_filename}'.")

    # transform json data to CSV
    log.info(f"--- Starting data transformation for '{json_filename}'...")
    transform_data(file_datetime, json_data, file_datetime_ts)
    log.info(f"--- Data transformation complete for '{json_filename}'.")

    # log background tasks completed
    log.info(f"--- Background task completed 'process_data_task'.")


def transform_data(eqmt_ip, file_datetime, json_body, file_datetime_ts):
    """Transform JSON data to columnar CSV"""

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

    # save to compressed CSV file
    df.to_csv(
        f"{base_dir}/data/processed/{transformed_filename}",
        compression="gzip",
        index=False,
    )


